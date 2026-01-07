import os
print("=== SCRIPT START ===")
print("GOOGLE_SHEET_ID present?", bool(os.getenv("GOOGLE_SHEET_ID")))
print("SERVICE JSON present?", bool(os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")))
print("Working directory files:", os.listdir("."))
print("=== END PRECHECK ===")
import json
import time
from datetime import datetime, timezone

import requests
import yaml
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer


# ---------- Helpers ----------

def env(name: str, default: str | None = None, required: bool = True) -> str:
    val = os.getenv(name, default)
    if required and (val is None or str(val).strip() == ""):
        raise RuntimeError(f"Missing required environment variable: {name}")
    return val


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def safe_sheet_title(title: str) -> str:
    # Google Sheets tab name rules: max 100 chars; cannot contain: : \ / ? * [ ]
    bad = [":", "\\", "/", "?", "*", "[", "]"]
    for b in bad:
        title = title.replace(b, " ")
    title = " ".join(title.split()).strip()
    return title[:100] if title else "Topic"


def sentiment_label(score: float) -> str:
    if score >= 0.05:
        return "positive"
    if score <= -0.05:
        return "negative"
    return "neutral"


# ---------- Reddit fetching (no Reddit API keys needed) ----------

def fetch_subreddit_posts(subreddit: str, limit: int, user_agent: str) -> list[dict]:
    url = f"https://www.reddit.com/r/{subreddit}/new.json"
    params = {"limit": str(limit)}
    headers = {"User-Agent": user_agent}

    r = requests.get(url, params=params, headers=headers, timeout=30)
    # Reddit can respond with 429 if hit too fast
    if r.status_code != 200:
        raise RuntimeError(f"Reddit HTTP {r.status_code} for r/{subreddit}: {r.text[:200]}")

    data = r.json()
    children = data.get("data", {}).get("children", [])
    posts = []
    for c in children:
        p = c.get("data", {})
        # Skip stickied or removed posts, keep it simple
        if p.get("stickied"):
            continue
        posts.append(p)
    return posts


# ---------- Google Sheets ----------

def connect_gsheet(service_json: str, sheet_id: str):
    creds_dict = json.loads(service_json)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(sheet_id)
    return sh


def ensure_worksheet(sh, title: str, headers: list[str]):
title = safe_sheet_title(title)
    try:
        ws = sh.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows=1000, cols=max(10, len(headers)))

    # Force headers into row 1 if it's empty (more reliable than get_all_values())
    first_row = ws.row_values(1)
    if not first_row or all(str(c).strip() == "" for c in first_row):
        ws.update("A1", [headers])  # write headers in one shot
    return ws


def get_existing_post_ids(ws, post_id_col_index: int = 1, max_rows: int = 2000) -> set[str]:
    """
    Reads up to max_rows rows and returns the set of existing post IDs.
    post_id_col_index is 1-based (Google Sheets style).
    """
    values = ws.get_all_values()
    if len(values) <= 1:
        return set()

    # Skip header row
    ids = set()
    for row in values[1:max_rows]:
        if len(row) >= post_id_col_index and row[post_id_col_index - 1].strip():
            ids.add(row[post_id_col_index - 1].strip())
    return ids


def append_rows(ws, rows: list[list], chunk_size: int = 200):
    # Google Sheets has limits; batch in chunks
    for i in range(0, len(rows), chunk_size):
        ws.append_rows(rows[i:i + chunk_size], value_input_option="RAW")


# ---------- Main ----------

def main():
    print("=== Reddit → Sheets run starting ===")
    sheet_id = env("GOOGLE_SHEET_ID")
    service_json = env("GOOGLE_SERVICE_ACCOUNT_JSON")
    user_agent = env("REDDIT_USER_AGENT", default="sentiment-bot/1.0", required=False)

    request_sleep = float(env("REQUEST_SLEEP_SECONDS", default="1.5", required=False))
    per_subreddit_limit = int(env("POSTS_PER_SUBREDDIT", default="25", required=False))

    print("Time:", utc_now_iso())
    print("Sheet ID present:", bool(sheet_id))
    print("Service JSON present:", bool(service_json))
    print("User-Agent:", user_agent)
    print("Posts per subreddit:", per_subreddit_limit)
    print("Sleep seconds:", request_sleep)

    # Load topics.yml
    if not os.path.exists("topics.yml"):
        raise RuntimeError("topics.yml not found in repo root.")
    with open("topics.yml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    topics = cfg.get("topics", [])
    if not topics:
        raise RuntimeError("No topics found in topics.yml (expected a 'topics:' list).")

    # Connect to sheet
    sh = connect_gsheet(service_json, sheet_id)
    # --- SMOKE TEST: prove we can write at least one cell ---
    try:
        smoke = ensure_worksheet(sh, "SMOKE_TEST", ["status", "time_utc"])
        smoke.append_row(["hello from github actions", utc_now_iso()], value_input_option="RAW")
        print("SMOKE TEST: wrote a row to SMOKE_TEST")
    except Exception as e:
        print("SMOKE TEST FAILED:", repr(e))
        raise

    analyzer = SentimentIntensityAnalyzer()

    rollup_rows = []
    rollup_headers = [
        "run_time_utc",
        "topic",
        "subreddit",
        "new_posts_added",
        "avg_compound_sentiment",
    ]
    rollup_ws = ensure_worksheet(sh, "Rollup", rollup_headers)

    for topic in topics:
        topic_name = topic.get("name", "Topic")
        subs = topic.get("subreddits", [])
        if not subs:
            print(f"Skipping topic '{topic_name}' (no subreddits listed).")
            continue

        headers = [
            "post_id",
            "run_time_utc",
            "topic",
            "subreddit",
            "created_utc",
            "title",
            "sentiment_compound",
            "sentiment_label",
            "score_upvotes",
            "num_comments",
            "permalink",
        ]
        ws = ensure_worksheet(sh, topic_name, headers)
        existing_ids = get_existing_post_ids(ws, post_id_col_index=1)

        print(f"\n--- Topic: {topic_name} ---")
        topic_new_rows = []
        topic_scores = []

        for sub in subs:
            print(f"Fetching r/{sub} ...")
            try:
                posts = fetch_subreddit_posts(sub, limit=per_subreddit_limit, user_agent=user_agent)
            except Exception as e:
                print(f"  !! Error fetching r/{sub}: {e}")
                continue

            new_added = 0
            sub_scores = []

            for p in posts:
                post_id = p.get("id", "")
                if not post_id or post_id in existing_ids:
                    continue

                title = (p.get("title") or "").strip()
                if not title:
                    continue

                compound = analyzer.polarity_scores(title)["compound"]
                label = sentiment_label(compound)

                created_utc = p.get("created_utc", "")
                if created_utc:
                    created_utc = datetime.fromtimestamp(float(created_utc), tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

                permalink = "https://www.reddit.com" + (p.get("permalink") or "")

                row = [
                    post_id,
                    utc_now_iso(),
                    topic_name,
                    sub,
                    created_utc,
                    title,
                    compound,
                    label,
                    p.get("score", ""),
                    p.get("num_comments", ""),
                    permalink,
                ]
                topic_new_rows.append(row)
                existing_ids.add(post_id)
                new_added += 1
                sub_scores.append(compound)

            if sub_scores:
                sub_avg = sum(sub_scores) / len(sub_scores)
            else:
                sub_avg = 0.0

            rollup_rows.append([utc_now_iso(), topic_name, sub, new_added, sub_avg])

            topic_scores.extend(sub_scores)

            time.sleep(request_sleep)

        if topic_new_rows:
            print(f"Adding {len(topic_new_rows)} new rows to sheet tab '{safe_sheet_title(topic_name)}' ...")
            append_rows(ws, topic_new_rows)
        else:
            print("No new rows to add for this topic (maybe duplicates or no fresh posts).")

    # Write rollup
    if rollup_rows:
        append_rows(rollup_ws, rollup_rows)

    print("\n=== Done. Check your Google Sheet tabs (including 'Rollup') ===")


if __name__ == "__main__":
    main()
print("=== SCRIPT END (reached end of file) ===")


