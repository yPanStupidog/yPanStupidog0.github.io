"""Fetch Google Scholar citation data, resilient to Scholar blocking CI IPs.

Strategy (in order):
  1. SerpAPI (official JSON API) when SERPAPI_API_KEY is set -- reliable,
     ~30 calls/month on the free tier for a daily cron.
  2. Direct scrape via `scholarly`, with retries and backoff.
  3. Give up gracefully (exit 0, no files written): the workflow then skips
     the push, so the site keeps the last good data and no failure email
     is sent.
"""
import json
import os
import random
import sys
import time
from datetime import datetime

import requests
from scholarly import scholarly

SCHOLAR_ID = os.environ["GOOGLE_SCHOLAR_ID"]
SERPAPI_KEY = os.environ.get("SERPAPI_API_KEY")
MAX_RETRIES = 5


def via_serpapi():
    articles = []
    start = 0
    author = {}
    while True:
        params = {
            "engine": "google_scholar_author",
            "author_id": SCHOLAR_ID,
            "api_key": SERPAPI_KEY,
            "num": 100,
            "start": start,
        }
        resp = requests.get("https://serpapi.com/search.json", params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        if "error" in data:
            raise RuntimeError(f"SerpAPI error: {data['error']}")
        author = data.get("author", author)
        batch = data.get("articles", [])
        articles.extend(batch)
        if len(batch) < 100:
            break
        start += 100

    cited_by = author.get("cited_by", {}) or {}
    table = (cited_by.get("table") or [{}])[0]
    pubs = []
    for a in articles:
        # citation_id looks like "<author_id>:<pub_id>" -- the same
        # author_pub_id format the site theme uses for per-paper citations.
        pid = a.get("citation_id") or a.get("link") or a.get("title") or str(len(pubs))
        pubs.append(
            {
                "author_pub_id": pid,
                "bib": {"title": a.get("title"), "pub_year": a.get("year")},
                "num_citations": ((a.get("cited_by") or {}).get("value")) or 0,
            }
        )
    return {
        "name": author.get("name"),
        "citedby": cited_by.get("value") or 0,
        "hindex": ((table.get("h_index") or {}).get("all")) or 0,
        "i10index": ((table.get("i10_index") or {}).get("all")) or 0,
        "publications": pubs,
    }


def via_scholarly():
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            auth = scholarly.search_author_id(SCHOLAR_ID)
            scholarly.fill(auth, sections=["basics", "indices", "counts", "publications"])
            return auth
        except Exception as e:  # noqa: BLE001 - scraping Scholar is flaky by nature
            last_err = e
            wait = 30 * attempt + random.uniform(0, 10)
            print(f"[attempt {attempt}/{MAX_RETRIES}] scholarly failed: {e!r}; retrying in {wait:.0f}s")
            time.sleep(wait)
    raise last_err


def main():
    author = None
    if SERPAPI_KEY:
        try:
            print("Trying SerpAPI...")
            author = via_serpapi()
            print("SerpAPI OK")
        except Exception as e:  # noqa: BLE001
            print(f"SerpAPI failed ({e!r}); falling back to scholarly")
    if author is None:
        try:
            author = via_scholarly()
        except Exception as e:  # noqa: BLE001
            print(f"WARNING: all fetch methods failed ({e!r}). Keeping last published data; exiting 0.")
            return 0

    author["updated"] = str(datetime.now())
    pubs = author.get("publications") or []
    author["publications"] = {v["author_pub_id"]: v for v in pubs}
    print(json.dumps({"name": author.get("name"), "citedby": author.get("citedby"),
                      "updated": author["updated"]}, indent=2))
    os.makedirs("results", exist_ok=True)
    with open("results/gs_data.json", "w") as f:
        json.dump(author, f, ensure_ascii=False)
    with open("results/gs_data_shieldsio.json", "w") as f:
        json.dump({"schemaVersion": 1, "label": "citations",
                   "message": f"{author.get('citedby')}"}, f, ensure_ascii=False)
    return 0


if __name__ == "__main__":
    sys.exit(main())
