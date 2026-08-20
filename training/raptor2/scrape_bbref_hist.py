"""Historical per-100 + advanced season tables from basketball-reference,
1977-2023, for the 538 historical-RAPTOR experiments. bbref player ids match
`player_id` in fivethirtyeight/data nba-raptor exactly, so the join is exact
(no fuzzy names). Regular season tables only (the 538 CSV pools RS+PO; noted
as a small mismatch downstream).

Multi-team seasons: bbref lists a TOT/2TM row first -- that row is kept.
Resume-safe: seasons already in the output jsonl are skipped.

Run:  python training/raptor2/scrape_bbref_hist.py
"""

import json
import re
import sys
import time
import urllib.request
from pathlib import Path

OUT = Path(__file__).resolve().parent / "bbref_hist.jsonl"
UA = "Mozilla/5.0 (research; nba-rapture project)"
DELAY = 3.5
TABLES = {"per_poss": "https://www.basketball-reference.com/leagues/NBA_{y}_per_poss.html",
          "advanced": "https://www.basketball-reference.com/leagues/NBA_{y}_advanced.html"}
TOTMARK = {"TOT", "2TM", "3TM", "4TM", "5TM"}


def fetch(url):
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    for attempt in range(4):
        try:
            return urllib.request.urlopen(req, timeout=90).read().decode(
                "utf-8", "ignore")
        except Exception as e:
            print(f"  retry {attempt+1} after {e}", flush=True)
            time.sleep(15 * (attempt + 1))
    raise RuntimeError(f"failed: {url}")


def parse(html):
    out = {}
    for r in re.findall(r"<tr[^>]*>(.*?)</tr>", html, re.S):
        m = re.search(r'data-append-csv="([^"]+)"', r)
        if not m:
            continue
        pid = m.group(1)
        cells = dict(re.findall(
            r'data-stat="([^"]+)"[^>]*>(?:<[^>]*>)*([^<]*)', r))
        team = cells.get("team_name_abbr") or cells.get("team_id") or ""
        if pid in out and team not in TOTMARK:
            continue                      # keep first (TOT) row
        out[pid] = cells
    return out


def main():
    done = set()
    if OUT.exists():
        for line in OUT.read_text().splitlines():
            rec = json.loads(line)
            done.add((rec["season"], rec["table"]))
    with OUT.open("a") as f:
        for y in range(1977, 2024):
            for table, tmpl in TABLES.items():
                if (y, table) in done:
                    continue
                html = fetch(tmpl.format(y=y))
                rows = parse(html)
                f.write(json.dumps({"season": y, "table": table,
                                    "rows": rows}) + "\n")
                f.flush()
                print(f"{y} {table}: {len(rows)} players", flush=True)
                time.sleep(DELAY)
    print("done", flush=True)


if __name__ == "__main__":
    main()
