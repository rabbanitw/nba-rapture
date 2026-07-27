# Scraping

Collects per-player season stats into the `nba_rapture.nba_rapture` Mongo collection,
one document per `(source, timestamp, season_type, player)`.

## What runs where

| source | endpoint | reachable from the dev container? |
|---|---|---|
| `pbp` | `api.pbpstats.com/get-totals` | yes |
| `wowy` | `api.pbpstats.com/get-wowy-stats` | yes |
| `nba-tracking` | `stats.nba.com/stats/leaguedashptstats` | **no — run it yourself** |
| `538` | `web.archive.org` | yes, but there is nothing left to scrape |

`stats.nba.com` does not answer datacenter IPs. The TLS handshake completes, the
request goes out, and the server then sends nothing until the socket times out — 50s,
zero bytes, with or without browser headers, sandboxed or not. `cdn.nba.com` answers
403. Selenium does not get around it, since the browser would issue the same request
from the same address. Only `scrape_nba_tracking.py` is affected.

538 shut down and its RAPTOR page stopped updating, so seasons from 2023-24 on have
features but no label. They are inference cells. `build_dataset.py` keys every row off
a 538 document and will skip them; `leaderboards.py` and `estimated_raptor.py` are
what read them.

## Running a season

```bash
cd scraping

python scrape_pbp_totals.py                    # ~10 min, all three seasons
python scrape_wowy.py                          # hours; resumable, just re-run it
python scrape_nba_tracking.py --report         # ON YOUR OWN MACHINE, checks columns
python scrape_nba_tracking.py                  # ON YOUR OWN MACHINE, writes
```

`scrape_pbp_totals.py` must go first: it writes `rosters/roster_<timestamp>_<split>.json`,
which is how the other two learn who played and what each player's NBA id is.

Useful flags: `--seasons 2025-26`, `--dry-run`, `--limit N` (wowy, top N by minutes),
`--force` (wowy, re-fetch rows already stored).

Everything upserts on the document's key fields, so re-running is safe and skips work
already done. A killed run resumes by being run again.

## Modules

| file | what it does |
|---|---|
| `season_dates.py` | season boundaries from pbpstats' schedule, cached to `season_dates.json` |
| `nba_teams.py` | the 30 team ids, validated against a real schedule |
| `pbpstats_client.py` | retrying HTTP client tuned to how pbpstats fails |
| `mongo_sink.py` | connection + idempotent bulk upsert |
| `scrape_pbp_totals.py` | box/possession totals, and the roster files |
| `scrape_wowy.py` | on/off splits |
| `scrape_nba_tracking.py` | the 14 player-tracking tables |

## Things that bite

**A league-wide `get-totals` call is capped at 500 rows.** Undocumented, and it does
not error — it returns the top 500 by minutes and stops. For 2023-24 that cuts off at
67 minutes played and drops 72 real players. `&Limit=1000` does nothing. Filtering by
`TeamId` is uncapped, so `scrape_pbp_totals.py` fetches both: league-wide for the
aggregated rows (a traded player is one row there, not one per team), per-team for the
tail.

**pbpstats sheds load with a fast 503.** Measured: 16 requests at concurrency 16 gave
14 503s; at concurrency 4, ten; even strictly serial with a 1s gap, about half of
`get-wowy-stats` calls. They come back in ~0.05s and clear on retry — a sample reached
10/10 success at 2.2 calls per success. So the fix is low concurrency plus patient
retries with jitter, not parallelism. `wowy_scrape.py`'s `asyncio.Semaphore(50)` is
why that scrape was painful.

**Join players by id, never by name.** The old pipeline fuzzy-matched scraped names
against the names 538 knew, using `fuzzydict.FuzzyDict`, which returns the closest
match rather than no match. Anyone who debuted after RAPTOR stopped had no correct
answer available and got the nearest old name instead. At snapshot `20250306125347`,
2,572 of 8,370 `nba-tracking` documents are attributed to the wrong player across 170
distinct pairs — `Bronny James` stored as `Bernard James`, `Bilal Coulibaly` as
`Bradley Beal`, `Amen Thompson` as `Jason Thompson`. It affects roughly a third of
tracking documents from 2022 on, and 1.9% of the 2018 snapshot. **This is pre-existing
data, not something the new scrapers produce**, but it is still in the collection.
pbpstats' `EntityId` is the NBA player id, so the new scrapers join on it exactly.

**Tracking column names are display labels, not API fields.** The original scraper read
them out of the rendered HTML table, so the collection stores `CONTESTED\nDREB` — with
a real newline — and `passing` has one column whose name is the empty string.
`coverage.py` matches fields by exact name, so `scrape_nba_tracking.py` translates API
columns back to those labels and refuses to write a table it cannot fully map.

## Superseded files

`pbp_scrape.py`, `wowy_scrape.py`, `nba_tracking_scrape.py`, `data_saver.py`,
`database.py` and `fuzzydict.py` are the original pipeline, kept for reference. They
drove themselves off the filenames of previously scraped 538 snapshots, wrote CSV and
JSON to disk, and loaded it with `data_saver.py` afterwards. Note that `wowy_scrape.py`
and `nba_tracking_scrape.py` both call `utils.get_date_range()`, which did not exist
until this rewrite added it — both raised `AttributeError` on their first date lookup.
