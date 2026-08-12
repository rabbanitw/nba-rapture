"""Write scraped rows straight into Mongo, idempotently.

The original pipeline scraped to CSV/JSON on disk and then had data_saver.py walk
those folders and insert. That worked but it means a scrape and a load are two
separate jobs with a directory layout as the contract between them, and
data_saver's already_processed() guard makes a round trip per row.

Here the scrapers hand rows to write_rows() and it does one bulk upsert per batch.
Two consequences worth having:

  * Re-running is safe and cheap. The upsert key identifies a row uniquely, so a
    scrape killed halfway through resumes by simply being run again -- no
    processed_files log to keep in sync (wowy_scrape.py's approach, which marks a
    file done only after every player in it succeeded).
  * No duplicate documents. The existing collection needs training/coverage.py's
    pick_doc() to break ties between repeat inserts of the same player; rows
    written through here can't accumulate those.

The upsert filter rides the existing timestamp_1_standard_name_1 index, so nothing
new has to be built on a 4.2M document collection.
"""

import json
import os
import sys
import urllib.parse
from collections import defaultdict
from pathlib import Path

import pymongo

REPO_ROOT = Path(__file__).resolve().parent.parent
CRED_PATH = REPO_ROOT / "credentials.txt"

CLUSTER = "nba-rapture-2.qnfzf.mongodb.net"
DB_NAME = "nba_rapture"
COLL_NAME = "nba_rapture"

# Fields that identify a document rather than describe a player's play. A row is
# replaced when all of these match, so they must pin down exactly one row per source.
KEY_FIELDS = {
    "pbp": ["source", "timestamp", "season_type", "standard_name"],
    "wowy": ["source", "timestamp", "season_type", "standard_name", "on_or_off"],
    # Opponent stats while the player is on/off the floor -- same wowy endpoint with
    # Type=Opponent. Kept as its own source so the two never collide on upsert.
    "wowy-opp": ["source", "timestamp", "season_type", "standard_name", "on_or_off"],
    "nba-tracking": ["source", "timestamp", "season_type", "standard_name", "data_type"],
    # Nearest-defender defended shots (leaguedashptdefend); data_type = category.
    "nba-defend": ["source", "timestamp", "season_type", "standard_name", "data_type"],
    # Own shots by closest-defender distance + time of possession; data_type = table.
    "nba-shotdash": ["source", "timestamp", "season_type", "standard_name", "data_type"],
}


def mongo_uri(cred_path=CRED_PATH):
    """Same contract as training/db.py: MONGO_URI env var, else credentials.txt."""
    if os.environ.get("MONGO_URI"):
        return os.environ["MONGO_URI"]
    lines = [ln.strip() for ln in Path(cred_path).read_text().splitlines() if ln.strip()]
    if len(lines) < 2:
        raise SystemExit(f"{cred_path} needs username on line 1, password on line 2")
    user, pwd = urllib.parse.quote_plus(lines[0]), urllib.parse.quote_plus(lines[1])
    return (f"mongodb+srv://{user}:{pwd}@{CLUSTER}/"
            "?retryWrites=true&w=majority&appName=nba-rapture-2")


def get_collection(**kwargs):
    kwargs.setdefault("serverSelectionTimeoutMS", 60000)
    kwargs.setdefault("socketTimeoutMS", 600000)
    client = pymongo.MongoClient(mongo_uri(), **kwargs)
    return client[DB_NAME][COLL_NAME]


class RawSink:
    """Somewhere to put scraped rows when Mongo is not reachable from here.

    pbpstats blocks some networks and MongoDB Atlas rejects others, so there are
    setups where no single network reaches both -- a VPN gets you pbpstats and
    loses Atlas. Scraping and loading then have to happen at different times, which
    is what the original CSV-then-data_saver.py pipeline did by accident.

    Rows go to raw/<source>/<timestamp>_<season_type>.jsonl, one JSON object per
    line, appended as they arrive. A killed run keeps everything already written,
    and a resumed run reads the file back to see what it can skip. load_raw.py
    upserts the files into Mongo later, from a network that can see it.
    """

    def __init__(self, root):
        self.root = Path(root)

    def path(self, source, timestamp, season_type):
        d = self.root / source
        d.mkdir(parents=True, exist_ok=True)
        return d / f"{timestamp}_{season_type.replace(' ', '_')}.jsonl"

    def append(self, rows, source):
        by_cell = defaultdict(list)
        for r in rows:
            by_cell[(r["timestamp"], r["season_type"])].append(r)
        for (ts, st), group in by_cell.items():
            with self.path(source, ts, st).open("a", encoding="utf-8") as f:
                for r in group:
                    f.write(json.dumps(r, ensure_ascii=False) + "\n")
        return len(rows)

    def keys(self, source, timestamp, season_type, fields):
        p = self.path(source, timestamp, season_type)
        if not p.exists():
            return set()
        out = set()
        with p.open(encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    d = json.loads(line)
                except json.JSONDecodeError:
                    continue          # a torn final line from a killed run
                out.add(tuple(d.get(k) for k in fields))
        return out


def write_rows(coll, rows, source, dry_run=False):
    """Bulk-upsert rows for one source. -> (n_inserted, n_modified, n_matched).

    Each row must already carry every KEY_FIELDS[source] entry. dry_run reports what
    would happen and touches nothing.
    """
    if not rows:
        return 0, 0, 0
    if isinstance(coll, RawSink):
        n = coll.append(rows, source)
        return n, 0, 0
    keys = KEY_FIELDS[source]

    ops = []
    for row in rows:
        missing = [k for k in keys if not row.get(k)]
        if missing:
            raise ValueError(f"{source} row missing key field(s) {missing}: "
                             f"{ {k: row.get(k) for k in keys} }")
        # replace, not $set: a re-scrape should not leave stale fields behind from
        # an earlier run whose feed had columns this one doesn't.
        ops.append(pymongo.ReplaceOne({k: row[k] for k in keys}, row, upsert=True))

    if dry_run:
        print(f"    [dry-run] would upsert {len(ops)} {source} rows")
        return 0, 0, 0

    res = coll.bulk_write(ops, ordered=False)
    return res.upserted_count, res.modified_count, res.matched_count


def existing_keys(coll, source, timestamp, season_type, extra=None):
    """Key tuples already stored for this cell, so a resumed run can skip them."""
    q = {"source": source, "timestamp": timestamp, "season_type": season_type}
    q.update(extra or {})
    keys = [k for k in KEY_FIELDS[source] if k not in q]
    if isinstance(coll, RawSink):
        return coll.keys(source, timestamp, season_type, keys)
    proj = {k: 1 for k in keys}
    proj["_id"] = 0
    return {tuple(d.get(k) for k in keys) for d in coll.find(q, proj)}


def check_connection():
    """Fail loudly and early rather than after an hour of scraping."""
    coll = get_collection(serverSelectionTimeoutMS=20000)
    info = coll.database.client["admin"].command("connectionStatus")["authInfo"]
    roles = [r["role"] for r in info["authenticatedUserRoles"]]
    if not any("readWrite" in r or r == "root" for r in roles):
        raise SystemExit(f"Mongo user lacks write access (roles: {roles})")
    return coll


if __name__ == "__main__":
    coll = check_connection()
    print(f"connected: {coll.database.name}.{coll.name}, "
          f"{coll.estimated_document_count():,} documents")
    print("sources:", sorted(coll.distinct("source")))
    sys.exit(0)
