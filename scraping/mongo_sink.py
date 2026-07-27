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

import os
import sys
import urllib.parse
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
    "nba-tracking": ["source", "timestamp", "season_type", "standard_name", "data_type"],
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


def write_rows(coll, rows, source, dry_run=False):
    """Bulk-upsert rows for one source. -> (n_inserted, n_modified, n_matched).

    Each row must already carry every KEY_FIELDS[source] entry. dry_run reports what
    would happen and touches nothing.
    """
    if not rows:
        return 0, 0, 0
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
