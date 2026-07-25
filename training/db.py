"""Mongo connection helper.

Credentials live in credentials.txt at the repo root (username on line 1,
password on line 2). That file is gitignored -- keep it that way.
"""

import os
import urllib.parse
from pathlib import Path

import pymongo

REPO_ROOT = Path(__file__).resolve().parent.parent
CRED_PATH = REPO_ROOT / "credentials.txt"

CLUSTER = "nba-rapture-2.qnfzf.mongodb.net"
DB_NAME = "nba_rapture"
COLL_NAME = "nba_rapture"


def mongo_uri(cred_path=CRED_PATH):
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
    kwargs.setdefault("socketTimeoutMS", 1800000)
    client = pymongo.MongoClient(mongo_uri(), **kwargs)
    return client[DB_NAME][COLL_NAME]
