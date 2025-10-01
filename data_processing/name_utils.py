import json
from pathlib import Path
from pymongo import MongoClient

# ---- Config ----
CACHE_PATH = Path("./names_538.json")  # change if you want a different location

username = 'nbarapture'
password = 'fAY8cOij4S9NA8Bx'

MONGO_URI = (
    f"mongodb+srv://{username}:{password}@nba-rapture-2.qnfzf.mongodb.net/"
    "?retryWrites=true&w=majority&appName=nba-rapture-2"
)


# ---- DB init ----
def initialize_db():
    """Initialize database connection"""
    global client, db, coll
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000000, socketTimeoutMS=3000000)
    db = client["nba_rapture"]
    coll = db["nba_rapture"]

def get_names_from_db():
    """Query MongoDB for distinct standard names from source '538'."""
    return coll.distinct('standard_name', {"source": '538'})

# ---- Cache helpers ----
def save_names_to_file(names, cache_path: Path = CACHE_PATH):
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with cache_path.open("w", encoding="utf-8") as f:
        json.dump(names, f, ensure_ascii=False, indent=2)

def load_names_from_file(cache_path: Path = CACHE_PATH):
    if not cache_path.exists():
        return None
    try:
        with cache_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        # Ensure it's a list of strings
        if isinstance(data, list) and all(isinstance(x, str) for x in data):
            return data
        # Fallback if file contents aren't as expected
        return None
    except Exception:
        # Corrupt or unreadable cache — ignore and re-fetch from DB
        return None

def get_or_load_names(cache_path: Path = CACHE_PATH, force_refresh: bool = False):
    """
    If cache exists and not force_refresh, load from disk.
    Otherwise, fetch from DB and update the cache.
    """
    initialize_db()

    if not force_refresh:
        cached = load_names_from_file(cache_path)
        if cached is not None:
            return cached

    # Cache miss or forced refresh: hit DB and save
    names = get_names_from_db()
    save_names_to_file(names, cache_path)
    return names


