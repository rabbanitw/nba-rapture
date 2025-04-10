import os
import ssl
import json
import pymongo

ASC = 1
DESC = -1

# -------------------------------------------------------------------
# 1. CONNECTING TO MONGODB
# -------------------------------------------------------------------
def get_database():
    """
    Returns a reference to the 'my_database' database
    on the remote MongoDB cluster.
    """
    with open('../.env', 'r') as f:
        credentials = json.load(f)

    # Replace this with your actual connection string
    MONGO_URI = (
      f"mongodb+srv://{credentials['username']}:{credentials['password']}@nba-rapture-2.qnfzf.mongodb.net/"
      "?retryWrites=true&w=majority&appName=nba-rapture-2"
    )

    # Create a MongoClient using PyMongo
    client = pymongo.MongoClient(MONGO_URI)

    # The name of the database you want to access
    db = client["nba_rapture"]
    return db


# -------------------------------------------------------------------
# 2. CRUD OPERATION FUNCTIONS
# -------------------------------------------------------------------
def create_document(db, data):
    """
    Insert a single document into 'my_collection'.
    Returns the inserted document's ID.
    """
    collection = db["nba_rapture"]
    result = collection.insert_one(data)
    return result.inserted_id


def read_document(db, query):
    """
    Find a single document in 'my_collection' that matches 'query'.
    Returns the first matching document, or None if no match.
    """
    collection = db["nba_rapture"]
    document = collection.find_one(query)
    return document


def document_exists(db, query):
    """
    Find a single document in 'my_collection' that matches 'query'.
    Returns the first matching document, or None if no match.
    """
    collection = db["nba_rapture"]
    count = collection.count_documents(query, limit=1)
    return count > 0


def update_document(db, query, new_values):
    """
    Update a single document in 'my_collection' that matches 'query'.
    'new_values' should be a dict of fields to set.
    Returns the number of documents modified.
    """
    collection = db["nba_rapture"]
    result = collection.update_one(query, {"$set": new_values})
    return result.modified_count


def delete_document(db, query):
    """
    Delete a single document in 'my_collection' that matches 'query'.
    Returns the number of documents deleted.
    """
    collection = db["nba_rapture"]
    result = collection.delete_one(query)
    return result.deleted_count


# -------------------------------------------------------------------
# 3. DEMO USAGE
# -------------------------------------------------------------------
if __name__ == "__main__":
    # Get a reference to our remote database
    db = get_database()
    collection = db["nba_rapture"]  # Replace with your collection name
    documents = collection.find().sort("_id", ASC).limit(1)

    # Print results
    for doc in documents:
        print(doc)

