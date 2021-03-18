import os
from blake3 import blake3
import pymongo
import re

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["dupe-finder"]
source_collection = db["file-index"]
target_collection = db["target-index"]

def add_file_to_db(collection, path, filename):
    exists = collection.count_documents({"path": path}, limit=1) != 0
    if not exists:
        collection.insert_one({"path": path, "filename":filename})
    return


def calculate_missing_hashes(collection):
    unhashed_documents = collection.find({"hash": None})
    for document in unhashed_documents:
        path = document["path"]
        print(f"hashing {path}")
        hash_string = hash_file(path)
        print(f"hash:{path}:{hash_string}")
        collection.update_one({"_id": document["_id"]}, { "$set": { "hash": hash_string } })
    return

def database_create_indexes():
    print(target_collection.index_information())
    # target_collection.create_index("path")
    # target_collection.create_index("filename")
    target_collection.create_index("hash")
    print(target_collection.index_information())
    return

def find_dupes_in_same_collection(collection):
    f = collection.find({"$group": { "_id": "$hash", "count": { "$sum": 1}}}, {"$match": { "count": { "$gt": 1 }}})
    for file in f:
        print(file)
    return

def hash_file(path):
    hasher = blake3()
    with open(path,"rb") as f:
        for byte_block in iter(lambda: f.read(4096),b""):
            hasher.update(byte_block)
    hash_string = hasher.hexdigest()
    return hash_string

def add_all_files_to_index(collection, path):
    for dirname, dirs, files in os.walk(path):
        for filename in files:
            fullpath = os.path.join(dirname, filename)
            add_file_to_db(collection, fullpath, filename)
            print(f"{fullpath}")
    return

def find_and_delete_dupes(src, trg):
    all_targets = trg.find()
    for target in all_targets:
        target_hash = target["hash"]
        target_filename = target["filename"]
        target_path = target["path"]
        if not os.path.exists(target_path):
            print(f"removing from db {target_path} {target_filename}")
            trg.delete_one({"_id": target["_id"]})    

        is_dupe = src.count_documents({"hash": target_hash, "filename": target_filename}, limit=1) != 0
        if is_dupe:
            print(f"dupe found {target_filename}           {target_path}         {target_hash}")
            if os.path.exists(target_path):
                os.remove(target_path)  
            trg.delete_one({"_id": target["_id"]})    
    return

def remove_deleted_files_from_db(collection):
    all_targets = collection.find()
    for target in all_targets:
        target_hash = target["hash"]
        target_filename = target["filename"]
        target_path = target["path"]
        if not os.path.exists(target_path):
            print(f"removing from db {target_path} {target_filename}")
            collection.delete_one({"_id": target["_id"]})
    return

def delete_from_database_with_regex(collection):
    regx = re.compile(".*RECYCLE.*", re.IGNORECASE)
    found = collection.find({"path" : regx})
    for target in found:
        target_hash = target["hash"]
        target_filename = target["filename"]
        target_path = target["path"]
        print(f"found {target_path} {target_filename} {target_hash}")
        collection.delete_one({"_id": target["_id"]})

# add_all_files_to_index(target_collection, "C:/Users/kardan/Desktop")
# calculate_missing_hashes(target_collection)
# setup()
# find()
# find_and_delete_dupes(source_collection, target_collection)
# remove_deleted_files_from_db(source_collection)



