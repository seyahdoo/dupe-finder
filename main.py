import os
from blake3 import blake3
import pymongo
import re
from send2trash import send2trash


def hash_file(path):
    hasher = blake3()
    with open(path,"rb") as f:
        for byte_block in iter(lambda: f.read(4096),b""):
            hasher.update(byte_block)
    hash_string = hasher.hexdigest()
    return hash_string

def calculate_missing_hashes(collection):
    unhashed_documents = collection.find({"hash": None})
    for document in unhashed_documents:
        path = document["path"]
        print(f"hashing {path}")
        hash_string = hash_file(path)
        print(f"hash:{path}:{hash_string}")
        collection.update_one({"_id": document["_id"]}, { "$set": { "hash": hash_string } })
    return

def database_create_indexes(collection):
    print(collection.index_information())
    collection.create_index("path")
    collection.create_index("filename")
    collection.create_index("hash")
    print(collection.index_information())
    return

def sync_index_with_path(collection, path):
    all_entries = collection.find()
    for entry in all_entries:
        if not os.path.exists(entry["path"]):
            print(f"removing from db {entry['filename']}         {entry['path']} ")
            collection.delete_one({"_id": entry["_id"]})
    for dirname, dirs, files in os.walk(path):
        for filename in files:
            fullpath = os.path.join(dirname, filename)
            exists = collection.count_documents({"path": fullpath}, limit=1) != 0
            if not exists:
                print(f"adding to db {filename}       {fullpath}")
                collection.insert_one({"path": fullpath, "filename":filename})
    calculate_missing_hashes(collection)
    return

def find_and_delete_dupes(source_collection, target_collection):
    all_targets = target_collection.find()
    for target in all_targets:
        target_hash = target["hash"]
        target_filename = target["filename"]
        target_path = target["path"]
        if not os.path.exists(target_path):
            print(f"removing from db {target_path} {target_filename}")
            target_collection.delete_one({"_id": target["_id"]})    
            
        is_dupe = source_collection.count_documents({"hash": target_hash, "filename": target_filename}, limit=1) != 0
        if is_dupe:
            print(f"dupe found {target_filename}           {target_path}         {target_hash}")
            if os.path.exists(target_path):
                send2trash(target_path)
            target_collection.delete_one({"_id": target["_id"]})    
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
    return

def find_dupes_in_same_collection(collection):
    findings = collection.aggregate([
        {"$group" : { "_id": "$hash", "count": { "$sum": 1 } } },
        {"$match": {"_id" :{ "$ne" : None } , "count" : {"$gt": 1} } },
        {"$project": {"hash" : "$_id", "_id" : 0} }
    ])
    
    # f = collection.find({"$group": { "_id": "$hash", "count": { "$sum": 1}}}, {"$match": { "count": { "$gt": 1 }}})
    for document in findings:
        found_documents = collection.find({"hash": document["hash"]})
        for found_document in found_documents:
            print(found_document["path"])
        print("--")
    return

def do_job(source_collection, target_collection, source_path, target_path):
    sync_index_with_path(source_collection, source_path)
    sync_index_with_path(target_collection, target_path)
    find_and_delete_dupes(source_collection, target_collection)
    return

def main():
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["dupe-finder"]
    source_collection = db["file-index"]
    target_collection = db["target-index"]
    
    temporary_source_collection = db["tmp_source"]
    temporary_target_collection = db["tmp_target"]
    
    # do_job(
    #     temporary_source_collection, 
    #     temporary_target_collection, 
    #     "U:/COLD_STORAGE/Terasad/Backups/Backup 05.08.2017/Records/Fantazya2017/", 
    #     "U:/COLD_STORAGE/Terasad/Backups/Backup 15.09.2017/Photos/miCam/"
    # )
    
    # sync_index_with_path(source_collection, "U:/COLD_STORAGE")
    find_dupes_in_same_collection(source_collection)
    return 

main()


