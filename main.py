import os
from blake3 import blake3
import pymongo
import re
import shutil
from dirsync import sync

def hash_file(path):
    hasher = blake3()
    with open(path,"rb") as f:
        for byte_block in iter(lambda: f.read(4096),b""):
            hasher.update(byte_block)
    hash_string = hasher.hexdigest()
    return hash_string

def calculate_missing_hashes(collection, cache_collection):
    unhashed_documents = collection.find({"hash": None})
    for document in unhashed_documents:
        path = document["path"]
        hash = None
        if cache_collection != None:
            cached_document = cache_collection.find_one({"path": path})
            if cached_document != None:
                hash = cached_document["hash"]
        if hash == None:
            print(f"hashing   {path}")
            hash = hash_file(path)
            print(f"hash      {path}     {hash}")
        collection.update_one({"_id": document["_id"]}, { "$set": { "hash": hash } })
    return

def database_create_indexes(collection):
    print(collection.index_information())
    collection.create_index("path")
    collection.create_index("filename")
    collection.create_index("hash")
    print(collection.index_information())
    return

def sync_index_with_path(collection, path, cache_collection=None):
    all_entries = collection.find()
    for entry in all_entries:
        if not os.path.exists(entry["path"]):
            print(f"removing from db {entry['filename']}         {entry['path']} ")
            collection.delete_one({"_id": entry["_id"]})
    for dirname, dirs, files in os.walk(path):
        for filename in files:
            fullpath = os.path.join(dirname, filename)
            fullpath = fullpath.replace("\\", "/")
            exists = collection.count_documents({"path": fullpath}, limit=1) != 0
            if not exists:
                print(f"adding to db {filename}       {fullpath}")
                collection.insert_one({"path": fullpath, "filename":filename})
    calculate_missing_hashes(collection, cache_collection)
    return

def find_and_delete_dupes(source_collection, target_collection, recycle_path):
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
                shutil.move(target_path, os.path.join(recycle_path, target_filename))
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

def is_filename_image(filename):
    return filename.endswith(".jpg") or \
           filename.endswith(".png") or \
           filename.endswith(".JPG") or \
           filename.endswith(".PNG") or \
           filename.endswith(".webm") or \
           filename.endswith(".bmp") or \
           filename.endswith(".mp3") or \
           filename.endswith(".wav") or \
           filename.endswith(".jpeg") or \
           filename.endswith(".MOV") or \
           filename.endswith(".mp4")

def find_dupes_in_same_collection(collection):
    
    dupes_with_date = 0
    dupes_withuut_date = 0
    
    pipeline = [
        {"$group": {"_id": { "hash": "$hash", "filename": "$filename"}, "count": {"$sum": 1}}},
        {"$match": {"count": {"$gt": 1}}}
    ]
    findings = collection.aggregate(pipeline)
    for document in findings:
        hash = document["_id"]["hash"]
        filename:str = document["_id"]["filename"]
        if "SquareHome2_backups" in filename:
            continue
            
        # if filename.endswith(".txt"):
        #     continue
            
        # if filename.endswith(".txt") or filename.endswith(".ini") or filename.endswith(".js") or filename.endswith(".pdf"):
        #     continue
            
        if not is_filename_image(filename):
            continue
            
        found_documents = collection.find({"hash": hash, "filename": filename})
        count = collection.count_documents({"hash": hash, "filename": filename})
        if count > 0:
            dated_document_count = 0
            for found_document in found_documents:
                found_document_path:str = found_document['path']
                if re.match(".*(\d\d\.\d\d\.\d\d\d\d).*", found_document_path) is not None:
                    dated_document_count += 1
                # print(f"{found_document['filename']}            {found_document['path']}")
            if dated_document_count == 1 and count >= 2:
                # dupes_with_date += 1
                found_documents.rewind()
                for found_document in found_documents:
                    found_document_path:str = found_document['path']
                    found_document_path = found_document_path.replace("\\", "/")
                    
                    print(f"{found_document['filename']}            {found_document_path}")
                print("--")


            # else:
                # dupes_withuut_date += 1
                
            # print("--")
            # print(count)
    # print(f"dupes with date {dupes_with_date}")
    # print(f"dupes without date {dupes_withuut_date}")
    return

# def check_folder_similarity(source_folder, target_folder):
#     
#     return 

def clean_collection(collection):
    collection.delete_many({})
    return 

def do_job(source_collection, target_collection, source_path, target_path, recycle_bin_path, cache_collection):
    clean_collection(source_collection)
    clean_collection(target_collection)
    sync_index_with_path(source_collection, source_path, cache_collection)
    sync_index_with_path(target_collection, target_path, cache_collection)
    find_and_delete_dupes(source_collection, target_collection, recycle_bin_path)
    return

def change_path_format(collection):
    all_documents = collection.find({})
    for document in all_documents:
        document["path"] = document["path"].replace("\\", "/")
        collection.delete_one({"_id": document["_id"]})
        collection.insert_one(document)
    return 

def find_differences(source, target):
    sync(source, target, "diff")
    return 

def main():
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["dupe-finder"]
    source_collection = db["file-index"]
    target_collection = db["target-index"]
    index_after_refactorings = db["index-after-refactorings"]
    
    temporary_source_collection = db["tmp_source"]
    temporary_target_collection = db["tmp_target"]
    
    recycle_bin_path = "U:/_RECYCLE_BIN/"
    
    # do_job(
    #     temporary_source_collection, 
    #     temporary_target_collection, 
    #     "U:/COLD_STORAGE/Terasad/TERASAD/DATA/Resimler/Fotoğraflar/28.05.2016 DCIM/Camera/", 
    #     "U:/COLD_STORAGE/Terasad/TERASAD/DATA/Resimler/Fotoğraflar/resim/",
    #     recycle_bin_path,
    #     source_collection
    # )
    
    
    # low_priority_paths = [
    #     "U:/COLD_STORAGE/Seaboss/CDLERDEN/cd2/121CASIO/"
    # ]
    
    find_differences("D:/COLD_STORAGE/", "U:/GOOGLE_DRIVE/COLD_STORAGE")
    # sync_index_with_path(source_collection, "U:/COLD_STORAGE")
    # find_dupes_in_same_collection(source_collection)
    return 

main()


