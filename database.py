import pymongo

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["dupe-finder"]
collection = db["file-index"]

def add_file_to_db(path, filename):
    exists = collection.count_documents({"path": path}, limit=1) != 0
    if not exists:
        collection.insert_one({"path": path, "filename":filename})
    return 

def add_hash_to_file(path, hash):
    
    return 

def setup():
    print(collection.index_information())
    collection.create_index("path")
    collection.create_index("filename")
    collection.create_index("hash")
    print(collection.index_information())
    return 

def find():
    
    f = collection.count_documents({"hash": "4396c92b08255dd836e1127abe6891cb05a1ffb10bcb90c33999d728e3eb2679"}, limit=1)
    print(f)
    # f = collection.find()

    # f = collection.find({"$group": { "_id": "$hash", "count": { "$sum": 1}}}, {"$match": { "count": { "$gt": 1 }}})
    # for file in f:
    #     print(file)
    
    
    return 

