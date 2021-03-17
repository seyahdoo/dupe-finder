import os
from hash_module import hash_file
import database

# foreach file in folder recursively
# cache file name, file hash, file size
# index with hash, file name

def hash_directory(path):
    for dirname, dirs, files in os.walk(path):
        for filename in files:
            fullpath = os.path.join(dirname, filename)
            print(f"hashing {filename}")
            hash_string = hash_file(fullpath)
            print(f"{filename}, {fullpath}, {hash_string}")
            database.add_file_to_db(fullpath, filename, hash_string)
    return

def add_all_files_to_index(path):
    for dirname, dirs, files in os.walk(path):
        for filename in files:
            fullpath = os.path.join(dirname, filename)
            database.add_file_to_db(fullpath, filename)
    return


add_all_files_to_index("U:/COLD_STORAGE")
# hash_directory("U:/COLD_STORAGE")
# database.setup()
# database.find()



