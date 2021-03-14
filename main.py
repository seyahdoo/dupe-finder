import os
import hash_module


# foreach file in folder recursively
# cache file name, file hash, file size
# index with hash, file name


def index_directory(path):
    for dirname, dirs, files in os.walk(path):
        for filename in files:
            fullpath = os.path.join(dirname, filename)
            sha256 = hash_module.hash_file(fullpath)
            print(f"{filename}, {fullpath}, {sha256}")
            
            
            
    return




index_directory("ExampleFolder")






