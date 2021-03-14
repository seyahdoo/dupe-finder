import os

# foreach file in folder recursively
# cache file name, file hash, file size
# index with hash, file name


def index_directory(path):
    for dirname, dirs, files in os.walk(path):
        #print(dirname)     # relative path (from cwd) to the directory being processed
        #print(dirs)       # list of subdirectories in the currently processed directory
        #print(files)       # list of files in the currently processed directory

        for filename in files:
            print(os.path.join(dirname, filename))   # relative path to the "current" file
    return


index_directory("C:/CODE_SEGMANT/dupe-finder/ExampleFolder")

