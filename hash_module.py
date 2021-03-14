import hashlib

def hash_file(path):
    sha256_hash = hashlib.sha256()
    with open(path,"rb") as f:
        for byte_block in iter(lambda: f.read(4096),b""):
            sha256_hash.update(byte_block)
        hash_string = sha256_hash.hexdigest()
        return hash_string
