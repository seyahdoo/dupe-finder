from blake3 import blake3


def hash_file(path):
    hasher = blake3()
    with open(path,"rb") as f:
        for byte_block in iter(lambda: f.read(4096),b""):
            hasher.update(byte_block)
    hash_string = hasher.hexdigest()
    return hash_string
