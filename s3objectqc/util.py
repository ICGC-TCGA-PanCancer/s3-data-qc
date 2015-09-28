import hashlib

def get_md5(fname):
    hash = hashlib.md5()
    with open(fname) as f:
        for chunk in iter(lambda: f.read(1024*256), ""):
            hash.update(chunk)
    return hash.hexdigest()
