import duckdb
import os 

def connect_spot(file_path):
    path = os.path.expanduser(file_path)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    return duckdb.connect(path)
