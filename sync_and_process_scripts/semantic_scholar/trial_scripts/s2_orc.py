import json
import os
import re
import requests
from tqdm import tqdm
from pathlib import Path
# modify these
API_KEY = "dRPhWx6oN78EdeZTr417N5QBmcb1ni8j8gIUbKAK"
DATASET_NAME = "s2orc"
LOCAL_PATH = Path(r'C:\Users\diego\OneDrive\Desktop\Desktop\Software\Research\DataCiteSync\sync_and_process_scripts\testing_data')
os.makedirs(LOCAL_PATH, exist_ok=True)

# get latest release's ID
response = requests.get("https://api.semanticscholar.org/datasets/v1/release/latest").json()
RELEASE_ID = response["release_id"]
print(f"Latest release ID: {RELEASE_ID}")

# get the download links for the s2orc dataset; needs to pass API key through `x-api-key` header
# download via wget. this can take a while...
response = requests.get(f"https://api.semanticscholar.org/datasets/v1/release/{RELEASE_ID}/dataset/{DATASET_NAME}/", headers={"x-api-key": API_KEY}).json()
import gzip
import pickle

for url in tqdm(response["files"]):
    match = re.match(r"https://ai2-s2ag.s3.amazonaws.com/staging/(.*)/s2orc/(.*).gz(.*)", url)
    assert match.group(1) == RELEASE_ID
    SHARD_ID = match.group(2)
    
    # Download the .gz file
    local_gz_file = os.path.join(LOCAL_PATH, f"{SHARD_ID}.gz")
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_gz_file, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    
    # Decompress the .gz file and read the content
    with gzip.open(local_gz_file, 'rt') as f:
        data = f.read()  # Assuming the content is text or JSON

    # Parse the content into a Python object (assuming JSON)
    json_data = json.loads(data)

    # Save the Python object as a pickle file
    pickle_file = os.path.join(LOCAL_PATH, f"{SHARD_ID}.pkl")
    with open(pickle_file, 'wb') as pkl_file:
        pickle.dump(json_data, pkl_file)

    # Optionally, delete the original .gz file to save space
    os.remove(local_gz_file)

print("Downloaded all shards.")