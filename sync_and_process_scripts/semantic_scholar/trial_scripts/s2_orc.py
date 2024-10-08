import json
import re
import requests
from tqdm import tqdm
from pathlib import Path
import gzip
import pandas as pd
import time
import argparse

def get_latest_release_id():
    """Fetches the latest release ID from the Semantic Scholar API."""
    response = requests.get("https://api.semanticscholar.org/datasets/v1/release/latest")
    response.raise_for_status()
    return response.json()["release_id"]

def get_dataset_files(release_id, dataset_name, api_key):
    """Retrieves the list of dataset file URLs."""
    url = f"https://api.semanticscholar.org/datasets/v1/release/{release_id}/dataset/{dataset_name}/"
    headers = {"x-api-key": api_key}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()["files"]

def download_file(url, output_path):
    """Downloads a file from a URL to the specified output path."""
    success = False
    while not success:
        try:
            with requests.get(url, stream=True, timeout=60) as r:
                r.raise_for_status()
                with open(output_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
            success = True
        except requests.exceptions.RequestException as e:
            # Check for ExpiredToken error
            if r.status_code == 403 and 'ExpiredToken' in r.text:
                print("\nExpiredToken error detected. Refreshing download URLs...")
                raise ExpiredTokenException("Token has expired.")
            else:
                print(f"\nError downloading {url}: {e}")
                print("Waiting for 5 minutes before retrying...")
                time.sleep(300)  # Wait for 5 minutes

class ExpiredTokenException(Exception):
    """Custom exception for expired tokens."""
    pass

def process_gz_file(gz_file_path):
    """Decompresses a .gz file and returns the JSON data."""
    with gzip.open(gz_file_path, 'rt') as f:
        data = f.read()  # Assuming the content is text or JSON
    json_data = json.loads(data)
    return json_data

def save_parquet(data, parquet_file_path):
    """Saves data to a Parquet file."""
    # Convert the JSON data to a pandas DataFrame
    df = pd.DataFrame(data)
    # Save the DataFrame as a Parquet file
    df.to_parquet(parquet_file_path, engine='pyarrow', index=False)

def parse_arguments():
    """Parses command-line arguments."""
    parser = argparse.ArgumentParser(description="Download and process S2ORC dataset shards.")
    parser.add_argument('--api_key', type=str, required=True, help='Your Semantic Scholar API key.')
    parser.add_argument('--output_dir', type=str, required=True, help='Path to the output directory.')
    parser.add_argument('--dataset_name', type=str, default='s2orc', help='Name of the dataset to download.')
    parser.add_argument('--rate_limit', type=float, default=0.5, help='Delay between requests in seconds.')
    args = parser.parse_args()
    return args

def main():
    args = parse_arguments()

    API_KEY = args.api_key
    DATASET_NAME = args.dataset_name
    OUTPUT_PATH = Path(args.output_dir)
    RATE_LIMIT = args.rate_limit

    OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

    # Get the latest release's ID
    RELEASE_ID = get_latest_release_id()
    # print(f"Latest release ID: {RELEASE_ID}")

    # Get the download links for the dataset
    file_urls = get_dataset_files(RELEASE_ID, DATASET_NAME, API_KEY)

    # Keep track of the index to resume from
    total_files = len(file_urls)
    file_index = 0

    while file_index < total_files:
        url = file_urls[file_index]
        match = re.match(r"https://ai2-s2ag.s3.amazonaws.com/staging/(.*)/s2orc/(.*)\.gz(.*)", url)
        assert match.group(1) == RELEASE_ID
        SHARD_ID = match.group(2)

        # Define file paths
        local_gz_file = OUTPUT_PATH / f"{SHARD_ID}.gz"
        parquet_file = OUTPUT_PATH / f"{SHARD_ID}.parquet"

        # Skip if parquet file already exists
        if parquet_file.exists():
            # print(f"Parquet file {parquet_file} already exists. Skipping...")
            file_index += 1
            continue

        # Limit requests to the specified rate
        time.sleep(RATE_LIMIT)

        # Download the .gz file with retry logic
        try:
            download_file(url, local_gz_file)
        except ExpiredTokenException:
            # Refresh the download URLs
            # print("Refreshing download URLs...")
            file_urls = get_dataset_files(RELEASE_ID, DATASET_NAME, API_KEY)
            # print("Download URLs refreshed.")
            # Continue without incrementing file_index to retry the current file with new URL
            continue

        # Process the .gz file
        json_data = process_gz_file(local_gz_file)

        # Save as Parquet
        save_parquet(json_data, parquet_file)

        # Optionally, delete the original .gz file to save space
        local_gz_file.unlink()

        # Increment the file index
        file_index += 1


if __name__ == "__main__":
    main()
