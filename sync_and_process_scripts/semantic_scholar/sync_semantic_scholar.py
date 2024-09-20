import requests
import json
import gzip
import io
from pathlib import Path
from config import api_key
from tqdm import tqdm
from urllib.parse import urlparse
import sys
import argparse
import re

def create_s3_directories(release_date_path, files, date):
    for i, file_url in enumerate(files):
        # Extract the filename from the URL

        # Create a subdirectory named after the cleaned filename
        file_dir = release_date_path / f"zip_file_{i}_for_{date}" 
        file_dir.mkdir(parents=True, exist_ok=True)

        # Add research objects belonging to zip file
        

        # Future method: fill in each directory
        # fileObjects = extract_research_objects(file_url, headers=headers)
        # for idx, obj in enumerate(fileObjects, 1):
        #     print(f"\nObject {idx}:")
        #     print(json.dumps(obj, indent=2))

def extract_research_objects(gz_url, headers):
    """
    Streams a .gz file from a URL, decompresses it on the fly,
    and reads the content.

    Parameters:
    - gz_url (str): The pre-signed URL to the .gz file.

    Returns:
    - list: A list of JSON objects (as dictionaries).
    """
    with requests.get(gz_url, headers=headers, stream=True) as response:
        response.raise_for_status()  # Raise an exception for HTTP errors
        response.raw.decode_content = True  # Ensure content is decoded
        with gzip.GzipFile(fileobj=response.raw) as gz:
            with io.TextIOWrapper(gz, encoding='utf-8') as reader:
                objects = []
                for i, line in enumerate(tqdm(reader, desc="Reading lines")):
                    if i >= 100:
                        break
                    if line.strip():
                        try:
                            obj = json.loads(line)
                            objects.append(obj)
                        except json.JSONDecodeError as e:
                            print(f"JSON decode error on line {i+1}: {e}")
                            continue
                return objects

def main():
    headers = {'x-api-key': api_key}
    base_url = 'https://api.semanticscholar.org/datasets/v1/release/'

    # Parse command-line arguments
    # parser = argparse.ArgumentParser(description='Process Semantic Scholar datasets.')
    # parser.add_argument('--base-path', type=str, default='.',
    #                     help='Base path where directories will be created.')
    # args = parser.parse_args()

    base_path = Path(r'C:\Users\diego\OneDrive\Desktop\Desktop\Software\Research\DataCiteSync\sync_and_process_scripts\semantic_scholar\SS_output')

    # Step 1: Fetch all release dates
    release_dates_response = requests.get(base_url, headers=headers)
    release_dates_response.raise_for_status()
    release_dates = release_dates_response.json()

    # Step 2: Fetch dataset details for every date
    no_files_found = []
    for date in release_dates:
        url = f"{base_url}{date}/dataset/papers"
        dataset_response = requests.get(url, headers=headers)
        dataset_info = dataset_response.json()
        files = dataset_info.get('files', [])
        if not files:
            print(f"No files found for {date}")
            no_files_found.append(dataset_info)
            pass 
        release_date_dir = date
        release_date_path = base_path / release_date_dir
        release_date_path.mkdir(parents=True, exist_ok=True)

        # create directory for every zip object
        create_s3_directories(release_date_path, files=files, date=date)
    
    for file in no_files_found:
        print(f"\n No files found for {file} ")




    # dataset_url = f"{base_url}{release_dates[0]}/dataset/papers"
    # print(f"\nFetching dataset details from {dataset_url}...")
    # dataset_response = requests.get(dataset_url, headers=headers)
    # dataset_response.raise_for_status()
    # dataset_info = dataset_response.json()

    # # Step 3: Get the list of .gz file URLs from the 'files' list
    # files = dataset_info.get('files', [])
    # if not files:
    #     print("No files found in the dataset.")
    #     return

    # # Create the release date directory under the base path
    # release_date_dir = release_dates[0]
    # release_date_path = base_path / release_date_dir
    # release_date_path.mkdir(parents=True, exist_ok=True)

    # # Create directories for each file
    # create_s3_directories(release_date_path, files=files)

if __name__ == '__main__':
    main()
