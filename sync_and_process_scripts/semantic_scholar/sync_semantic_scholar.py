import os
import requests
import json
import gzip
import io
from config import api_key
from tqdm import tqdm
from urllib.parse import urlparse


def create_s3_directories():
    pass

def inspect_lines(gz_url, headers):
    """
    Streams a .gz file from a URL, decompresses it on the fly,
    and reads the first n lines.

    Parameters:
    - gz_url (str): The pre-signed URL to the .gz file.
    - n (int): Number of lines to read from the decompressed file.

    Returns:
    - list: A list of the first n JSON objects (as dictionaries).
    """

    # Initiate a streaming GET request
    with requests.get(gz_url, headers=headers, stream=True) as response:
        response.raise_for_status()  # Raise an exception for HTTP errors
        # Wrap the response raw bytes in a GzipFile for decompression
        with gzip.GzipFile(fileobj=response.raw) as gz:
            # Wrap the GzipFile in a TextIOWrapper to read text lines
            with io.TextIOWrapper(gz, encoding='utf-8') as reader:
                first_n_lines = []
                for i, line in enumerate(tqdm(reader, desc="Reading lines")):
                    
                    if line.strip():  # Ensure the line is not empty
                        try:
                            # Parse the JSON object
                            obj = json.loads(line)
                            first_n_lines.append(obj)
                        except json.JSONDecodeError as e:
                            print(f"JSON decode error on line {i+1}: {e}")
                            continue
                return first_n_lines

def main():
    headers = {'x-api-key': api_key}
    base_url = 'https://api.semanticscholar.org/datasets/v1/release/'

    # Step 1: Fetch all release dates
    # print("Fetching available release dates...")
    release_dates_response = requests.get(base_url, headers=headers)
    release_dates_response.raise_for_status()
    release_dates = release_dates_response.json()
    # print(f"Available Release Dates: {release_dates}")

    # Step 2: Fetch dataset details for a specific release date
    dataset_url = f"{base_url}{release_dates[0]}/dataset/papers"
    print(f"\nFetching dataset details from {dataset_url}...")
    dataset_response = requests.get(dataset_url, headers=headers)
    dataset_response.raise_for_status()
    dataset_info = dataset_response.json()

    # Display dataset information
    # print(json.dumps(dataset_info, indent=2))

    # Step 3: Get the first .gz file URL from the 'files' list
    # NOTE: Maybe we need to add in some better error handling?
    files = dataset_info.get('files', [])
    if not files:
        print("No files found in the dataset.")
        return

    release_date_dir = release_dates[0]
    os.makedirs(release_date_dir, exist_ok=True)

    for i, file_url in enumerate(files):
        # print(f"\n.gz file URL: {file_url}, Index: {i}")

        # Extract the filename from the URL
        parsed_url = urlparse(file_url)
        filename = os.path.basename(f"s3_zip-{i}")

        # Remove any query parameters or fragments from the filename
        filename = filename.split('?')[0].split('#')[0]

        # Create a subdirectory named after the filename (without extension)
        filename_without_ext = os.path.splitext(filename)[0]
        file_dir = os.path.join(release_date_dir, filename_without_ext)
        os.makedirs(file_dir, exist_ok=True)       

        # fileObjects = inspect_lines(file, headers=headers)
        # for idx, obj in enumerate(fileObjects, 1):
        #     print(f"\nObject {idx}:")
        #     print(json.dumps(obj, indent=2))

    # Step 5: Display objects
    # print(f"\nFirst {len(first_100_objects)} JSON Objects:")
    # for idx, obj in enumerate(first_100_objects, 1):
    #     print(f"\nObject {idx}:")
    #     print(json.dumps(obj, indent=2))

if __name__ == '__main__':
    main()
