import requests
import json
import gzip
import io
from config import api_key
from tqdm import tqdm

def inspect_first_n_lines(gz_url, n=100):
    """
    Streams a .gz file from a URL, decompresses it on the fly,
    and reads the first n lines.

    Parameters:
    - gz_url (str): The pre-signed URL to the .gz file.
    - n (int): Number of lines to read from the decompressed file.

    Returns:
    - list: A list of the first n JSON objects (as dictionaries).
    """
    headers = {}  # Add any required headers if needed

    # Initiate a streaming GET request
    with requests.get(gz_url, headers=headers, stream=True) as response:
        response.raise_for_status()  # Raise an exception for HTTP errors

        # Wrap the response raw bytes in a GzipFile for decompression
        with gzip.GzipFile(fileobj=response.raw) as gz:
            # Wrap the GzipFile in a TextIOWrapper to read text lines
            with io.TextIOWrapper(gz, encoding='utf-8') as reader:
                first_n_lines = []
                for i, line in enumerate(tqdm(reader, desc="Reading lines")):
                    if i >= n:
                        break
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
    print("Fetching available release dates...")
    release_dates_response = requests.get(base_url, headers=headers)
    release_dates_response.raise_for_status()
    release_dates = release_dates_response.json()
    print(f"Available Release Dates: {release_dates}")

    # Step 2: Fetch dataset details for a specific release date
    desired_release = '2023-10-31'
    dataset_url = f"{base_url}{desired_release}/dataset/papers"
    print(f"\nFetching dataset details from {dataset_url}...")
    dataset_response = requests.get(dataset_url, headers=headers)
    dataset_response.raise_for_status()
    dataset_info = dataset_response.json()

    # Display dataset information
    # print(json.dumps(dataset_info, indent=2))

    # Step 3: Get the first .gz file URL from the 'files' list
    files = dataset_info.get('files', [])
    if not files:
        print("No files found in the dataset.")
        return

    first_gz_url = files[0]  # Assuming the first file is the desired one
    # print(f"\nFirst .gz file URL: {first_gz_url}")

    # Step 4: Inspect the first 100 lines from the first .gz file
    print(f"\nInspecting the first 100 lines from the first .gz file...")
    first_100_objects = inspect_first_n_lines(first_gz_url, n=100)

    # Step 5: Display the first 100 JSON objects
    print(f"\nFirst {len(first_100_objects)} JSON Objects:")
    for idx, obj in enumerate(first_100_objects, 1):
        print(f"\nObject {idx}:")
        print(json.dumps(obj, indent=2))

if __name__ == '__main__':
    main()
