import requests
import json
import gzip
import io
from pathlib import Path
from config import api_key
from tqdm import tqdm


def create_s3_directories(release_date_path, files, date, headers):
    for i, file_url in enumerate(files):
        # Create a subdirectory named after the zip file
        file_dir = release_date_path / f"zip_file_{i}_for_{date}"
        file_dir.mkdir(parents=True, exist_ok=True)


        # Extract research objects belonging to the zip file and save them in file_dir
        extract_research_objects(file_url, headers=headers, output_dir=file_dir)

def extract_research_objects(gz_url, headers, output_dir):
    """
    decompresses .gz file,
    and writes each JSON object to a separate file in output_dir.

    Parameters:
    - gz_url (str): The pre-signed URL to the .gz file (S3 BUCKET).
    - headers (dict): Headers for the HTTP request.
    - output_dir (Path): The directory where JSON files will be saved.

    Returns:
    - None
    """
    with requests.get(gz_url, headers=headers, stream=True) as response:
        response.raise_for_status()  # Raise an exception for HTTP errors
        response.raw.decode_content = True  # Ensure content is decoded
        with gzip.GzipFile(fileobj=response.raw) as gz:
            with io.TextIOWrapper(gz, encoding='utf-8') as reader:
                for i, line in enumerate(tqdm(reader, desc="Reading lines")):
                    # BREAK IS ONLY FOR LOCAL TESTING
                    #if i >= 100: 
                     #   break
                    if line.strip():
                        try:
                            obj = json.loads(line)
                            # Save obj to a JSON file
                            obj_filename = output_dir / f"object_{i+1}.json"
                            with obj_filename.open('w', encoding='utf-8') as f:
                                json.dump(obj, f, ensure_ascii=False, indent=2)
                        except json.JSONDecodeError as e:
                            print(f"JSON decode error on line {i+1}: {e}")
                            continue


def main():
    headers = {'x-api-key': api_key}
    base_url = 'https://api.semanticscholar.org/datasets/v1/release/'

    base_path = Path(r'')

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
        # 
        # Create a new directory for every zip object
        create_s3_directories(release_date_path, files=files, date=date, headers=headers)

    # Local tracker for rate limit hitting    
    for file in no_files_found:
        print(f"\n No files found for {file} ")


if __name__ == '__main__':
    main()
