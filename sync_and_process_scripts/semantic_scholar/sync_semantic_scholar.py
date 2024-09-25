import requests
import json
import gzip
import io
from pathlib import Path
from config import api_key
from tqdm import tqdm
import pandas as pd

def process_gzip_files(release_date_path, files, date, headers):
    """
    Creates a folder for each zip file stored under the SS bulk data api dates.

    Parameters: 
    - release_date_path:
    - files:
    - date:
    - headers:
    """
    for i, file_url in enumerate(files):
        # Create a subdirectory named after the zip file
        file_dir = release_date_path / f"zip_file_{i}_for_{date}"
        file_dir.mkdir(parents=True, exist_ok=True)

        # Extract research objects belonging to the zip file and save them in file_dir
        extract_research_objects(file_url, headers=headers, output_dir=file_dir)

def extract_research_objects(gz_url, headers, output_dir):
    """
    Decompresses .gz file and writes all JSON objects to a pandas DataFrame,
    saving it as a pickle file in output_dir.

    Parameters:
    - gz_url (str): The pre-signed URL to the .gz file (S3 BUCKET).
    - headers (dict): Headers for the HTTP request.
    - output_dir (Path): The directory where the DataFrame will be saved.

    Returns:
    - None
    """
    data_list = []
    output_file = output_dir / 'data.pkl'

    with requests.get(gz_url, headers=headers, stream=True) as response:
        response.raise_for_status()  # Raise an exception for HTTP errors
        response.raw.decode_content = True  # Ensure content is decoded
        with gzip.GzipFile(fileobj=response.raw) as gz:
            with io.TextIOWrapper(gz, encoding='utf-8') as reader:
                for i, line in enumerate(tqdm(reader, desc="Reading lines")):
                    # Uncomment the following line for local testing
                    # if i >= 100: break
                    if line.strip():
                        try:
                            obj = json.loads(line)
                            # print(obj)
                            data_list.append(obj)
                        except json.JSONDecodeError as e:
                            print(f"JSON decode error on line {i+1}: {e}")
                            continue

    # Convert the entire list to a DataFrame
    df = pd.DataFrame(data_list)
    # get_additional_citation_data(df=df)
    # Save the DataFrame as a pickle file
    df.to_pickle(output_file)

def main():
    """
    
    """
    headers = {'x-api-key': 'FcuPcoxxWC3ePxBABTLvkyWxqvt7v9h32sDBO4ug'}
    base_url = 'https://api.semanticscholar.org/datasets/v1/release/'

    base_path = Path(r'')

    # Step 1: Fetch all release dates
    release_dates_response = requests.get(base_url, headers=headers)
    release_dates_response.raise_for_status()
    release_dates = release_dates_response.json()

    # Step 2: Fetch dataset details for every date
    no_files_found = []
    for i,date in enumerate(release_dates):
        if i > 2:
            break
        url = f"{base_url}{date}/dataset/papers"
        dataset_response = requests.get(url, headers=headers)
        dataset_info = dataset_response.json()
        files = dataset_info.get('files', [])
        
        if not files:
            print(f"No files found for {date}")
            # no_files_found.append(dataset_info)
            pass 
        release_date_dir = date
        release_date_path = base_path / release_date_dir
        release_date_path.mkdir(parents=True, exist_ok=True)
        # 
        # Create a new directory for every zip object
        process_gzip_files(release_date_path, files=files, date=date, headers=headers)

    # Local tracker for rate limit hitting    
    # for file in no_files_found:
    #     print(f"\n No files found for {file} ")

if __name__ == '__main__':
    main()
