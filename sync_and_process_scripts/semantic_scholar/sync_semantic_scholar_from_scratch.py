import requests
import gzip
import json
import io
import time
import logging #
import argparse #
import pandas as pd
import pickle
from tqdm import tqdm
from typing import List,Tuple, TypedDict, Optional #
from pathlib import Path


# Release URL
RELEASE_URL = "https://api.semanticscholar.org/datasets/v1/release/"
RELEASE_DATASET_LIST_URL_TEMPLATE = "https://api.semanticscholar.org/datasets/v1/release/{release_id}"
DATASET_FILES_URL_TEMPLATE = "https://api.semanticscholar.org/datasets/v1/release/{release_id}/dataset/{dataset_name}"


class DatasetInfo(TypedDict):
    name: str
    description: str
    README: str


class ReleaseInfo(TypedDict):
    release_id: str
    README: str
    datasets: List[DatasetInfo]


class DatasetLinks(DatasetInfo):
    name: str
    description: str
    README: str
    files: List[str]
    message: Optional[str]
    code: Optional[str]


def get_latest_release() -> str:
    response = requests.get(RELEASE_URL)
    data = response.json()
    return data[-1]


def get_release_info(release_id: str) -> ReleaseInfo:
    target_url = RELEASE_DATASET_LIST_URL_TEMPLATE.format(release_id=release_id)
    response = requests.get(target_url)
    release_dict = response.json()
    return release_dict


def get_links_for_dataset(release_id: str, dataset_name: str, api_key: str) -> DatasetInfo:
    headers = {
        "x-api-key": api_key
    }
    target_url = DATASET_FILES_URL_TEMPLATE.format(release_id=release_id, dataset_name=dataset_name)
    response = requests.get(target_url, headers=headers)
    dataset_links_dict = response.json()
    # print('Dataset Links Dictionary, ',dataset_links_dict.keys())
    # if 'files' in dataset_links_dict:
        # print(dataset_links_dict['files'], type(dataset_links_dict['files']))
    return dataset_links_dict


def get_links_for_each_dataset(release_id: str, dataset_list: List[DatasetInfo], api_key: str) -> DatasetLinks:
    datasets_with_links = list()
    
    for dataset in dataset_list:
        dataset_links = get_links_for_dataset(release_id, dataset["name"], api_key)
        updated_dataset: DatasetLinks = dataset
        updated_dataset["files"] = dataset_links.get("files")
        updated_dataset["message"] = dataset_links.get("message")
        updated_dataset["code"] = dataset_links.get("code")
        datasets_with_links.append(updated_dataset)
    return datasets_with_links

def download_datasets(release_id: str, dataset_links_list: List[DatasetLinks], output_base_dir: str, api_key: str, testing: bool=False ) -> bool:
    # Prepare release output directory
    release_output_path = f"{output_base_dir}/{release_id}"
    release_path = Path(release_output_path)
    release_path.mkdir(parents=True, exist_ok=True)
    # For each dataset download files
    for dataset_links in dataset_links_list:
        if(dataset_links.get("code") != None):
            logging.info("API Key error : Dataset {dataset_name}".format(dataset_name=dataset_links["name"]))
        else:
            
            success = download_files_for_dataset(dataset_links, release_output_path, api_key=api_key, release_id=release_id, testing=testing)
            if(success == False):
                logging.error("Failed to download dataset : {dataset_name}".format(dataset_name=dataset_links["name"]))
            if(success == True):
               logging.info("Dataset {dataset_name} was downloaded".format(dataset_name=dataset_links["name"])) 
    return True


def download_files_for_dataset(dataset_links: DatasetLinks, output_base_dir: str,  api_key: str, release_id:str, testing: bool=False) -> bool:
    '''
    An iterative approach to solving the issue of tokens timing out, 
    per dataset, we are using a while loop to extract the data from the compressed urls.
    If a token times out on a url, we will request a new batch of urls, ensuring they are always in the same order. 
    we also keep track of the 

    '''
    # Prepare output dir for dataset
    output_dataset_path = "{output_base_dir}/{dataset_name}".format(output_base_dir=output_base_dir, dataset_name=dataset_links["name"])
    output_path = Path(output_dataset_path)
    output_path.mkdir(parents=True, exist_ok=True)

    # For each link in dataset
    link = 0
    last_downloaded_object = 0
    new_urls = {}

    while link < len(dataset_links["files"]):
        if len(new_urls.keys()) == 0:
                part_name = f"part_{link:03d}"
                return_bool, last_downloaded_object = download_data_file_as_parquet(output_dataset_path, part_name, dataset_links["files"][link], last_downloaded_object)
                if return_bool == True:
                    link += 1
                    last_downloaded_object = 0
                    continue
                #If false, then the token timed out and we new urls.
                else:
                    new_urls = get_links_for_dataset(release_id=release_id, dataset_name=dataset_links["name"], api_key=api_key )
        else:
                part_name = f"part_{link:03d}"
                return_bool, last_downloaded_object = download_data_file_as_parquet(output_dataset_path, part_name, dataset_links["files"][link], last_downloaded_object)
                if return_bool == True:
                    link += 1
                    last_downloaded_object = 0
                    continue
                else:
                    new_urls = get_links_for_dataset(release_id=release_id, dataset_name=dataset_links["name"], api_key=api_key )
    return True


def download_data_file_as_parquet(output_dir: str, output_filename: str, url: str) -> bool:
    records = list()
    try:
        with requests.get(url, stream=True) as response:
            response.raise_for_status()
            with gzip.GzipFile(fileobj=response.raw) as gz:
                with io.TextIOWrapper(gz, encoding="utf-8") as reader:
                    print((reader))
                    for i, line in enumerate(tqdm(reader, desc="Reading lines")):
                        line = line.strip()
                        if line:
                            try:
                                record_object = json.loads(line)
                                records.append(record_object)
                            except json.JSONDecodeError as e:
                                logging.error(f"JSON decode error on line {i+1}: {e}")
                                continue
                            except Exception as e:
                                logging.error(f"Unknown Error {i+1}: {e}")
                                continue

        records_df = pd.DataFrame(records)
        print('records df', records_df)
        records_df.to_parquet(f"{output_dir}/{output_filename}.parquet", engine="pyarrow")
        return True
    except requests.RequestException as e:
        # Return False and the last processed line 
        logging.error(f"Request error (possible token expiration or network issue): {e}")
        return False
    except Exception as e:
        # General error handling, return 
        logging.error(f"General error: {e}")
        return False 

def main():    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="""
    Full download of latest sematic scholar dataset release
""")
    parser.add_argument(
        "--key", help="API key for semantic scholar API"
    )
    parser.add_argument(
        "-o", "--output_dir", help="Output base directory of downloaded files"
    )
    parser.add_argument(
        "--test", action="store_true"
    )

    arguments = parser.parse_args()

    output_base_path_str = arguments.output_dir
    output_base_path = Path(output_base_path_str)
    output_base_path.mkdir(parents=True, exist_ok=True)

    log_level = logging.INFO

    api_key = arguments.key

    # is_testing = True if arguments.test else False
    is_testing = False
    # Setup logging file
    logging.basicConfig(filename=output_base_path / "log.out", 
                        level=log_level,
                        format="%(asctime)s %(levelname)s %(processName)s %(message)s")

    logging.info("Started sync process")

    # Start time
    start_time = time.time()

    if(is_testing):
        # Get latest release id
        latest_release_id = "2024-09-17"
        # Load URLs for each file in each dataset from pickle file
        with open("testing_data/dataset_list.pickle", "rb") as f:
            dataset_list = pickle.load(f)
        # Load URLs for each file in each dataset from pickle file
        with open("testing_data/dataset_links.pickle", "rb") as f:
            dataset_links_list = pickle.load(f)
    else:
        # Get latest release id
        latest_release_id = get_latest_release()
        
        # List datasets in latest release
        release_dict = get_release_info(latest_release_id)
        
        dataset_list = release_dict.get("datasets")
        # Get URLs for each file in each dataset
        # return 
        dataset_links_list = get_links_for_each_dataset(latest_release_id, dataset_list, api_key)
    # Download files for each dataset
    dataset_download_success = download_datasets(latest_release_id, dataset_links_list, output_base_path_str, api_key, testing=False )

    # End time
    sync_duration = time.time() - start_time

    # Output sync result to log file
    if(dataset_download_success):
        logging.info(f"Sync succeeded on release {latest_release_id}: time elapsed {sync_duration} seconds")
    else:
        logging.info(f"Sync failed on release {latest_release_id}: time elapsed {sync_duration} seconds")


if __name__ == "__main__":
    main()