import requests
import gzip
import json
import io
import time
import logging #
import argparse #
import pandas as pd
import pickle
import re
from tqdm import tqdm
from typing import List,Tuple, TypedDict, Optional, Dict, Any #
from pathlib import Path
from ratelimit import limits, sleep_and_retry

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

class LinkDownloadStatus(TypedDict): 
    url: str
    complete: bool

def get_latest_release() -> str:
    response = requests.get(RELEASE_URL)
    data = response.json()
    return data[-1]


def get_release_info(release_id: str) -> ReleaseInfo:
    target_url = RELEASE_DATASET_LIST_URL_TEMPLATE.format(release_id=release_id)
    response = requests.get(target_url)
    release_dict = response.json()
    return release_dict

@sleep_and_retry
@limits(calls=1, period=60)
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


def download_files_for_dataset(dataset_links: DatasetLinks, output_base_dir: str, release_id:str, api_key: str, testing: bool=False) -> bool:
    # Prepare output dir for dataset
    output_dataset_path = "{output_base_dir}/{dataset_name}".format(output_base_dir=output_base_dir, dataset_name=dataset_links["name"])
    output_path = Path(output_dataset_path)
    output_path.mkdir(parents=True, exist_ok=True)
    # Build Link Download Status dict
    link_download_status_dict = build_link_status_dict(dataset_links["files"])
    # Continue downloading until download is complete
    download_complete = False
    while(download_complete == False):
        # Attempt to download file set
        download_complete, link_download_status_dict  = download_file_set(dataset_links["name"], link_download_status_dict, output_dataset_path, testing)
        if(download_complete == False):
            # Refresh links
            refreshed_dataset_links = get_links_for_dataset(release_id, dataset_links["name"], api_key)
            # Update link status + url in link status dict
            link_download_status_dict = update_link_status_dict(refreshed_dataset_links["files"], link_status_dict=link_download_status_dict)
    return True

def download_file_set(dataset_name:str, link_download_status_dict: Dict[str, LinkDownloadStatus], output_dataset_path: str, testing: bool=False) -> Tuple[bool, Any]:
    # For each link in dataset
    for i, (filename, link_status_dict) in enumerate(link_download_status_dict.items()):
        if(link_status_dict["complete"] == False):
            part_name = filename
            success = download_data_file_as_parquet(output_dataset_path, filename, link_status_dict["url"])
            if(success == False):
                logging.error("Failed to download {dataset_name} : {part_name} : {url}".format(dataset_name=dataset_name, part_name=part_name, url=link_status_dict["url"]))
                # Exit function if error occured
                return False, link_download_status_dict
            else:
                #key in the link_download_status_dict is a filename, not 'filename': filename_var:{}
                link_download_status_dict[filename]["complete"] = True
        if(testing and i == 0):
            break
    return True, link_download_status_dict

def get_filename_from_url(url: str) -> str:
    """
    URL Name is before the ?, and after the amazon S3 path
    This will split the url into a list of three elements: one before the  final '/', '?',  and one after it.
    """ 
    
    PATTERN = r"(/)([^/]*)(\?)"

    matches = re.findall(PATTERN, url)

    if len(matches) > 0:
        return matches[0][1]
    else:
        # this url did not match the format from before
        logging.info('Failed to get filename from URL: ', url)
        return url.split('?', 1)[0]
        

def build_link_status_dict(file_urls: List[str]) -> Dict[str, LinkDownloadStatus]:
    link_status_dict = dict()
    for url in file_urls:
        # Get filename from url
        filename = get_filename_from_url(url)
        # Set record in dict
        link_status_dict[filename] = {
            "url": url,
            "complete": False
        }
    return link_status_dict

def update_link_status_dict(new_urls: List[str], link_status_dict: Dict[str, LinkDownloadStatus]) -> Dict[str, LinkDownloadStatus]:
    for url in new_urls:
        # Get filename from url
        filename = get_filename_from_url(url)
        # Get link status dict record
        record = link_status_dict[filename]
        # Update record
        if(record["complete"] == False):
            # Update link
            record['url'] = url
    return link_status_dict

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
        #save as pickle, 
        with open(f'{output_base_path_str}/links-{latest_release_id}.pickle', 'wb') as f:
            pickle.dump(dataset_links_list, f)
        
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