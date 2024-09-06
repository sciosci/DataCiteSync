import boto3
from pathlib import Path
from botocore import UNSIGNED
from botocore.client import Config
from typing import Optional, TypedDict, List, Set

class FileAndFolderDict(TypedDict):
    filenames: List[str]
    folders: Set[str]


def get_folder_from_path(in_path: str) -> Optional[str]:
    segments = in_path.split("/")
    if(len(segments) > 1):
        return "/".join(segments[:-1])
    return None


def get_files_and_folder_names(client, bucket: str, prefix: str) -> FileAndFolderDict:
    """
    A function to get all the file and folder names from a s3 bucket
    """
    filenames = list()
    folders = set()

    default_kwargs = {
        "Bucket": bucket,
        "Prefix": prefix
    }
    next_token = ""

    while next_token is not None:
        updated_kwargs = default_kwargs.copy()
        if next_token != "":
            updated_kwargs["ContinuationToken"] = next_token
        
        response = client.list_objects_v2(**updated_kwargs)
        contents = response.get("Contents")

        for result in contents:
            key = result.get("Key")
            if(key[-1] == "/"):
                folders.add(key)
            else:
                extracted_folder_name = get_folder_from_path(key)
                if(extracted_folder_name != None):
                    folders.add(extracted_folder_name)
                filenames.append(key)

        next_token = response.get("NextContinuationToken")

    return FileAndFolderDict(filenames=filenames, folders=folders)


def download_data_from_openalex_s3(client, bucket_name: str, prefix: str, base_output_path: str):
    #   Get file_names and folders
    #       bucket name s3://openalex
    #       folder name openalex-snapshot
    files_and_folder_dict = get_files_and_folder_names(client, bucket_name, prefix)

    #   Make output directory
    for folder in files_and_folder_dict["folders"]:
        folder_path = Path(f"{base_output_path}/{folder}")
        folder_path.mkdir(parents=True, exist_ok=True)

    #   Download each file into the appropriate directory
    for file_entry in files_and_folder_dict["filenames"]:
        output_path = f"{base_output_path}/{file_entry}"
        client.download_file(
            bucket_name,
            file_entry,
            output_path
        )
    return


def main():
    BUCKET_NAME = "openalex"
    BUCKET_PREFIX = ""
    base_output_path = "REPLACE_WITH_PATH_TO_STORE_DATA_FROM_S3"

    # Initiate connection to OpenAlex S3 bucket
    client = boto3.client("s3", config=Config(signature_version=UNSIGNED))
    
    # Download data from OpenAlex S3 on to local disk
    download_data_from_openalex_s3(client, BUCKET_NAME, BUCKET_PREFIX, base_output_path)
    
    # Preprocess downloaded data

    # Upload to Petalibrary

    # Cleanup data files on grobid

    # Add sync record to SQLite database on grobid

    # If failed, retry in __ minutes, ...
    

if __name__ == "__main__":
    main()