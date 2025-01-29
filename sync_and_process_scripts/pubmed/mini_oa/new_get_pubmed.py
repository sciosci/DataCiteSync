"""
Notes from Jan 28th, 2025
 - Added strict python type checking
 - retrBinary read binary (rb) is unnecesary, write binary 'wb' can download to output folder and 
 overrides old files so it takes care of tracking old files in memory to be re-written, as well as removing 
 write_binary_to_storage. ftp.mlsd() returns file_name as well as necessary metadata for tracking file versions. 
 - Moved manifest functions to a manifest_helper_functions script to reduce clutter in a single file. 
 - Using queue.Queue() to create a thread-safe object where I 
    place file_metadata that will be written to manifest.  
 
 - What needs improving:
    (All tests are run with grobid, not local machine)
    - With current threadpool setup, when I use multiple threads, the FTP object disconnects,
    closes current thread, then begins a new thread in a seperate first level folder. I have inspected CPU usage
     using `htop` while running script with up to 10 threads and CPU utilization is never above 10%.
    I do not get this issue when I use 1 thread, so current file can use 1 or many threads depending on what is commented
    out in the main() function
"""

import concurrent.futures
from pathlib import Path
from ftplib import FTP
import argparse, ftplib, logging, gzip, tarfile, sqlite3
from datetime import datetime, timezone
from typing import TypedDict, Dict, List
import io
import threading, time
import queue
# Helpder functions for interacting with manifest and keeping this script more readible 
from manifest_helper_functions import create_or_connect_to_database, batch_upload_to_sql, get_manifest_data_from_first_level_dir 



def get_current_time():
    return datetime.now()

class FileMetadataContent(TypedDict):
    # file_binary: io.BytesIO
    article_id: str
    article_last_update: datetime
    first_level_folder: str
    second_level_folder: str

def convert_ftp_file_modify_to_datetime(file_info):

    # Parse the string into a datetime object
    if isinstance(file_info, dict) and 'modify' in file_info:
        try:
            dt = datetime.strptime(file_info['modify'], '%Y%m%d%H%M%S')
            return dt
        except ValueError:
            return 'Invalid date format'
    else:
        return 'Date not found'
    
def get_files_in_directory(ftp) -> list[str]:
    """
    Returns a list of directory (or file) names using ftp.mlsd() which yields (name, facts).
    """
    files = []
    files = ftp.nlst()
    
    return files


def get_first_level_dir(ftp_server:str, starting_ftp_directory:str)-> list[str]:
    ftp = ftplib.FTP(ftp_server)
    # Login anonymous user and password
    ftp.login()
    # cwd is change into /dir
    ftp.cwd(starting_ftp_directory)
    files = []
    files = ftp.nlst()
    ftp.quit()
    return files


def process_folder(ftp_host, pubmed_dir, first_level_folder, output_dir_path, q: queue.Queue, db_path):
    ftp = ftplib.FTP(ftp_host)
    ftp.login()
    ftp.cwd(f"{pubmed_dir}/{first_level_folder}")

    output_path_first_level_folder = Path(f'{output_dir_path}/{first_level_folder}')
    output_path_first_level_folder.mkdir(parents=True, exist_ok=True)
    
    
    sub_folders = ftp.nlst() 
    # Get existing data from the database
    # SQL Connection to check if files already exist in manifest
    db_conn = sqlite3.connect(db_path)
    sql_filtered_tbl = get_manifest_data_from_first_level_dir(db_conn, first_level_folder=first_level_folder)
    
    for name in sub_folders:
        manifest_updates = get_second_level_directory_files(ftp_client=ftp, output_path=output_dir_path, first_level_folder=first_level_folder,
                                        second_level_folder= name, sql_filtered_tbl=sql_filtered_tbl)
        # Put list of dictionaries to be written as a single item in queue
        # for writing to manifest
        q.put(manifest_updates)




def get_second_level_directory_files(ftp_client, output_path,first_level_folder, second_level_folder, sql_filtered_tbl):
    try:
        logging.info(f"Entering {output_path}/{first_level_folder}/{second_level_folder}")
        ftp_client.cwd(second_level_folder)
        
        file_content_to_manifest = []
        ftp_client.voidcmd('NOOP')
        
        file_list = list(ftp_client.mlsd())    
        
        # Compare current state ftp with current state oa_package, 
        file_content_to_manifest = compare_ftp_files_with_manifest(ftp_client=ftp_client, file_list=file_list, 
                                        first_level_folder=first_level_folder, second_level_folder = second_level_folder,
                                        output_path= output_path, file_content_to_manifest = file_content_to_manifest, 
                                        sql_filtered_tbl=sql_filtered_tbl)
        
        logging.info(f"Exiting {output_path}/{first_level_folder}/{second_level_folder}")
        ftp_client.cwd('..')
        
        return file_content_to_manifest
    except Exception as e:
        logging.error(f'Error {e}: in {first_level_folder}/{second_level_folder}')
        ftp_client.voidcmd('NOOP') 
        # I've tried adding ftp re-connection here, but it would just start a new thread. Perhaps 
        # needs further debugging


def compare_ftp_files_with_manifest(ftp_client, file_list, first_level_folder, second_level_folder,
                                    output_path, file_content_to_manifest,sql_filtered_tbl ):
    for gzip_file in file_list:
            # file_info contais metadata we want
            file_name, file_info = gzip_file
            ftp_client.voidcmd('NOOP')
            if file_info.get('type') == 'file' and file_name.endswith('.tar.gz'):
                article_last_modified = convert_ftp_file_modify_to_datetime(file_info)
                
                # If FTP file has been updated, write new binary to storage and update manifest
                if file_name in sql_filtered_tbl and article_last_modified > sql_filtered_tbl[file_name]['article_last_update']: 
                    file_contents = write_gzip_data_to_storage(
                    file_name=file_name,
                    ftp_client=ftp_client,
                    first_level_folder=first_level_folder,  # Hard-coded for demonstration
                    second_level_folder=second_level_folder,
                    output_dir=output_path,
                    article_last_update= article_last_modified)
                    
                    ftp_client.voidcmd('NOOP')
                    
                    file_content_to_manifest.append(file_contents)
                # else file name not in manifest, writ to storage as well since it is a new file not currently tracked
                elif file_name not in sql_filtered_tbl:
                    file_contents = write_gzip_data_to_storage(
                    file_name=file_name,
                    ftp_client=ftp_client,
                    first_level_folder=first_level_folder,  # Hard-coded for demonstration
                    second_level_folder=second_level_folder,
                    output_dir=output_path,
                    article_last_update= article_last_modified)
                    
                    ftp_client.voidcmd('NOOP')
                    
                    file_content_to_manifest.append(file_contents)
                    
                # else everything is up to date, continue
    
    # return file information to update manifest    
    return file_content_to_manifest




def write_gzip_data_to_storage(
    file_name: str,
    ftp_client: ftplib.FTP,
    first_level_folder: str,
    second_level_folder: str,
    output_dir: Path,
    article_last_update, # maybe error handling in get last update isn't needed
    max_retries: int = 3) -> FileMetadataContent:

    local_path = output_dir / first_level_folder / second_level_folder / file_name
    local_path.parent.mkdir(parents=True, exist_ok=True)

    contents: FileMetadataContent =  {
        'article_id': file_name,
        'article_last_update': article_last_update,
        'first_level_folder': first_level_folder,
        'second_level_folder': second_level_folder
    } 
   
    try:
        ftp_client.voidcmd('NOOP')
        # writes binary to output path, overrides old binary 
        with open(local_path, 'wb') as f:
            # increased blocksize to try and improve speed
            ftp_client.retrbinary(f"RETR {file_name}", f.write, blocksize=100000)
        ftp_client.voidcmd('NOOP') 
        return contents
    except Exception as e:
        ftp_client.voidcmd("NOOP")
        logging.error(f"[Attempt /{max_retries}] Error retrieving {file_name}: {e}")
        # Attempt to reconnect if we haven't maxed out
        return contents


def setup_logger(output_folder):
    # Configure the logging
    logging.basicConfig(
        level=logging.INFO,  # Set the logging level
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Format of log messages
        handlers=[
            logging.FileHandler(output_folder / "app.log"),  # Log to a file
            logging.StreamHandler()  # Also log to console
        ]
    )
    return logging.getLogger("MyLogger")  # Return a logger instance


 
def consumer_thread(q, db_path):
    """
    Writes article metadata to database in seperate thread from process_folder
    """
    while True:
        updates_list = q.get()  # blocks until there's something to process
        if updates_list is None:
            # Sentinel value => exit the loop
            q.task_done()
            break
        
        # Persist the batch of metadata
        batch_upload_to_sql(updates_list, db_path)
        
        # Mark this task as complete
        q.task_done()

    print("[Consumer] Exiting normally.")




def main() -> None:
    ftp_host = 'ftp.ncbi.nlm.nih.gov'
    starting_pubmed_dir = '/pub/pmc/oa_package'
    
    output_folder = Path('./output')
    output_folder.mkdir(parents=True, exist_ok=True)
    db_path = output_folder / 'manifest.db'
    logger = setup_logger(output_folder)
    
    create_or_connect_to_database(db_path)
    
    #Connect to ftp to get first_level_folders
    top_level_folders = get_first_level_dir(ftp_server=ftp_host, starting_ftp_directory = starting_pubmed_dir)

    # We will use this to write to SQL 
    q = queue.Queue()
    
    consumer = threading.Thread(target=consumer_thread, args=(q, db_path), daemon=True)
    consumer.start() 
    for first_level_folder in top_level_folders:
        process_folder(ftp_host, starting_pubmed_dir, first_level_folder, output_folder,q, db_path)
        
    # There is something happening where the pipe breaks with multiple threads and begins 
    # a new thread in a new first_level_folder
    # with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
    #     futures = []
    #     for folder_name in top_level_folders:
    #         futures.append(
    #             executor.submit(process_folder, ftp_host, starting_pubmed_dir, folder_name, output_folder, q)
    #         )
    #     # Wait for all folder downloads to finish
    #     concurrent.futures.wait(futures)

    q.join()
    logger.info("All downloads and writes complete.")

if __name__ == '__main__':
    main()
