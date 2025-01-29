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
import argparse, ftplib, logging, gzip, tarfile
from datetime import datetime, timezone
from typing import TypedDict, Dict, List
import io
import threading, time
import queue
from manifest_helper_functions import create_database, batch_upload_to_sql 



def get_current_time():
    return datetime.now()

class FileMetadataContent(TypedDict):
    # file_binary: io.BytesIO
    article_id: str
    downloaded_at: datetime
    first_level_folder: str
    second_level_folder: str
    
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


def process_folder(ftp_host, pubmed_dir, first_level_folder, output_dir_path, q: queue.Queue):
    ftp = ftplib.FTP(ftp_host)
    ftp.login()
    ftp.cwd(f"{pubmed_dir}/{first_level_folder}")

    output_path_first_level_folder = Path(f'{output_dir_path}/{first_level_folder}')
    output_path_first_level_folder.mkdir(parents=True, exist_ok=True)
    
    # sub_folders = []
    # for name, facts in ftp.mlsd():
    #     if facts['type'] == 'dir' and name not in ('.', '..'):
    #         sub_folders.append(name) 
    # # logging.info(sub_folders)
    sub_folders = ftp.nlst() 
    
    for name in sub_folders:
        get_second_level_directory_files(ftp_client=ftp, output_path=output_dir_path, first_level_folder=first_level_folder,
                                        second_level_folder= name)




def get_second_level_directory_files(ftp_client, output_path,first_level_folder, second_level_folder):
    try:
        logging.info(f"Entering {output_path}/{first_level_folder}/{second_level_folder}")
        ftp_client.cwd(second_level_folder)
        
        file_content_to_storage = []
        ftp_client.voidcmd('NOOP')
        
        file_list = list(ftp_client.mlsd())    
        
        for gzip_file in file_list:
            # file_info contais metadata we want
            file_name, file_info = gzip_file
            ftp_client.voidcmd('NOOP')
            if file_info.get('type') == 'file' and file_name.endswith('.tar.gz'):
                
                file_contents = write_gzip_data_to_storage(
                file_name=file_name,
                ftp_client=ftp_client,
                first_level_folder=first_level_folder,  # Hard-coded for demonstration
                second_level_folder=second_level_folder,
                output_dir=output_path)
                
                ftp_client.voidcmd('NOOP')
                
                file_content_to_storage.append(file_contents)
        
        logging.info(f"Exiting {output_path}/{first_level_folder}/{second_level_folder}")
        ftp_client.cwd('..')
        
        return file_content_to_storage
    except Exception as e:
        logging.error(f'Error {e}: in {first_level_folder}/{second_level_folder}')
        ftp_client.voidcmd('NOOP') 


def compare_ftp_files_with_manifest():
    pass





def write_gzip_data_to_storage(
    file_name: str,
    ftp_client: ftplib.FTP,
    first_level_folder: str,
    second_level_folder: str,
    output_dir: Path,
    max_retries: int = 3) -> FileMetadataContent:

    local_path = output_dir / first_level_folder / second_level_folder / file_name
    local_path.parent.mkdir(parents=True, exist_ok=True)

    contents: FileMetadataContent =  {
        'article_id': file_name,
        'downloaded_at': get_current_time(),
        'first_level_folder': first_level_folder,
        'second_level_folder': second_level_folder
    } 

   
    try:
        ftp_client.voidcmd('NOOP')
        with open(local_path, 'wb') as f:
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



def main() -> None:
    ftp_host = 'ftp.ncbi.nlm.nih.gov'
    starting_pubmed_dir = '/pub/pmc/oa_package'
    
    output_folder = Path('./output')
    output_folder.mkdir(parents=True, exist_ok=True)
    logger = setup_logger(output_folder)
    
    #Connect to ftp to get first_level_folders
    top_level_folders = get_first_level_dir(ftp_server=ftp_host, starting_ftp_directory = starting_pubmed_dir)

    # We will use this to write to SQL 
    q = queue.Queue()
    for first_level_folder in top_level_folders:
        process_folder(ftp_host, starting_pubmed_dir, first_level_folder, output_folder,q)
        
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
