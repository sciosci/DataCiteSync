import concurrent.futures
from pathlib import Path
from ftplib import FTP
import argparse, ftplib, logging, gzip, tarfile, sqlite3,time
from datetime import datetime, timezone
from typing import TypedDict, Dict, List
import io
import threading, time
import queue
# Helpder functions for interacting with manifest and keeping this script more readible 
from sync_and_process_scripts.pubmed.version2.v2_manifest_helper_functions import create_or_connect_to_database, batch_upload_to_sql, get_manifest_data_from_first_level_dir 



class FileMetadataContent(TypedDict):
    # file_binary: io.BytesIO
    article_id: str
    article_last_update: datetime
    first_level_folder: str
    second_level_folder: str

def get_current_time():
    return datetime.now()

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


def process_folder(ftp_host, pubmed_dir, first_level_folder, output_dir_path, db_path):
    return
    '''
    NOTE: 
    give every top level folder a sql table, to avoid overuse of memory and sharing information between threads. 
    '''
    ftp = ftplib.FTP(ftp_host)
    ftp.login()
    ftp.cwd(f"{pubmed_dir}/{first_level_folder}")

    output_path_first_level_folder = Path(f'{output_dir_path}/{first_level_folder}')
    output_path_first_level_folder.mkdir(parents=True, exist_ok=True)

    
    sub_folders = ftp.nlst() 
    
    ftp.close()
    # Get existing data from the database
    # SQL Connection to check if files already exist in manifest
    db_conn = sqlite3.connect(db_path)
    # Check the return size of the sql_filtered_table to see 
    sql_filtered_tbl = get_manifest_data_from_first_level_dir(db_conn, first_level_folder=first_level_folder)
    
    
        




def get_second_level_directory_files(output_path,first_level_folder, second_level_folder, sql_filtered_tbl, ftp_host:str='test', pubmed_dir:str='test' ):
    try:
        
        ftp_client = ftplib.FTP(ftp_host)
        ftp_client.login()
        ftp_client.cwd(f"{pubmed_dir}/{first_level_folder}")
        logging.info(f"Entering dir: {output_path}/{first_level_folder}/{second_level_folder}")
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
        # ftp_client.voidcmd('NOOP') 
        # I've tried adding ftp re-connection here, but it would just start a new thread. Perhaps 
        # needs further debugging


def compare_ftp_files_with_manifest(ftp_client, file_list, first_level_folder, second_level_folder,
                                    output_path, file_content_to_manifest,sql_filtered_tbl ):
    for gzip_file in file_list:
            # file_info contais metadata we want
            file_name, file_info = gzip_file
            ftp_client.voidcmd('NOOP')
            if file_info.get('type') == 'file' and file_name.endswith('.tar.gz'):
                # Change to ftp_article_last_modified
                article_last_modified = convert_ftp_file_modify_to_datetime(file_info)
                
                # If FTP file has been updated, write new binary to storage and update manifest
                if file_name in sql_filtered_tbl and article_last_modified > sql_filtered_tbl[file_name]['article_last_update']: 
                    file_contents = write_gzip_data_to_storage(
                    file_name=file_name,
                    # ftp_client=ftp_client,
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
                    # ftp_client=ftp_client,
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
    # ftp_client: ftplib.FTP,
    first_level_folder: str,
    second_level_folder: str,
    output_dir: Path,
    article_last_update, # maybe error handling in get last update isn't needed
    max_retries: int = 3,
    ftp_host: str='ftp.ncbi.nlm.nih.gov',
    starting_pubmed_dir: str='/pub/pmc/oa_package') -> FileMetadataContent:
    
    # We want to return the metadata if the 
    # file download works, thats why I am keeping it here 
    contents: FileMetadataContent =  {
        'article_id': file_name,
        'article_last_update': article_last_update,
        'first_level_folder': first_level_folder,
        'second_level_folder': second_level_folder
    }

    local_path = output_dir / first_level_folder / second_level_folder / file_name
    local_path.parent.mkdir(parents=True, exist_ok=True)
   
    try:
        #connect to an FTP server for every file being written
        ftp_gzip_download = ftplib.FTP(ftp_host)
        # Login anonymous user and password
        ftp_gzip_download.login()
        # cwd is change into /dir
        ftp_gzip_download.cwd(f'{starting_pubmed_dir}/{first_level_folder}/{second_level_folder}')
        
        # writes binary to output path, overrides old binary 
        with open(local_path, 'wb') as f:
            # increased blocksize to try and improve speed
            ftp_gzip_download.retrbinary(f"RETR {file_name}", f.write)
        ftp_gzip_download.quit()
        return contents
    except Exception as e:
        # ftp_client.voidcmd("NOOP")
        logging.error(f"[Attempt /{max_retries}] Error retrieving {file_name}: {e}")
        # Attempt to reconnect here
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



def compare_L2_ftp_files_with_manifest(sub_dir_record, output_path, file_content_to_manifest, sql_filtered_tbl ):
    for obj in sub_dir_record:
        first_level_folder = obj['first_level_dir']
        second_level_folder = obj['second_level_dir']
        file_list = obj['file_list']
        
        for gzip_file in file_list:
                # file_info contais metadata we want
                file_name, file_info = gzip_file
                if file_info.get('type') == 'file' and file_name.endswith('.tar.gz'):
                    # Change to ftp_article_last_modified
                    article_last_modified = convert_ftp_file_modify_to_datetime(file_info)
                    
                    # If FTP file has been updated, write new binary to storage and update manifest
                    if file_name in sql_filtered_tbl and article_last_modified > sql_filtered_tbl[file_name]['article_last_update']: 
                        logging.info(f'file: {file_name} needs updating')
                        file_contents = write_gzip_data_to_storage(
                        file_name=file_name,
                        # ftp_client=ftp_client,
                        first_level_folder=first_level_folder,  # Hard-coded for demonstration
                        second_level_folder=second_level_folder,
                        output_dir=output_path,
                        article_last_update= article_last_modified)
                        
                        
                        file_content_to_manifest.append(file_contents)
                        
                    # else file name not in manifest, writ to storage as well since it is a new file not currently tracked
                    elif file_name not in sql_filtered_tbl:
                        file_contents = write_gzip_data_to_storage(
                        file_name=file_name,
                        # ftp_client=ftp_client,
                        first_level_folder=first_level_folder,  # Hard-coded for demonstration
                        second_level_folder=second_level_folder,
                        output_dir=output_path,
                        article_last_update= article_last_modified)
                        
                        
                        file_content_to_manifest.append(file_contents)
                    # else everything is up to date, continue
        # return file information to update manifest    
    return file_content_to_manifest



def main() -> None:
    ftp_host = 'ftp.ncbi.nlm.nih.gov'
    starting_pubmed_dir = '/pub/pmc/oa_package'
    
    output_folder = Path('./output')
    output_folder.mkdir(parents=True, exist_ok=True)
    db_path = output_folder / 'manifest.db'
    logger = setup_logger(output_folder)
    
    create_or_connect_to_database(db_path)
    
    #Connect to ftp to get first_level_folders
    #top_level_folders = get_first_level_dir(ftp_server=ftp_host, starting_ftp_directory = starting_pubmed_dir)
    ftp_client = ftplib.FTP(ftp_host)
    # Login anonymous user and password
    ftp_client.login()
    # cwd is change into /dir
    ftp_client.cwd(starting_pubmed_dir)
    files = []
    first_folders = ftp_client.nlst()
    # ftp.quit()
    # print(first_folders)
    all_records = []
    start = time.time()
    
    # with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor: 
    for first_level_folder in first_folders[:16]:
        #threadpool 
        # navigate into sub folder
        ftp_client.cwd(f"/pub/pmc/oa_package/{first_level_folder}")
        # ftp_client.cwd(first_level_folder)
        # print out files in subfolder with their metadata 
        second_level_folders = ftp_client.nlst()
        for second_level_folder in second_level_folders:
            ftp_client.cwd(f"/pub/pmc/oa_package/{first_level_folder}/{second_level_folder}")
            # ftp_client.cwd(second_level_folder)
            a = list(ftp_client.mlsd())
            # prints out a list of tuples
            sub_dir_records = {
                'first_level_dir': first_level_folder,
                'second_level_dir': second_level_folder,
                'file_list': a}
            all_records.append(sub_dir_records)
        # navigate out of sub_folder for next loop
        # ftp_client.cwd('..')
    
    # print(all_records[0])
    
    #closing the FTP
    ftp_client.close()
    # get the sql filtered table
    db_conn = sqlite3.connect(db_path)
    sql_filtered_tbl = get_manifest_data_from_first_level_dir(db_conn, first_level_folder='00')    
    
    # check the values in output list of dict against manifest current state
    compare_L2_ftp_files_with_manifest(sub_dir_record=all_records, output_path=output_folder, file_content_to_manifest=[], sql_filtered_tbl=sql_filtered_tbl )    
    end = time.time()
    print(f"Time elapsed : {end - start}, ({len(all_records)} L2 records)")

if __name__ == '__main__':
    main()
