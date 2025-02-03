import concurrent.futures
from pathlib import Path
from ftplib import FTP
import argparse, ftplib, logging, gzip, tarfile, sqlite3,time
from datetime import datetime, timezone
from typing import TypedDict, Dict, List
import io
import threading, time
# Helpder functions for interacting with manifest and keeping this script more readible 
from manifest_helper_functions import create_or_connect_to_database, batch_upload_to_sql, get_manifest_data_from_first_level_dir 

class FileMetadataContent(TypedDict):
    # file_binary: io.BytesIO
    article_id: str
    article_last_update: str
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
    


def process_folder(first_level_folder, db_path, output_folder,  ftp_host:str='ftp.ncbi.nlm.nih.gov', pubmed_dir:str='pub/pmc/oa_package', ):
    """
    Still sharing a manifest.db, need to decouple to give every first level folder its own directory 
    """
    all_records = []
    ftp_client = ftplib.FTP(ftp_host)
    ftp_client.login()
    logging.info(f' Entering {first_level_folder}')
    ftp_client.cwd(f"{pubmed_dir}/{first_level_folder}")
    
    # print out files in subfolder with their metadata 
    second_level_folders = ftp_client.nlst()
    for second_level_folder in second_level_folders:
        try: 
            ftp_client.cwd(f"/pub/pmc/oa_package/{first_level_folder}/{second_level_folder}")
            # ftp_client.cwd(second_level_folder)
            a = list(ftp_client.mlsd())
            # prints out a list of tuples
            sub_dir_records = {
                'first_level_dir': first_level_folder,
                'second_level_dir': second_level_folder,
                'file_list': a}
            all_records.append(sub_dir_records)
        except Exception as E:
            logging.error(f'Error in {first_level_folder}/{second_level_folder}')
            
    # navigate out of sub_folder for next loop
        
    ftp_client.quit()
    db_conn = sqlite3.connect(db_path)
    sql_filtered_tbl = get_manifest_data_from_first_level_dir(db_conn, first_level_folder=first_level_folder)    
    db_conn.close()
    # check the values in output list of dict against manifest current state, write to output as needed
    files_write_to_db = compare_ftp_files_with_manifest(sub_dir_record=all_records, output_path=output_folder, file_content_to_manifest=[], sql_filtered_tbl=sql_filtered_tbl )  
    logging.info(f'Exiting {first_level_folder}')
    
    batch_upload_to_sql(pubmed_result_data=files_write_to_db, db_path=db_path)
    
    return True
        


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


def compare_ftp_files_with_manifest(sub_dir_record, output_path, file_content_to_manifest, sql_filtered_tbl ):
    for obj in sub_dir_record:
        first_level_folder = obj['first_level_dir']
        second_level_folder = obj['second_level_dir']
        file_list = obj['file_list']
        
        for gzip_file in file_list:
                # file_info contais metadata we want
                try:
                    file_name, file_info = gzip_file
                    if file_info.get('type') == 'file' and file_name.endswith('.tar.gz'):
                        # Change to ftp_article_last_modified
                        article_last_modified = convert_ftp_file_modify_to_datetime(file_info)
                        logging.info(sql_filtered_tbl)
                        print(sql_filtered_tbl)
                        
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
                except Exception as e:
                    logging.error('Error in compare L2 ftp files with manifest')
                    # else everything is up to date, continue
        # return file information to update manifest    
    return file_content_to_manifest



def main() -> None:
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="""
    Full download of latest oa_subset from pubmed FTP server 
""")
    
    parser.add_argument(
        "-o", "--output_dir", help="Output base directory of downloaded files"
    )
    parser.add_argument(
        "--ftp_host", help="FTP host endpoint" 
    )
    
    parser.add_argument(
        "--starting_pubmed_dir", help="Starting directory for openAccess subset"
    )

    arguments = parser.parse_args()
    
    ftp_host = arguments.ftp_host
    starting_pubmed_dir = arguments.starting_pubmed_dir
    
    # output folder string to path object
    output_folder = Path(arguments.output_dir) 
    output_folder.mkdir(parents=True, exist_ok=True)
    db_path = output_folder / 'manifest.db'
    
    logger = setup_logger(output_folder)
    
    create_or_connect_to_database(db_path)
    
    #Connect to ftp to get first_level_folders
    ftp_client = ftplib.FTP(ftp_host)
    # Login anonymous user and password
    ftp_client.login()
    # cwd is change into /dir
    ftp_client.cwd(starting_pubmed_dir)
    files = []
    first_level_folders = ftp_client.nlst()[:16]
    
    all_records = []
    start = time.time()
    #closing the FTP
    ftp_client.close() 
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor: 
        future_to_folder = {
            executor.submit(
                process_folder, folder, db_path, output_folder               
            ): folder for folder in first_level_folders
        }
        for future in concurrent.futures.as_completed(future_to_folder):
            folder = future_to_folder[future]
            try:
                result = future.result()
                if result:
                    logging.info(f"Successfully processed folder {folder}, {future.result()} \n \n")
                else:
                    logging.error(f"Failed to process folder {folder} \n, {future.result()} ")
            except Exception as exc:
                logging.error(f"Exception generated while processing folder {folder}: {exc}") 
    
       
    end = time.time()
    print(f"Time elapsed : {end - start}, ({len(all_records)} L2 records)")

if __name__ == '__main__':
    main()
