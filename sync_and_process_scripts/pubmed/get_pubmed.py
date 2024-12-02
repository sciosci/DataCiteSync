'''
Notes on December 2nd:

RAM is not the issue, grobid can handle the information for many files at once, 
Current issue's I am facing. 
- [X] File Binary is not writing

Steps Algorithm needs to complete before considered finished
- [ ] checking if the file existing in output directory and manifest needs to be updated
- [X] Bulk Write to SQL
- [X] Manifest Updates 
'''

# imports here
import time, logging, ftplib, tarfile, gzip, io
from pathlib import Path
import sqlite3
import datetime
import argparse #
import concurrent.futures
from typing import List
from io import BytesIO


def connect_to_pubmed(ftp_server:str, starting_ftp_directory:str)->object:
    ftp = ftplib.FTP(ftp_server)
    # Login anonymous user and password
    ftp.login()
    # cwd is change into /dir
    ftp.cwd(starting_ftp_directory)
    return ftp

def get_first_level_directory(ftp)->list[str]:
    # filenames = ftp.nlst()
    files = []
    # get filenames
    ftp.retrlines('NLST',files.append)
    # 
    return files


def convert_ftp_file_modify_to_datetime(file_info)-> str:

    # Parse the string into a datetime object]
    if file_info is object:
        dt = datetime.strptime(file_info.modify, '%Y%m%d%H%M%S')
        # Convert to ISO 8601 format
        return dt.isoformat()
    else:
        return None



def traverse_second_level_directory(ftp_path, output_path, first_level_folder, sql_filtered_tbl, ftp, second_level_dir_name )->list[str]:
    '''
    This function we want check multiple things: 
       1. Which files are in the database -> Pass over them
       2. Which files are NOT in the database -> Add themn to files_to_be_written, and write to output dir
       3. Which files are in database but have old data -> Add themn to files_to_be_written, and write to output dir

    Files will be written to database AND outputdirectory after individual threads complete in batch, no writing will occur during this function.
    '''
    # enter into the second level directory
    files_to_be_written: List[object] = [] 
    zip_files_in_ftp_directory = []

    ftp.cwd(f"{ftp_path}/{first_level_folder}/{second_level_dir_name}")
    path_to_second_level_folder = Path(f"{output_path}/{first_level_folder}/{second_level_dir_name}") 
    path_to_second_level_folder.mkdir(parents=True, exist_ok=True)
    
    # list of zip files in the current directory here 
    logging.info(f'Entered {first_level_folder}/{second_level_dir_name}')
    ftp.retrlines('NLST',zip_files_in_ftp_directory.append)
    for gzip_file in ftp.mlsd():
        # iterate through list 
        try:
            file_name, file_info = gzip_file

            # some entries from the ftp server can be navigation so we filter those out
            if file_info.get('type') == 'file' and file_name.endswith('.tar.gz'):
                # get the modified date 
                article_last_modified = convert_ftp_file_modify_to_datetime(file_info) 
            # perform an if check here on file information modify
                gzip_meta = get_gzip_data_in_memory(path_to_second_level_folder, file_name, ftp, ftp_path, first_level_folder, second_level_dir_name, article_last_modified)
                files_to_be_written.append(gzip_meta)
            # if the table is completely empty (first time running) 
            if not sql_filtered_tbl:
               pass
               
        except Exception as exc:
            logging.info(f'Error in traverse second level directory: {exc}') 
    logging.info(f'Exiting {first_level_folder}/{second_level_dir_name}') 
    ftp.cwd('..')
    return files_to_be_written


def process_folder(ftp_host, pubmed_dir, first_level_folder, output_dir_path, db_path):
    """
    Worker function to process second-level directories in batches.
    """
    try:
        # Each thread creates its own FTP connection
        ftp = ftplib.FTP(ftp_host)
        ftp.login()
        ftp.cwd(f"{pubmed_dir}/{first_level_folder}")

        # Create the output folder if it does not already exist
        output_path_first_level_folder = Path(f'{output_dir_path}/{first_level_folder}')
        output_path_first_level_folder.mkdir(parents=True, exist_ok=True)

        db_conn = sqlite3.connect(db_path)

        # Get existing data from the database
        sql_filtered_tbl = get_manifest_data_from_first_level_dir(db_conn, first_level_folder=first_level_folder)

        # Get the list of second-level directories
        items = ftp.nlst()

        batch_size = 2  # Number of second-level directories per batch

        # Process items in batches, reducing memory and speeding up the script
        for i in range(0, len(items), batch_size):
            batch_items = items[i:i+batch_size]
            queue = []

            for item in batch_items:
                # Collect data from each second-level directory
                second_dir_output_temp = traverse_second_level_directory(
                    pubmed_dir, output_dir_path, first_level_folder,
                    sql_filtered_tbl, ftp=ftp, second_level_dir_name=item
                )
                queue.extend(second_dir_output_temp)

            # After collecting data from the batch, write to output and upload to SQL
            write_file_to_output_dir(queue)
            batch_upload_to_sql(queue, db_path)

            # Clear the queue
            queue.clear()

        ftp.quit()  # Close the connection
        return True

    except Exception as e:
        logging.error(f"Error processing folder {first_level_folder}: {e}")
        return False



def get_manifest_data_from_first_level_dir(db_conn, first_level_folder):
    try:
        # Enable dictionary-like access for rows
        db_conn.row_factory = sqlite3.Row
        cursor = db_conn.cursor()
        
        # Parameterized query to prevent SQL injection
        query = "SELECT article_last_update FROM articles_metadata WHERE first_level_dir = ?"
        cursor.execute(query, (first_level_folder,))
        return cursor.fetchall()
    except Exception as e:
        logging.info(f'Error in get Manifest data thread:{e}' )    


def create_database(db_path)-> None:
    '''
    Connect to the sqLite database (or create it if it doesn’t exist)
    '''
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()


    # Create the table if non-existent
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS articles_metadata (
        article_id TEXT NOT NULL,
        article_last_update TEXT,
        downloaded_at TEXT,
        first_level_dir TEXT,
        second_level_dir TEXT,
        image_count INTEGER,
        xml_count INTEGER,
        pdf_count INTEGER
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS pubmed_runtime_data (
        date_execution TEXT NOT NULL,
        run_duratio TEXT,
        downloaded_at TEXT,
        first_level_dir TEXT,
        second_level_dir TEXT,
        image_count INTEGER,
        xml_count INTEGER,
        pdf_count INTEGER
    )
    ''')

    # Commit changes 
    conn.commit()
    conn.close()
    

def get_gzip_data_in_memory(path_to_folder, file_name, ftp, ftp_path, first_level_folder, second_level_folder, article_last_modified)->object:
# Create the contents dictionary
    contents = {
        'file_binary': None,
        'path_to_folder': path_to_folder,
        'article_id': file_name,
        'article_last_update': article_last_modified,  # Update if you have this info
        'downloaded_at': get_current_time(),
        'first_level_folder': first_level_folder,
        'second_level_folder':second_level_folder,
        'image_count': 0,
        'xml_count': 0,
        'pdf_count': 0,
        'other_files': 0 
    }
    try:
        # Define the local file path
        # local_file_path = Path(f'{path_to_folder}/{file_name}')

        # Open a local file for writing binary data
        # with open(local_file_path, 'wb') as local_file:
        #     # Download the file and write it directly to disk
        #     ftp.retrbinary(f'RETR {file_name}', local_file.write)
        # Use BytesIO to store file content in memory
        
        # storing the binary in a data structure
        file_data = io.BytesIO()
        file_ftp_path = f'{ftp_path}/{first_level_folder}/{second_level_folder}/{file_name}'
        ftp.retrbinary(f"RETR {file_ftp_path}", file_data.write)
        file_data.seek(0)  # Reset pointer to the start of the BytesIO object
        contents['file_binary'] = file_data

        # Open the downloaded .tar.gz file
        with gzip.open(file_data, 'rb') as gz_file:
            with tarfile.open(fileobj=gz_file) as tar:
                for member in tar.getmembers():
                    if member.isfile():
                        filename_lower = member.name.lower()
                        if filename_lower.endswith('.pdf'):
                            contents['pdf_count'] += 1
                        elif filename_lower.endswith('.xml'):
                            contents['xml_count'] += 1
                        elif filename_lower.endswith(('.jpg', '.jpeg', '.png', '.gif')):
                            contents['image_count'] += 1
                        else:
                            contents['other_files'] += 1

        # Optionally, store or log the contents
        # print(f"Processed '{file_name}': {contents}")
        return contents
        # reconnect here
    except Exception as e:
        print(f"Error processing '{first_level_folder}/{second_level_folder}/{file_name}': {e}")




def get_current_time():
    """Returns the current time."""
    return datetime.datetime.now().isoformat()

def setup_logger():
    # Configure the logging
    logging.basicConfig(
        level=logging.INFO,  # Set the logging level
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Format of log messages
        handlers=[
            logging.FileHandler("output_dir/app.log"),  # Log to a file
            logging.StreamHandler()  # Also log to console
        ]
    )
    return logging.getLogger("MyLogger")  # Return a logger instance


def write_file_to_output_dir(pubmed_result_data):
    '''
    Writing the file binary to the output directories. 
    Only to be done if files do not yet exist in the database or if they 
    need to be updated. 
    '''
    logging.info(f"pubmed result data array of objects:  {pubmed_result_data} ")
    for obj in pubmed_result_data:
        
        try: 
            if obj is None:
                logging.error("Encountered NoneType object, skipping...")
                continue

            if not isinstance(obj, dict):
                logging.error(f"Unexpected object type: {type(obj)}, skipping...")
                continue
            
            file_binary = obj['file_binary']  # Binary data of the tar.gz file
            path_to_folder = obj['path_to_folder']  # The PosixPath object for the folder
            file_name = obj['article_id']  # Name of the tar.gz file
            
            # Ensure the output folder exists
            path_to_folder.mkdir(parents=True, exist_ok=True)
            
            # Define the file path for the .tar.gz file
            tar_file_path = path_to_folder / file_name
            
            # Reset the pointer of the file_binary object to the beginning
            if hasattr(file_binary, "seek"):
                file_binary.seek(0)
            
            # Write the binary data to the .tar.gz file
            with tar_file_path.open("wb") as file:
                file.write(file_binary.read())
        except Exception as e:
           logging.error(f"Error writing file {obj} to output dir: {e}")


def batch_upload_to_sql(pubmed_result_data, db_path):
    """
    Efficiently batch upload data to the SQL table. If the article_id exists,
    override the old value with the new data; otherwise, insert the new data.
    """
    if not pubmed_result_data:
        logging.info("No data to upload to SQL.")
        return

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Use a transaction for batch operation
    conn.execute("BEGIN TRANSACTION")
    try:
        for obj in pubmed_result_data:
            if obj is None:
                continue

            article_id = obj['article_id']
            article_last_update = obj['article_last_update']
            downloaded_at = obj['downloaded_at']
            first_level_dir = obj.get('first_level_folder', 'unknown')
            second_level_dir = obj.get('second_level_folder', 'unknown')
            image_count = obj['image_count']
            xml_count = obj['xml_count']
            pdf_count = obj['pdf_count']
            
            # Ensure article_id is not None
            if not article_id:
                logging.warning("Skipping entry with missing article_id.")
                continue

            # Use INSERT with ON CONFLICT clause
            cursor.execute('''
            INSERT INTO articles_metadata (
                article_id, article_last_update, downloaded_at,
                first_level_dir, second_level_dir,
                image_count, xml_count, pdf_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                article_id, article_last_update, downloaded_at,
                first_level_dir, second_level_dir,
                image_count, xml_count, pdf_count
            ))
        
        # Commit the transaction
        conn.commit()
    except sqlite3.DatabaseError as e:
        # Rollback if there is any issue
        conn.rollback()
        logging.error(f"Database error during batch upload: {e}")
        raise
    finally:
        # Close the connection
        conn.close()
        


def main():
    # Setup FTP connection and logger
    ftp_host = 'ftp.ncbi.nlm.nih.gov'
    starting_pubmed_dir = r'/pub/pmc/oa_package'

    output_folder = Path('./output_dir')
    db_path = output_folder / 'manifest.db'
    output_folder.mkdir(parents=True, exist_ok=True)
    logger = setup_logger()

    # Initial FTP connection to get first-level folders
    ftp = connect_to_pubmed(ftp_server=ftp_host, starting_ftp_directory=starting_pubmed_dir)
    
     # connect to sqLite database
    create_database(db_path)
   
    # get pubmed first level folders
    first_level_folders = get_first_level_directory(ftp=ftp)
    ftp.quit()

    # Process a subset of first-level folders for testing
    first_twenty = first_level_folders[:10]

    # Use ThreadPoolExecutor to process folders concurrently
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_folder = {
            executor.submit(
                process_folder, ftp_host, starting_pubmed_dir, folder, output_folder, db_path
            ): folder for folder in first_twenty
        }
        for future in concurrent.futures.as_completed(future_to_folder):
            folder = future_to_folder[future]
            try:
                result = future.result()
                if result:
                    logging.info(f"Successfully processed folder {folder}")
                else:
                    logging.error(f"Failed to process folder {folder}")
            except Exception as exc:
                logging.error(f"Exception generated while processing folder {folder}: {exc}")

if __name__ == "__main__":
    main()
