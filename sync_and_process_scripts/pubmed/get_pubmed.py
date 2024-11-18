
'''
Notes: on 11/16/2024
- Am currently dealing with a multithreading issue with SQL queries. 
    Error exaple: Error in get Manifest data thread:SQLite objects created in a thread can only be used in that same thread.
     The object was created in thread id 140533752776192 and this is thread id 140533711976128.

Why am I multithreading with a sqlConnection, 
    - To get the first level folder data for each individual thread, before going into second level folders.
    Improving the speed of checks between FTP data and pl data. 
Multithreading works, but I am implementing it incorrectly right now.  

'''

# imports here
import ftplib
import tarfile
from pathlib import Path
import sqlite3
import time
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


def traverse_second_level_directory(ftp_path, output_path, first_level_folder, sql_filtered_tbl, ftp, second_level_dir_name )->list[str]:
    '''
    This function we want check multiple things: 
       1. Which files are in the database -> Pass over them
       2. Which files are NOT in the database -> Add themn to files_to_be_written, and write to output dir
       3. Which files are in database but have old data -> Add themn to files_to_be_written, and write to output dir

    Files will be written to database AND outputdirectory after individual threads complete in batch, no writing will occur during this function.
    '''
    # enter into the second level directory
    files_to_be_written: List[str] = [] 
    zip_files_in_ftp_directory = []
    ftp.cwd(f"{ftp_path}/{first_level_folder}/{second_level_dir_name}")
    path_to_second_level_folder = Path(f"{output_path}/{first_level_folder}/{second_level_dir_name}") 
    path_to_second_level_folder.mkdir(parents=True, exist_ok=True)
    # list of zip files in the current directory here 
    ftp.retrlines('NLST',zip_files_in_ftp_directory.append)
    try:
        # iterate through list 
        for gzip_file in ftp.mlsd():
            
            file_name, file_info = gzip_file

            # perform an if check here on file information modify
             
            # if the table is completely empty (first time running) 
            if not sql_filtered_tbl:
               metadata = get_gzip_metadata(gzip_file, path_to_second_level_folder)
            #    print(f'metadata: {metadata}, \n, file_info: {file_info}, \n file_name: {file_name}') 
                


    except Exception as exc:
        print(f'Error in traverse second level directory: {exc}') 

    
    # print('Zip files here')
    # Here I do the metadata check against the sql data
    # zip_files = active_ftp.nlst()


def process_folder(ftp_host, pubmed_dir, first_level_folder, output_dir_path, db_path):
    """
    Worker function to connect to the FTP server, list items in a folder,
    and return the results.
    """
    try:
        # Each thread creates its own FTP connection
        ftp = ftplib.FTP(ftp_host)
        ftp.login()  # Add credentials if needed
        ftp.cwd(f"{pubmed_dir}/{first_level_folder}")
        
        # Create the output folder if it does not already exist
        ouput_path_first_level_folder = Path(f'{output_dir_path}/{first_level_folder}')
        ouput_path_first_level_folder.mkdir(parents=True, exist_ok=True)

        db_conn = sqlite3.connect(db_path)

        # Perform the sql query here
        sql_filtered_tbl = get_manifest_data_from_first_level_dir(db_conn, first_level_folder=first_level_folder)
        # print('table: ', sql_filtered_tbl)
        
        # Get the list of items in the folder
        items = ftp.nlst()
        # go now into the second level folder
        for item in items:
            # print(item)
            traverse_second_level_directory(pubmed_dir, output_dir_path, first_level_folder, sql_filtered_tbl, ftp=ftp, second_level_dir_name=item,  )
        
        ftp.quit()  # Close the connection

        # Return folder name and its contents
        return (first_level_folder, items)
    except Exception as e:
        return (first_level_folder, f"Error: {e}")


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
        print(f'Error in get Manifest data thread:{e}' )    


def create_database(db_path)-> None:
    '''
    Connect to the sqLite database (or create it if it doesn’t exist)
    '''
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create the table if non-existent
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS articles_metadata (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
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
        id INTEGER PRIMARY KEY AUTOINCREMENT,
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
    

def get_gzip_metadata(gzip_file: tuple[str, object], path_to_folder) -> dict:
    contents = {
        'binary_content': None,
        'path_to_folder':path_to_folder,
        'article_id': gzip_file[0],
        'article_last_update': None,
        'downloaded_at': get_current_time(),
        'first_level_dir': None,
        'second_level_dir': None,
        'image_count': 0,
        'xml_count': 0,
        'pdf_count': 0,
        'other_files': 0
    }
    print(f'gzip_file: {gzip_file}, \n \n path to folder:  {path_to_folder}')
    # return
    with tarfile.open(f'{path_to_folder}/{gzip_file[0]}', 'r:gz') as tar:
        for member in tar.getmembers():
            file_extension = member.name.split('.')[-1].lower() if '.' in member.name else ''
            if file_extension == 'pdf':
                contents['pdf_count'] += 1
            elif file_extension == 'xml':
                contents['xml_count'] += 1
            elif file_extension in ['jpg', 'png', 'gif']:
                contents['image_count'] += 1
            else:
                contents['other_files'] += 1
    print(contents)
    return contents

def get_current_time():
    """Returns the current time."""
    return datetime.datetime.now().isoformat()


def main():
    # connect to pubmed
    ftp_host = 'ftp.ncbi.nlm.nih.gov'
    starting_pubmed_dir = r'/pub/pmc/oa_package' 
    
    #Creating output directory
    output_folder = Path('./output_dir')
    db_path = output_folder / 'manifest.db'  # SQLite database filce
    output_folder.mkdir(parents=True, exist_ok=True)

    # Make sure FTP server is live and we can still connect to it    
    ftp = connect_to_pubmed(ftp_server=ftp_host, starting_ftp_directory= starting_pubmed_dir)
    
    # connect to sqLite database
    create_database(db_path)
   
    # get pubmed first level folders
    first_level_folders = get_first_level_directory(ftp=ftp)
    
    # quit the single thread of FTP server to allow for 
    ftp.quit() 
    # begin multithreading with the response object for every one
    first_twenty = first_level_folders[:21] 
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_zip = {executor.submit(process_folder, ftp_host, starting_pubmed_dir, folder, output_folder, db_path): folder for folder in first_twenty}
        for future in concurrent.futures.as_completed(future_to_zip):
            zip_file = future_to_zip[future]
            try:
                print(future.result())
            except Exception as exc:
                print('%r generated as exception: %s' % (zip_file, exc))
                 
        # process folder files
    
    # batch upload to sqLite 

    #close ftp
    # close sqLite

if __name__ == "__main__":
    main()