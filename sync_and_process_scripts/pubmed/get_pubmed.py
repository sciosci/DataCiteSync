
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
from datetime import datetime
import time
import argparse #
import concurrent.futures


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


def traverse_second_level_directory(ftp_path,output_path,first_level_folder, db_conn, ftp, second_level_dir_name ):
    # enter into the second level directory
    
    ftp.cwd(f"{ftp_path}/{first_level_folder}/{second_level_dir_name}")
    path_to_second_level_folder = Path(f"{output_path}/{first_level_folder}/{second_level_dir_name}") 
    path_to_second_level_folder.mkdir(parents=True, exist_ok=True)
    zip_files = []
    ftp.retrlines('NLST',zip_files.append)
    # get the zip files in here 
    print(zip_files)
    # Here I do the metadata check against the sql data
    # zip_files = active_ftp.nlst()


def process_folder(ftp_host, pubmed_dir, first_level_folder, output_dir_path, db_conn):
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
        # Perform the sql query here
        tbl = get_manifest_data_from_first_level_dir(db_conn, first_level_folder=first_level_folder)
        if tbl:
            print('we got one', tbl)
        # Get the list of items in the folder
        items = ftp.nlst()
        # go now into the second level folder
        for item in items:
            # print(item)
            traverse_second_level_directory(pubmed_dir, output_dir_path, first_level_folder, db_conn, ftp=ftp, second_level_dir_name=item,  )
        
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


def create_database(db_path):
    # Connect to the SQLite database (or create it if it doesn’t exist)
    conn = sqlite3.connect(db_path, check_same_thread=False)
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
    return conn

def get_gzip_metadata(file_path: str) -> dict:
    contents = {}
    
    with tarfile.open(file_path, 'r:gz') as tar:
        # Iterate through each file in the tar archive
        for member in tar.getmembers():
            file_name = member.name
            # print('filename: ', file_name)
            file_extension = file_name.split('.')[-1] if '.' in file_name else 'obj_id'
            key =  f'{file_extension}_count' if '.' in file_name else 'obj_id'
            # Update the count in contents dictionary for each file type
            if key in contents:
                contents[key] += 1
            elif key == 'obj_id':
                contents[key] = file_name
            else:
                contents[key] = 1
    # print(contents)
    return contents


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
    db_conn = create_database(db_path)
   
    # get pubmed first level folders
    first_level_folders = get_first_level_directory(ftp=ftp)
    
    # quit the single thread of FTP server to allow for 
    ftp.quit() 
    # begin multithreading with the response object for every one
    first_twenty = first_level_folders[:21] 
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_zip = {executor.submit(process_folder, ftp_host, starting_pubmed_dir, folder, output_folder, db_conn): folder for folder in first_twenty}
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