import ftplib
import tarfile
from pathlib import Path
import sqlite3
from datetime import datetime
import time
import argparse #

def create_database(db_path):
    # Connect to the SQLite database (or create it if it doesn’t exist)
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

    # Commit changes and close the connection
    conn.commit()
    return conn

def insert_article_information(conn: sqlite3, filename:str, modify:datetime, parent_dir:str, sub_dir:str, zip_data:object)->None:
    cursor = conn.cursor()
    
    # Get the zip information if present, else set to none
    pdf_count = zip_data.get("pdf_count", 0)
    xml_count = zip_data.get("xml_count", 0)
    image_count = zip_data.get("jpg_count", 0)
    
    # Convert modify timestamp to a datetime object
    modify_datetime = datetime.strptime(modify, "%Y%m%d%H%M%S")
    
    # Store as ISO 8601 string format
    modify_str = modify_datetime.isoformat()
    
    # Current timestamp for when the file was downloaded
    downloaded_at = datetime.now().isoformat()

    cursor.execute('''
    INSERT OR REPLACE INTO articles_metadata (article_id, article_last_update, downloaded_at, first_level_dir, second_level_dir, image_count, xml_count, pdf_count)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', (filename, modify_str, downloaded_at, parent_dir, sub_dir, image_count, xml_count, pdf_count))
    
    conn.commit()


def connect_to_pubmed(ftp_server:object, directory:str) -> object:
   
    ftp = ftplib.FTP(ftp_server)
    # Login anonymous user and password
    ftp.login()
    # cwd is change into /dir
    ftp.cwd(directory)
    return ftp

    

def process_first_level_directory(ftp, base_output:str)->list[str]:
    # filenames = ftp.nlst()
    files = []
    # get filenames
    ftp.retrlines('NLST',files.append)
    for file in files:
        output_path = Path(f'{base_output}/{file}')
        output_path.mkdir(parents=True, exist_ok=True)
    return files

def process_second_level_directory(ftp: object,ftp_server:str,starting_pubmed_dir, db_conn:object, parent_dir:list[str], base_output:str):
    """
    for every parent folder, 
    create the sub folder directory
    """
    for dir in parent_dir:
        # enter into that dir in the ftp
        ftp.cwd(dir)
        folders = []
        # get all folders that belong to the parent directory
        ftp.retrlines('NLST',folders.append)
        for folder in folders:
            output_path = Path(f'{base_output}/{dir}/{folder}')
            # output_path = Path(f'{dir}/{folder}')
            # create child directorys
            output_path.mkdir(parents=True, exist_ok=True)
            write_files_to_directory_with_sqlLite_tracking(ftp, ftp_server, starting_pubmed_dir, db_conn, output_path=output_path, sub_dir=folder, parent_dir=dir)
        # back out, to create sub directories for other folders
        print('finished a second level directory')
        ftp.cwd('..')
        # return after first folder filled to make sure function works correctly


def write_files_to_directory_with_sqlLite_tracking(ftp, ftp_server, starting_pubmed_dir, db_conn, output_path, sub_dir, parent_dir):
    cursor = db_conn.cursor()
    ftp.sendcmd("NOOP")
    try:
        ftp.cwd(sub_dir)
    except EOFError as e:
        # Reconnect to FTP server
        print(f"Connection lost. Reconnecting to FTP server...")
        ftp = connect_to_pubmed(ftp_server=ftp_server, directory=starting_pubmed_dir)
        ftp.cwd(parent_dir)
        ftp.cwd(sub_dir)
    except Exception as e:
        # Handle other exceptions
        downloaded_at = datetime.now().isoformat()
        cursor.execute('''
        INSERT INTO pubmed_runtime_data (date_execution, downloaded_at, first_level_dir, second_level_dir)
        VALUES (?, ?, ?, ?)
        ''', (downloaded_at, downloaded_at, parent_dir, sub_dir))
        db_conn.commit()
        print(f"Error {e} encountered in subdirectory {sub_dir} of {parent_dir}. Logged in pubmed_runtime_data.")
        return

    # Proceed with the rest of the function
    for entry in ftp.mlsd():
        file_name, file_info = entry
        
        # Check if the entry is a file and has a .tar.gz extension
        if file_info.get('type') == 'file' and file_name.endswith('.tar.gz'):
            local_file_path = output_path / file_name
            try:
                # Download the file in binary mode without extracting
                with open(local_file_path, 'wb') as f:
                    ftp.retrbinary(f'RETR {file_name}', f.write)
                    
                # Open the zip file in read mode and return count of items in gzip
                zip_data = get_gzip_metadata(file_path=local_file_path) 
                
                # Get the modify timestamp and insert into database
                modify_date = file_info.get('modify')
                
                # Check if the article already exists and if it needs updating
                if article_needs_update(db_conn=db_conn, article_name=file_name, article_info=file_info):
                    insert_article_information(db_conn, file_name, modify_date, parent_dir, sub_dir, zip_data)
            
            except EOFError as e:
                # Handle EOFError during file processing
                downloaded_at = datetime.now().isoformat()
                cursor.execute('''
                INSERT INTO pubmed_runtime_data (date_execution, downloaded_at, first_level_dir, second_level_dir, file_name)
                VALUES (?, ?, ?, ?, ?)
                ''', (downloaded_at, downloaded_at, parent_dir, sub_dir, file_name))
                db_conn.commit()
                print(f"Connection lost during processing of file {file_name}. Reconnecting...")
                time.sleep(1)
                # Reconnect to the FTP server
                ftp = connect_to_pubmed(ftp_server=ftp_server, directory=starting_pubmed_dir)
                ftp.cwd(parent_dir)
                ftp.cwd(sub_dir)
                # Retry downloading the file
                with open(local_file_path, 'wb') as f:
                    ftp.retrbinary(f'RETR {file_name}', f.write)
                # Proceed with the rest of the processing as before
                zip_data = get_gzip_metadata(file_path=local_file_path)
                modify_date = file_info.get('modify')
                if article_needs_update(db_conn=db_conn, article_name=file_name, article_info=file_info):
                    insert_article_information(db_conn, file_name, modify_date, parent_dir, sub_dir, zip_data)
            except Exception as e:
                # Handle other exceptions during file processing
                downloaded_at = datetime.now().isoformat()
                cursor.execute('''
                INSERT INTO pubmed_runtime_data (date_execution, downloaded_at, first_level_dir, second_level_dir, file_name)
                VALUES (?, ?, ?, ?, ?)
                ''', (downloaded_at, downloaded_at, parent_dir, sub_dir, file_name))
                db_conn.commit()
                print(f"Error {e} encountered with file {file_name} in {sub_dir} of {parent_dir}. Logged in pubmed_runtime_data.")
                # Optionally, continue to the next file or re-raise the exception
                continue

    ftp.cwd('..')


def article_needs_update(db_conn: sqlite3.Connection, article_name: str, article_info: dict) -> bool:
    """
    Checks if an article needs an update by verifying the last update date.
    
    Args:
        db_conn (sqlite3.Connection): The database connection object.
        article_name (str): The name or ID of the article to check.

    Returns:
        bool: True if the article needs an update, False otherwise.
    """

    try:
        # Enable dictionary-like access for rows
        db_conn.row_factory = sqlite3.Row
        cursor = db_conn.cursor()
        
        # Parameterized query to prevent SQL injection
        query = "SELECT article_last_update FROM articles_metadata WHERE article_id = ?"
        cursor.execute(query, (article_name,))
        
        # Fetch the result
        row = cursor.fetchone()
        if row is None:
            # return true if the article does not exist to maintain  
            return True  
        
        last_update = row["article_last_update"]  # Access the value by column name
        curr_update = datetime.strptime(article_info.get('modify'), "%Y%m%d%H%M%S").isoformat()
        # print(last_update, 'current Update', curr_update)
        # Access column by name
        if curr_update > last_update:
            return True
        else:
            return False

    except sqlite3.Error as e:
        print("Database error:", e)
        return False  # Or handle as appropriate for your use case


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

def main()->None:

    parser = argparse.ArgumentParser(description="""
    Full download of latest sematic scholar dataset release
""")
    
    parser.add_argument(
        "-o", "--output_dir", help="Output base directory of downloaded files"
    )
    # Getting arguments passed from
    arguments = parser.parse_args()
    base_output = arguments.output_dir

    # FTP Server information 
    ftp_server = 'ftp.ncbi.nlm.nih.gov'
    starting_pubmed_dir = r'/pub/pmc/oa_package' 
    
    #Creating output directory
    output_folder = Path('./output_dir')
    db_path = output_folder / 'manifest.db'  # SQLite database filce
    output_folder.mkdir(parents=True, exist_ok=True)

    # Make sure FTP server is live and we can still connect to it    
    ftp = connect_to_pubmed(ftp_server=ftp_server, directory= starting_pubmed_dir)
    
    # Create or connect to the SQLite database
    db_conn = create_database(db_path)

    # create output top structure
    parent_folders = process_first_level_directory(ftp=ftp, base_output=base_output)

    # print('Parent Folders: ', parent_folders)
    #create second level directory
    process_second_level_directory(ftp, ftp_server,starting_pubmed_dir, db_conn, parent_dir=parent_folders, base_output=base_output)

    # disconnect from FTP and sqlLite
    ftp.quit()
    #close db_conn, 
    db_conn.close()


if __name__ == "__main__":
    main()
