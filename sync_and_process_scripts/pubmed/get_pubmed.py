import ftplib
import tarfile
from pathlib import Path
import sqlite3
from datetime import datetime
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

    # Commit changes and close the connection
    conn.commit()
    return conn

def insert_article_information(conn, filename, modify, parent_dir, sub_dir, zip_data):
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

def process_second_level_directory(ftp:object, db_conn:object, parent_dir:list[str])->list[str]:
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
            # create child directorys
            output_path.mkdir(parents=True, exist_ok=True)
            write_files_to_directory_with_sqlLite_tracking(ftp, db_conn, output_path=output_path, sub_dir=folder, parent_dir=dir)
            return 
        # back out, to create sub directories for other folders
        ftp.cwd('..')
        # return after first folder filled to make sure function works correctly

def write_files_to_directory_with_sqlLite_tracking(ftp, db_conn, output_path, sub_dir, parent_dir):
    ftp.cwd(sub_dir)
    #getting a list of the files
    for entry in ftp.mlsd():
        file_name, file_info = entry
        
        # Check if the entry is a file and has a .tar.gz extension
        if file_info.get('type') == 'file' and file_name.endswith('.tar.gz'):
            # print(entry)
            local_file_path = output_path / file_name
            
            # Download the file in binary mode without extracting
            with open(local_file_path, 'wb') as f:
                ftp.retrbinary(f'RETR {file_name}', f.write)

            # Open the zip file in read mode and return count of items in gzip
            zip_data = get_gzip_metadata(file_path=local_file_path) 
            # return 
            # Get the modify timestamp and insert into database
            modify_date = file_info.get('modify')
            
            
            # Comparison if the article already exists, if we need to overwrite the information
            if article_needs_update(db_conn=db_conn, article_name=file_name, article_info=file_info):
                insert_article_information(db_conn, file_name, modify_date, parent_dir, sub_dir, zip_data)
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

if __name__ == "__main__":

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
    db_path = output_folder / 'manifest.db'  # SQLite database file
    output_folder.mkdir(parents=True, exist_ok=True)


    # Make sure FTP server is live and we can still connect to it    
    ftp = connect_to_pubmed(ftp_server=ftp_server, directory= starting_pubmed_dir)
    
    # Create or connect to the SQLite database
    db_conn = create_database(db_path)

    # create output top structure
    parent_folders = process_first_level_directory(ftp=ftp, base_output=base_output)

    #create second level directory
    second_level_dir = process_second_level_directory(ftp,  db_conn, parent_dir=parent_folders)

    # disconnect from FTP and sqlLite
    ftp.quit()
    #close db_conn, 
    db_conn.close()
