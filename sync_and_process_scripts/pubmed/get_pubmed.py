import ftplib
import tarfile
from pathlib import Path
import sqlite3
from datetime import datetime

def create_database(db_path):
    # Connect to the SQLite database (or create it if it doesn’t exist)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create the files table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS files (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        article_id TEXT NOT NULL,
        last_update TEXT,
        downloaded_at TEXT,
        first_level_dir TEXT,
        second_level_dir TEXT,
        image_count TEXT,
        has_xml BOOL,
        has_pdf BOOL
    )
    ''')

    # Commit changes and close the connection
    conn.commit()
    return conn

def insert_file_info(conn, filename, modify, parent_dir, sub_dir):
    cursor = conn.cursor()
    
    # Convert modify timestamp to a datetime object
    modify_datetime = datetime.strptime(modify, "%Y%m%d%H%M%S")
    
    # Store as ISO 8601 string format
    modify_str = modify_datetime.isoformat()
    
    # Current timestamp for when the file was downloaded
    downloaded_at = datetime.now().isoformat()
    image_count, has_xml, has_pdf = 0, False, False
    cursor.execute('''
    INSERT INTO files (article_id, last_update, downloaded_at, first_level_dir, second_level_dir, image_count, has_xml, has_pdf)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', (filename, modify_str, downloaded_at, parent_dir, sub_dir, image_count, has_xml, has_pdf))
    
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
    for entry in ftp.mlsd():
        file_name, file_info = entry
        
        # Check if the entry is a file and has a .tar.gz extension
        if file_info.get('type') == 'file' and file_name.endswith('.tar.gz'):
            print(entry)
            local_file_path = output_path / file_name
            
            # Download the file in binary mode without extracting
            with open(local_file_path, 'wb') as f:
                ftp.retrbinary(f'RETR {file_name}', f.write)

            # Open the zip file in read mode and return count of items in gzip
            get_gzip_metadata(file_path=local_file_path) 
            return 
            # Get the modify timestamp and insert into database
            modify_date = file_info.get('modify')
            # Add some comparison if clause here to avoid duplication
            if modify_date:
                insert_file_info(db_conn, file_name, modify_date, parent_dir, sub_dir)
    ftp.cwd('..')

def get_gzip_metadata(file_path: str) -> dict:
    contents = {
        "pdf": 0,
        "xml": 0,
        "jpg": 0
    }
    
    with tarfile.open(file_path, 'r:gz') as tar:
        # Iterate through each file in the tar archive
        for member in tar.getmembers():
            file_name = member.name
            
            # Check the file extension and update the count in contents dictionary
            if file_name.endswith(".pdf"):
                contents["pdf"] += 1
            elif file_name.endswith(".xml"):
                contents["xml"] += 1
            elif file_name.endswith(".jpg"):
                contents["jpg"] += 1
            # Additional file types can be added here if needed
    print(contents)
    return

if __name__ == "__main__":
    ftp_server = 'ftp.ncbi.nlm.nih.gov'
    main_dir = r'/pub/pmc/oa_package' 
    db_path = 'file_tracking.db'  # SQLite database file

    base_output = r'C:\Users\diego\OneDrive\Desktop\Desktop\Software\Research\DataCiteSync\sync_and_process_scripts\pubmed\output_dir'
    # Make sure FTP server is live and we can still connect to it    
    ftp = connect_to_pubmed(ftp_server=ftp_server, directory= main_dir)
    # Create or connect to the SQLite database
    db_conn = create_database(db_path)

    # create output top structure
    parent_folders = process_first_level_directory(ftp=ftp, base_output=base_output)

    #create second level directory
    second_level_dir = process_second_level_directory(ftp,  db_conn, parent_dir=parent_folders)

    # disconnect from FTP and sqlLite
    ftp.quit()
    db_conn.close()
