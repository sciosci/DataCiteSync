from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import threading
import logging
import sqlite3
import tarfile
from queue import Queue



def process_folder(folder_path):
    folder_first_level = Path(folder_path)
    thread_name = threading.current_thread().name
    print(f'Thread {thread_name} processing folder: {folder_first_level}')
    try:
        for folder_second_level in folder_first_level.iterdir():
            if folder_second_level.is_dir():
                for file in folder_second_level.iterdir():
                    # print(f'Thread {thread_name} found file: {file}')\

                    logging.info(f'Thread {thread_name} found file: {file}')
                    file_data = get_gzip_metadata(file_path=file)
                    print(file_data)
                    #here is where the metadata is added to the queue to add the rows? 
                    
    except Exception as e:
        print(f'Error in thread {thread_name}:', e)


def get_gzip_metadata(file_path: str) -> dict:
    contents = {
        'pdf_count': 0,
        'other_docs': 0,
        'obj_id': None
    }
    
    with tarfile.open(file_path, 'r:gz') as tar:
        # Iterate through each file in the tar archive
        for member in tar.getmembers():
            file_name = member.name
            file_extension = file_name.split('.')[-1] if '.' in file_name else 'obj_id'
            key = 'pdf_count' if file_extension == 'pdf' else ('obj_id' if file_extension == 'obj_id' else 'other_docs')
            
            # Update the count in contents dictionary for each file type
            if key == 'obj_id':
                contents[key] = file_name 
            elif key == 'pdf_count':
                contents[key] += 1
            else:
                contents['other_docs'] += 1
                
    return contents


def create_database(db_path)->object:
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



def main():
    base_directory = Path('./output_dir/data')
    output_folder = Path('./output_dir')
    db_path = output_folder / 'threading_queue.db'  # SQLite database filce

    # Create or connect to the SQLite database
    db_conn = create_database(db_path)
    logging.basicConfig(
    filename='output.log',
    filemode='a',
    format='%(asctime)s [%(levelname)s] %(threadName)s: %(message)s',
    level=logging.INFO
)

    folders = [item for item in base_directory.iterdir() if item.is_dir()]

    with ThreadPoolExecutor() as executor:
        executor.map(process_folder, folders)



if __name__ =='__main__':

    main()