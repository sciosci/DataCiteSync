from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import threading
import logging
import sqlite3
import tarfile
import datetime
from queue import Queue


def process_folder(folder_path, data_queue):
    folder_first_level = Path(folder_path)
    thread_name = threading.current_thread().name
    # print(f'Thread {thread_name} processing folder: {folder_first_level}')
    try:
        for folder_second_level in folder_first_level.iterdir():
            if folder_second_level.is_dir():
                for file in folder_second_level.iterdir():
                    logging.info(f'Thread {thread_name} found file: {file}')
                    file_data = get_gzip_metadata(file_path=file)
                    # Add the metadata to the queue
                    data_queue.put(file_data)
    except Exception as e:
        print(f'Error in thread {thread_name}:', e)

def db_worker(db_path, data_queue, BATCH_SIZE):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    batch = []
    
    while True:
        item = data_queue.get()
        if item == "DONE":
            # Insert any remaining items in the batch
            if batch:
                insert_batch(cursor, batch)
                conn.commit()
            data_queue.task_done()
            break
        batch.append(item)
        if len(batch) >= BATCH_SIZE:
            insert_batch(cursor, batch)
            conn.commit()
            batch = []
        data_queue.task_done()
    conn.close()

def insert_batch(cursor, batch):
    insert_data = []
    for item in batch:
        insert_data.append((
            item.get('article_id'),
            item.get('article_last_update'),
            item.get('downloaded_at'),
            item.get('first_level_dir'),
            item.get('second_level_dir'),
            item.get('image_count'),
            item.get('xml_count'),
            item.get('pdf_count'),
            item.get('other_files')
        ))
    cursor.executemany('''
        INSERT INTO articles_metadata (
            article_id, article_last_update, downloaded_at,
            first_level_dir, second_level_dir,
            image_count, xml_count, pdf_count, other_files
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?,?)
    ''', insert_data)


def get_gzip_metadata(file_path: str) -> dict:
    contents = {
        'article_id': None,
        'article_last_update': None,
        'downloaded_at': None,
        'first_level_dir': str(file_path.parent.parent.name),
        'second_level_dir': str(file_path.parent.name),
        'image_count': 0,
        'xml_count': 0,
        'pdf_count': 0,
        'other_files':0
    }
    
    with tarfile.open(file_path, 'r:gz') as tar:
        for member in tar.getmembers():
            file_name = member.name
            file_extension = file_name.split('.')[-1].lower() if '.' in file_name else ''
            # Begin Setting values to contents
            #Fle name is unique so set to article_id
            contents['article_id'] = file_path.name 
            if file_extension == 'pdf':
                contents['pdf_count'] += 1
            elif file_extension == 'xml':
                contents['xml_count'] += 1
            elif file_extension in ['jpg', 'png', 'gif']:
                contents['image_count'] += 1
            else:
                contents['other_files'] += 1  
    # Set 'article_id', 'article_last_update', 'downloaded_at' as needed
    # contents['article_last_update'] = extract_last_update(file_path)

    # Current Time 
    current_time = get_current_time()
    contents['downloaded_at'] = current_time 
    contents['article_last_update'] = current_time 
    
    return contents


def get_current_time():
    """
    Returns current time of article download
    """
    return datetime.datetime.now().isoformat()


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
        pdf_count INTEGER,
        other_files INTEGER
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS pubmed_runtime_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date_execution TEXT NOT NULL,
        run_duration TEXT,
        downloaded_at TEXT,
        new_zip_files_added INTEGER,
        zip_files_updated INTEGER
    )
    ''')

    # Commit changes and close the connection
    conn.commit()
    return conn



def main():
    base_directory = Path('./output_dir/data')
    output_folder = Path('./output_dir')
    db_path = output_folder / 'threading_queue.db'
    data_queue = Queue()
    BATCH_SIZE = 100  # Adjust as needed
    # Create or connect to the SQLite database
    create_database(db_path)
    logging.basicConfig(
        filename='output.log',
        filemode='a',
        format='%(asctime)s [%(levelname)s] %(threadName)s: %(message)s',
        level=logging.INFO
    )
    # Start the database worker thread
    db_thread = threading.Thread(target=db_worker, args=(db_path,data_queue, BATCH_SIZE))
    db_thread.start()

    folders = [item for item in base_directory.iterdir() if item.is_dir()]

    with ThreadPoolExecutor() as executor:
        executor.map(lambda folder: process_folder(folder, data_queue), folders)

    # Wait until all data has been processed
    data_queue.join()
    # Send the sentinel value to the db_worker to signal completion
    data_queue.put("DONE")
    db_thread.join()


if __name__ =='__main__':

    main()