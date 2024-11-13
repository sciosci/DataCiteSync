# To-Do:
#   After getting feedback from Pawin, 
#   there isnt a need to pass the sql_object to all the threads. 
#   In the main thread, we can have the return value from process folder,
#   such as an object or df, which can make it easier to batch write to sqlLite
# EXAMPLE: https://docs.python.org/3.12/library/concurrent.futures.html#concurrent.futures.ThreadPoolExecutor

from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import threading
import logging
import sqlite3
import tarfile
import datetime
from queue import Queue
import concurrent.futures

def process_folder(folder_path):
    folder_first_level = Path(folder_path)
    thread_name = threading.current_thread().name
    logging.info(f'Thread {thread_name} processing folder: {folder_first_level}')
    results = []
    try:
        for folder_second_level in folder_first_level.iterdir():
            if folder_second_level.is_dir():
                for file in folder_second_level.iterdir():
                    logging.info(f'Thread {thread_name} found file: {file}')
                    file_data = get_gzip_metadata(file_path=file)
                    # Add the metadata to the queue
                    # data_queue.put(file_data)
                    # Here instead, lets pass the data to a dictionary
                    results.append(file_data)
        return results
    except Exception as e:
        logging.exception(f'Error in thread {thread_name}: {e}')
        return []


def insert_batch(cursor, batch):
    insert_data = []
    print('Batch: ',batch)
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
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', insert_data)
    return len(insert_data)  # Return the number of records inserted

def get_gzip_metadata(file_path: str) -> dict:
    contents = {
        'article_id': file_path.name,
        'article_last_update': None,
        'downloaded_at': get_current_time(),
        'first_level_dir': str(file_path.parent.parent.name),
        'second_level_dir': str(file_path.parent.name),
        'image_count': 0,
        'xml_count': 0,
        'pdf_count': 0,
        'other_files': 0
    }
    
    with tarfile.open(file_path, 'r:gz') as tar:
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
    return contents

def get_current_time():
    """Returns the current time."""
    return datetime.datetime.now().isoformat()

def create_database(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create articles_metadata table
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

    # Create pubmed_runtime_data table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS pubmed_runtime_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date_execution TEXT NOT NULL,
        run_duration TEXT,
        downloaded_at TEXT,
        new_zip_files_added INTEGER
    )
    ''')

    # Commit changes and close the connection
    conn.commit()
    conn.close()

def main():
    base_directory = Path('./output_dir/data')
    output_folder = Path('./output_dir')
    db_path = output_folder / 'threading_queue.db'
    data_queue = Queue()
    batch_size = 1000  # Adjust as needed

    # Create or connect to the SQLite database
    create_database(db_path)

    logging.basicConfig(
        filename='output.log',
        filemode='a',
        format='%(asctime)s [%(levelname)s] %(threadName)s: %(message)s',
        level=logging.INFO
    )

    # Record the start time
    start_time = datetime.datetime.now()
    total_records_inserted = 0 


    folders = [item for item in base_directory.iterdir() if item.is_dir()]
    data_list_to_be_written = []
    # we want to change to use concurrent futures threadpool since that will let us 
    # return the object, and we can just append for now, 
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        # start the load operations and mark each future 
        future_to_zip = {executor.submit(process_folder, folder): folder for folder in folders}
        for future in concurrent.futures.as_completed(future_to_zip):
            zip_file = future_to_zip[future]
            try:
                data = future.result()
                if data:
                    data_list_to_be_written.extend(data) 
            except Exception as exc:
                print('%r generated an exception: %s' % (zip_file, exc))
        # Wait until all data has been processed


    # Now, insert data into the database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    for i in range(0, len(data_list_to_be_written), batch_size):
        batch = data_list_to_be_written[i:i+batch_size]
        records_inserted = insert_batch(cursor, batch)
        print('records inserted: ',records_inserted, 
              'total records inserted: ', total_records_inserted)
        total_records_inserted += records_inserted
    conn.commit()

    # Calculate run duration
    end_time = datetime.datetime.now()
    run_duration = str(end_time - start_time)

    # Update pubmed_runtime_data table
    cursor.execute('''
        INSERT INTO pubmed_runtime_data (
            date_execution,
            run_duration,
            new_zip_files_added
        ) VALUES (?, ?, ?)
    ''', (
        start_time.isoformat(),
        run_duration,
        total_records_inserted
    ))
    conn.commit()
    conn.close()


if __name__ == '__main__':
    main()
