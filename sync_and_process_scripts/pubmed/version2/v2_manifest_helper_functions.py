"""
NOTES: 

 - Reduced the properties of every file being tracked by manifest, I did this to improve 
    .tar.gz writing to storage speed. We are only tracking necessary values for 
    keeping the oa_package up to date with current state of FTP. 

 Properties being tracked:
    article_id
    article_last_update
    first_level_dir
    second_level_dir
"""

import sqlite3, logging
from datetime import datetime


def create_or_connect_to_database(db_path)-> None:
    '''
    Connect to the sqLite database (or create it if it doesn’t exist)
        Tables being created:
            1. articles_metadata
            2. pubmed_runtime_data
    '''
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create the table if non-existent
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS articles_metadata (
        article_id TEXT NOT NULL,
        article_last_update TEXT,
        first_level_dir TEXT,
        second_level_dir TEXT)
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS pubmed_runtime_data (
        date_execution TEXT NOT NULL,
        run_duration TEXT,
        uploaded TEXT,
        rows_added TEXT,
        is_first_or_last BOOLEAN)
    ''')

    # Commit changes
    conn.commit()
    conn.close()

def get_manifest_data_from_first_level_dir(db_conn, first_level_folder):
    try:
        # Enable dictionary-like access for rows
        db_conn.row_factory = sqlite3.Row
        cursor = db_conn.cursor()

        # Parameterized query to prevent SQL injection
        query = "SELECT * FROM articles_metadata WHERE first_level_dir = ?"
        cursor.execute(query, (first_level_folder,))
        rows = cursor.fetchall()

        # Return the results as a dictionary so the lookup is O(1)
        result_dicts = {}
        for row in rows:
            article_id = row["article_id"]
            # Convert article_last_update to a datetime object if you need to compare dates
            try:
                article_last_update = datetime.strptime(row["article_last_update"], "%Y-%m-%d %H:%M:%S")
            except ValueError:
                # Handle unexpected format or set a fallback
                article_last_update = None

            result_dicts[article_id] = {
                "article_last_update": article_last_update
            }

        return result_dicts

    except Exception as e:
        logging.info(f'Error in get_manifest_data_from_first_level_dir: {e}')
        return {}



def update_runtime_table(pubmed_result_data, db_path):
    """
    Table to let us keep track of the start, end , upload interbal and amount of files uploaded every interval to manifest 
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

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
            first_level_dir = obj['first_level_folder']
            second_level_dir = obj['second_level_folder']

            # Ensure article_id is not None
            if not article_id:
                logging.warning("Skipping entry with missing article_id.")
                continue

            # Use INSERT with ON CONFLICT clause
            cursor.execute('''
            INSERT INTO articles_metadata (
                article_id, article_last_update, 
                first_level_dir, second_level_dir)
                VALUES ( ?, ?, ?, ?)
            ''', (
                article_id, article_last_update, 
                first_level_dir, second_level_dir
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

