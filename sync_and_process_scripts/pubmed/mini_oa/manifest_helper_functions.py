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


def create_database(db_path)-> None:
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
        second_level_dir TEXT,
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS pubmed_runtime_data (
        date_execution TEXT NOT NULL,
        run_duratio TEXT,
        downloaded_at TEXT,
        first_level_dir TEXT,
        second_level_dir TEXT,
        
    )
    ''')

    # Commit changes
    conn.commit()
    conn.close()




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
                first_level_dir, second_level_dir,
            ) VALUES ( ?, ?, ?, ?)
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

