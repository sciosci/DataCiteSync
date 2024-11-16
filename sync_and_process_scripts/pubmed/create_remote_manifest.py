from pathlib import Path
import argparse
import sqlite3
from datetime import datetime, timezone

def navigate_base_level_directory(path:str)->list[str]:
    for item in path.iterdir():
        print(item)
        #now that we are inside the 
        if item.is_dir() and item.name == 'data':
            return [name for name in item.iterdir() if name.is_dir()]
            
def navigate_data_directories(data_dirs):
    for first_level_dir in data_dirs:
        if first_level_dir.is_dir():
            #go into second level dir
            for second_level_dir in first_level_dir.iterdir():
                for zip_items in second_level_dir.iterdir():
                    #create manifest here
                    print(zip_items.name,
                          'first_level_directory: ', first_level_dir.name,
                          'second_level_directory: ', second_level_dir.name,
                          )


def create_manifest(db_path):
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



def main()->None:

    """
    Ideal outputs:
        - A manifest of the information already stored in oa_package folder in PL
        - We are going to run some tests with the current local repo's
    """
    #Getting parent directory from shell file
    parser = argparse.ArgumentParser(description="""
    Full download of latest pubmed dataset release
    """)
    
    parser.add_argument(
         "--input_dir", help="Output base directory of downloaded files"
    )

    # Getting arguments passed from
    arguments = parser.parse_args()
    input_dir = arguments.input_dir

    start_script = datetime.now(timezone.utc) 
    # dir = Path(input_dir)
    thread_read_output_dir = Path('./output_dir')
    base_output_directory = Path('./output_dir/test_create')

    db_path = base_output_directory / 'manifest.db'  # SQLite database filce    

    # navigating base directory to ensure that the data directory exists
   # first_level_directories = navigate_base_level_directory(path=base_output_directory)

    # Now that we have directories in first level, get second level directories
    #  navigate_data_directories(data_dirs=first_level_directories)

    create_manifest(db_path=db_path)

    navigate_data_directories()


    


if __name__ == '__main__':
    main()
