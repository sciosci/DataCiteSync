from pathlib import Path
import argparse
import sqlite3
from datetime import datetime, timezone
import tarfile
           
def navigate_data_directories(data_dirs):
    for first_level_dir in data_dirs.iterdir():
        if first_level_dir.is_dir():
            # go into second level dir
            for second_level_dir in first_level_dir.iterdir():
                for zip_item in second_level_dir.iterdir():
                    #create manifest here
                    print(zip_item,
                          'first_level_directory: ', first_level_dir.name,
                          'second_level_directory: ', second_level_dir.name,
                          )
                    item_content = get_gzip_metadata(file_path=zip_item)
                    # if item_content['obj_id'] not in sqlLite table
                    # add item_content to list of values to add to sql table

def add_values_to_articles_table(articles_list:list[dict])-> None:
    ''' Adds multiple values to articles_metadata table and 
    Args:
        articles_list List[Dict]:  list of zip data that needs to be added to articles table  

    Returns:
        None
    '''
    pass


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
    thread_read_output_dir = Path('./output_dir/data')
    base_output_directory = Path('./output_dir/test_create')

    db_path = base_output_directory / 'manifest.db'  # SQLite database filce    

    # navigating base directory to ensure that the data directory exists
   # first_level_directories = navigate_base_level_directory(path=base_output_directory)

    # Now that we have directories in first level, get second level directories
    #  navigate_data_directories(data_dirs=first_level_directories)

    # create_manifest(db_path=db_path)

    navigate_data_directories(thread_read_output_dir)


    


if __name__ == '__main__':
    main()
