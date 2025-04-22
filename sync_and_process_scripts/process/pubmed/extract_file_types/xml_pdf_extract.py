'''
What is this file for?
    If there is ever an error with the db writes for pdf_extraction of xml_extraction updates, then
    you can run this file to make sure that 
'''

import concurrent.futures
from pathlib import Path
import sys
import tarfile
import argparse
import sqlite3
import logging
from datetime import datetime
from typing import List, Dict

def configure_logging(log_path: Path):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s: %(message)s",
        handlers=[
            logging.FileHandler(log_path),    # writes to file
            logging.StreamHandler()           # prints to console
        ]
    )

class PubMedExtractor:
    """
    Command

    I did not like the prop drilling that was beginning to happen,
    so I changed this to OOP, using command structure where

    Passing logger to PubMedDB, since it is defined outside of the classes
    I tried looking into passing it into every thread but it seemed a bit
    too complicated for now
    """
    def __init__(self, input_dir:Path, output_dir:Path, db_path:Path):
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.logger = logging.getLogger("PubMedExtraction")
        # The PubMedDB is a factory class
        self.db = PubMedDB(db_path, self.logger)


    def traverse_second_level_dir(self, first_level_folder):
        """
        Here we will query the pubmed_pipeline_database and check if both xml_extraction_is_old is True,
        and pdf_extraction_is_old is True. After that, we can check if the article contains 1 pdf file, if it does,
        extract pdf and write to disk. else continue to the next file
        If there is 1 nxml, write to disk, else continuie to the next file

        updating manifest
        """
        for second_level_folder in first_level_folder.iterdir():
            # get rows with first level folder, second level folder and set rows to dictionary for efficient lookup

            for article_folder in second_level_folder.iterdir():
                try:
                    folder_name = article_folder.name.split('.')[0]
                    output_pdf_dir = self.output_dir / Path('pdf')  / first_level_folder.name / second_level_folder.name
                    output_xml_dir = self.output_dir / Path('xml') / first_level_folder.name / second_level_folder.name
                    # Count files written folder directory
                    current_article_folder_name_string = article_folder.name
                    #article_db_entry = manifest.get(current_article_folder_name_string)
                    updates = []
                    # Setting the pdf and xml update to True initially. When an update is succesful do we write it as true to db
                    pdf_update, xml_update = True, True

                    # Update stored pdf record, return pdf status
                    #if  article_db_entry and article_db_entry['pdf_needs_update']:
                        #pdf_update = self.write_main_pdf_from_tar_zip(article_folder,output_pdf_dir)
                    # Update stored xml file, return update status
                    #if article_db_entry and article_db_entry['xml_needs_update']:
                    xml_update = self.write_main_xml_from_tar_zip(article_folder, output_xml_dir)

                    # Updating Database
                    '''
                    if updates:
                        try:
                            updates.append({
                        'article_id':current_article_folder_name_string,
                        #'pdf_extraction_needs_update':pdf_update,
                        'xml_extraction_needs_update':xml_update
                    })
                        except Exception as e:
                            self.logger.exception("Error In Traverse Second Level Directory")
                    '''
                except Exception as e:
                    # handle errors with logging here
                    self.logger.error("Error In Traverse Second Level Directory: %s", e)

            # Update manifest At the conclusion of every second  level folder,
            # so there will be 256 updates per folder
            self.db.update_manifest(updates)

    def _process_second_level_dir(self, first_level_folder, second_level_folder):
        updates: List[Dict[str, bool]] = []
        manifest = {}
        manifest = self.db.get_files_in_dict_structure(first_level_folder.name,second_level_folder.name)
        folder_name = article_folder.name.split('.')[0]
        output_pdf_dir = self.output_dir / Path('pdf')  / first_level_folder.name / second_level_folder.name
        output_xml_dir = self.output_dir / Path('xml') / first_level_folder.name / second_level_folder.name

        for article_folder in second_level_folder.iterdir():

            # Count files written folder directory
            current_article_folder_name_string = article_folder.name
            article_db_entry = manifest.get(current_article_folder_name_string)
            updates = []
            # Setting the pdf and xml update to True initially. When an update is succesful do we write it as true to db
            pdf_update, xml_update = True, True

            # Update stored pdf record, return pdf status
            if article_db_entry and article_db_entry['pdf_needs_update']:
                pdf_update = self.write_main_pdf_from_tar_zip(article_folder,output_pdf_dir)
            # Update stored xml file, return update status
            if article_db_entry and article_db_entry['xml_needs_update']:
                xml_update = self.write_main_xml_from_tar_zip(article_folder, output_xml_dir)

            # Updating Database
            
            if updates:
                try:
                    updates.append({
                    'article_id':current_article_folder_name_string,
                    #'pdf_extraction_needs_update':pdf_update,
                    'xml_extraction_needs_update':xml_update
                    })
                except Exception as e:
                    self.logger.exception("Error In Traverse Second Level Directory")

            

    def write_main_xml_from_tar_zip(self, file_path,output_dir)-> bool:
        """
        Returns False, since we are working off the needs_update column
        """
        # count files written to output directory
        try:
            xml_file = []
            with tarfile.open(file_path, 'r:*') as tar:
                for member in tar.getmembers():
                    if member.isfile() and member.name.lower().endswith('.nxml'):
                        xml_file.append(member)

                if len(xml_file) > 1 or len(xml_file) == 0:
                    return False
                xml_file = xml_file.pop()
                file_data = tar.extractfile(xml_file)
                # Make the Output file path
                output_path = output_dir / xml_file.name
                output_path.parent.mkdir(parents=True, exist_ok=True)
                with open(output_path, 'wb') as f:
                    f.write(file_data.read())
                # update sql_table here
            return False

        except Exception as e:
            self.logger.error(f"Error In Write XML:{e} ")
            return True
    def write_main_pdf_from_tar_zip(self, file_path, output_dir)-> int:
        """
        There are many files within the tar,
        we want to save space for now, so we are extracting the nxml file and writing to disk,
        while keeping the pubmed structure to allow us to test nxml extraction later on
        """
        # count files written to output directory
        try:
            pdf_file = []
            with tarfile.open(file_path, 'r:*') as tar:
                for member in tar.getmembers():
                    if member.isfile() and member.name.lower().endswith('.pdf'):
                        pdf_file.append(member)
                        #print(member.name)

                if len(pdf_file) > 1 or len(pdf_file) == 0:
                    return False
                file = pdf_file.pop()
                file_data = tar.extractfile(file)

                output_path = output_dir
                output_path.mkdir(parents=True, exist_ok=True)
                with open(output_path, 'wb') as f:
                    f.write(file_data.read())
                # update sql_table here
            return False

        except Exception as e:
            self.logger.error(f"Error In Write PDF: {e}")
            return False


    def run(self, max_workers):
        first_level_folders = [f for f in self.input_dir.iterdir() if f.is_dir() and (len(f.name) == 2)]
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(self.traverse_second_level_dir,  folder) for folder in first_level_folders ]



class PubMedDB:
    """
    Factory Class
    """
    def __init__(self, db_path:Path, logger):
        self.db_path = db_path
        self.logger = logger

        # Turn on WAL for concurrent reads and writes
        conn = sqlite3.connect(self.db_path, timeout=3)

        conn.execute("PRAGMA journal_mode=WAL;")
        conn.close()

    def _connect(self):
        """
        Ensuring every thread has its own sqlite3 connection
        """
        try:
            conn = sqlite3.connect(self.db_path, timeout=30)
            conn.row_factory = sqlite3.Row
            return conn
        except Exception as e:
            print("Error in Connect")

    def update_manifest(self, updates: List[Dict[str, bool]]) -> None:
        """
        Note: This currently only updates the xml, for some reason the pdf column is blank.
        Given a list of dicts each containing:
            article_id: str,
            xml_needs_update: bool,
            pdf_needs_update: bool
        update the corresponding rows in articles_metadata.
        """
        try:
            conn = self._connect()
            cursor = conn.cursor()

            sql = """
                UPDATE articles_metadata
               SET xml_extraction_needs_update = ?
                WHERE article_id = ?
                """

            # Build a list of tuples in the exact order of the placeholders (?,?,?)
            params = [
                (entry["xml_extraction_needs_update"] , entry["article_id"])
                for entry in updates
            ]

            cursor.executemany(sql, params)
            conn.commit()
            conn.close()
        except Exception as e:
            self.logger.error("Error in Update Manifest: ", e)

    def get_files_in_dict_structure(self, first_level: str, second_level: str) -> Dict[str, Dict[str, bool]]:
        """
        Returns a dict of rows from articles_metadata keyed by article_id.
        Only rows for the given folders are returned.
        """
        try:
            # data structure of result
            result: dict[str, dict[str, bool]] = {}
            conn = self._connect()
            # So cursor.fetchall() gives you Row objects you can index by name
            cursor = conn.cursor()

            cursor.execute(
                """
                SELECT
                    article_id,
                    xml_extraction_needs_update   AS xml_needs_update,
                    pdf_extraction_needs_update   AS pdf_needs_update
                FROM articles_metadata
                WHERE first_level_dir = ?
                AND second_level_dir = ?
                """,
                (first_level, second_level)
            )
            rows = cursor.fetchall()
            conn.close()

            # Build a dict: { article_id: {column: value, …}, … }
            for row in rows:
                result[row['article_id']] = {
                    "xml_needs_update":bool(row["xml_needs_update"]),
                    "pdf_needs_update":bool(row["pdf_needs_update"]),
                }
            assert len(result) > 0, "Get Files Dict Structure Returning 0"

            return result
        except Exception as e:
            self.logger.error("Error In Get Files In Dictionary Structure: ", e)



def main():
    """

    """
    parser = argparse.ArgumentParser()
    # input path
    parser.add_argument("-i", "--input_dir", dest="input_path", type=Path, required=True)

    # output path
    parser.add_argument("-o", "--output_dir", dest="output_path", type=Path, required=True)

    args = parser.parse_args()

    # Get input paths from the shell args
    input_dir = Path(args.input_path)

    output_dir = Path(args.output_path)
    
    # db path
    db_path = Path(args.input_path) / <path_to_db_file>
    log_path = <path_to_log> 
    # configure once, before you instantiate or emit any logs
    configure_logging(log_path)

    # Initialize PubmedExtractor Class
    extractor = PubMedExtractor(input_dir, output_dir, db_path)
    extractor.run(max_workers=8)


if __name__ == "__main__":
    main()