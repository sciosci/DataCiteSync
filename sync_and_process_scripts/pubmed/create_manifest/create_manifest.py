"""
Purpose of this Script:
    - After investing a lot of time into sqlite3 for a oa_package manifest,
      I am moving instead to work with Pandas dataframes.
    This will allow us to work with parallel processing without needing to worry about database locks.
    
"""
from pathlib import Path
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import logging
import argparse
logger = logging.getLogger(__name__)

def configure_logging(log_path: Path):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s: %(message)s",
        handlers=[
            logging.FileHandler(log_path),    # writes to file
            logging.StreamHandler()           # prints to console
        ]
    )


def traverse_second_level_folder(first_level_folder, output_dir):
    for second_level_folder in first_level_folder.iterdir():
        if not second_level_folder.is_dir():
            continue
        # Create fresh records list for this second-level folder
        # Process all article folders in this second-level folder
        oa_records = []
        for article_folder in second_level_folder.iterdir():
            if article_folder.is_dir() or article_folder.name.endswith('.parquet'):
                continue
            oa_records.append({
                'article_id': article_folder.name,
                'last_updated': datetime.now(),
                'xml_extraction_needs_update': True,
                'pdf_extraction_needs_update': True,
                'minerU_extraction_needs_update': True,
            })
        # After collecting all articles in this second-level folder, create DataFrame
        if oa_records:  # Only proceed if we found articles
            df = pd.DataFrame.from_records(
                oa_records,
                columns= [
                    'article_id',
                    'last_updated',
                    'xml_extraction_needs_update',
                    'pdf_extraction_needs_update',
                    'minerU_extraction_needs_update'
                ]
            )
            # Create output directory matching the second-level folder's location
            out_dir = output_dir / first_level_folder.name / second_level_folder.name
            out_dir.mkdir(parents=True, exist_ok=True)
            out_file = out_dir / f"{first_level_folder.name}_{second_level_folder.name}_manifest.parquet"

            try:
                df.to_parquet(out_file, index=False)
            except Exception as e:
                logger.error(f'Error saving manifest for {second_level_folder.name}: {e}')



def  main():
    # connect to input directory
    parser = argparse.ArgumentParser()
    # input path
    parser.add_argument("-i", "--input_dir", dest="input_path", type=Path, required=True)

    # output path
    parser.add_argument("-o", "--output_dir", dest="output_path", type=Path, required=True)

    args = parser.parse_args()

    # Get input paths from the shell args
    input_dir = Path(args.input_path)

    output_dir = Path(args.output_path)

    # Configure logging from within main
    log_path = input_dir / "create_manifest.log"
    configure_logging(log_path)

    # get first level folders
    # There are alot of unnessary folders in oa_package
    first_level_folders = [folder for folder in input_dir.iterdir() if len(folder.name) ==2]

    # Begin multithreading folder crawls
    workers = 15
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = { executor.submit(traverse_second_level_folder, fl, output_dir): fl
            for fl in first_level_folders }
        for fut in as_completed(futures):
            try:
                fut.result()
            except Exception:
                logger.exception("Worker failed for %s", futures[fut])


if __name__ == "__main__":
    main()