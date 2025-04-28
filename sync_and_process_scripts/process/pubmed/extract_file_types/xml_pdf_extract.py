'''
Information for this file:   

We use MPI for increase throughput 

'''

from mpi4py import MPI
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import argparse, json
import tarfile
import logging
from datetime import datetime
from typing import List, Dict
import pandas as pd

def configure_logging(log_path: Path):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s: %(message)s",
        handlers=[
            logging.FileHandler(log_path),    # writes to file
            logging.StreamHandler()           # prints to console
        ]
    )
logger = logging.getLogger(__name__)

def update_xml_status(manifest, tar_path, out_dir):
    """ since xml and pdf have similar logic, I am, using this to try and avoide code duplication """
    return handle_status(
        manifest, tar_path, out_dir,
        ext=".nxml",
        manifest_col="xml_extraction_needs_update",
        log_label="XML"
    )

def update_pdf_status(manifest, tar_path, out_dir):
    return handle_status(
        manifest, tar_path, out_dir,
        ext=".pdf",
        manifest_col="pdf_extraction_needs_update",
        log_label="PDF"
    )

def handle_status(folder_manifest, article_tar: Path, output_dir: Path, ext: str, manifest_col: str, log_label: str):
    """Where the files are written to disk and the manifest is updated inplace"""
    tracker = folder_manifest.query("article_id == @article_tar.name")
    # If not in tracker, we want to create a new entry
    if not tracker.empty:  # Check if tracker is not empty
        if tracker.iloc[0][manifest_col] == True:
            try:
                # Open the tar file - this was missing
                with tarfile.open(article_tar, "r:gz") as tar:
                    members = [m for m in tar.getmembers() if m.isfile() and m.name.lower().endswith(ext)]
                    if len(members) != 1:
                        # If more than 1 or no members, we want to skip the file
                        return 
                    ext_file = members.pop()
                    file_data = tar.extractfile(ext_file)
                    # Making output_path, to include PMCid (without the .tar.gz)
                    output_path = output_dir / Path(ext_file.name)  # This would be ./xml/06/da/PMC3395936/IJPS-6-37.nxml
                    output_path.parent.mkdir(parents=True, exist_ok=True)
                    logger.info(f'Output_path: {output_path} # Ext File:{ext_file} # File data: {file_data}')
                    # Writing the file to disk, with the correct extension (pdf or nxml)
                    with open(output_path, "wb") as f:
                        f.write(file_data.read())

                # Update the manifest, article needs update is now false
                folder_manifest.loc[folder_manifest["article_id"] == article_tar.name, manifest_col] = False
            except Exception:
                logger.error(f"Error in {log_label} for {article_tar}", exc_info=True)
                folder_manifest.loc[folder_manifest["article_id"] == article_tar.name, manifest_col] = False
        
        else:
            logger.warning(f"No match found for article_id: {article_tar.name}")
            return  # Skip if no match found
    

def traverse_second_level_dir(first_level_folder:Path, output_dir:Path):
    """ """
    try:
        for second_level_folder in first_level_folder.iterdir():
            
            output_pdf_dir = output_dir / Path('pdf_extracted_from_zip')  / first_level_folder.name / second_level_folder.name
            output_xml_dir = output_dir / Path('xml_extracted_from_zip') / first_level_folder.name / second_level_folder.name
            # MPI reduces the number of layers in the path? This is a patch but I need to investigate further.
            folder_manifest = pd.read_parquet(second_level_folder / f"{first_level_folder.name}_{second_level_folder.name}_manifest.parquet")
            for article_folder_path in second_level_folder.iterdir():
                # XML and PDF status updates
                # Connect to parquet file, stored in second level folder, 00/00/00_00_manifest.parquet
                update_xml_status(folder_manifest, article_folder_path, output_xml_dir)
                update_pdf_status(folder_manifest, article_folder_path, output_pdf_dir)
            # Saving the updated manifest
            # After processing all articles in a second-level folder
            folder_manifest.to_parquet( second_level_folder / f"{first_level_folder.parent.name}_{first_level_folder.name}_manifest.parquet", index=False) 
    except Exception as e:
            logger.error("Error in process_second_level_dir: %s. First Level Folder: %s, Second Level Folder: %s", e, first_level_folder, second_level_folder)

def process_folder(first_level_folders: list[Path], output_dir: Path):
    """
    Traverse 
    """
    #ThreadPool: one thread per first‐level folder
    with ThreadPoolExecutor(max_workers=32) as pool:
        futures = { pool.submit(traverse_second_level_dir, fld, output_dir): fld
                    for fld in first_level_folders if fld.is_dir() }
        reports = []
        for fut in as_completed(futures):
            try:
                reports.append(fut.result())
            except Exception as e:
                print(f"[Rank] Error on {futures[fut]}: {e}")


def main():
    # 1) MPI init
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    # 2) args
    p = argparse.ArgumentParser()
    p.add_argument("-i","--input_dir",  type=Path, required=True)
    p.add_argument("-o","--output_dir", type=Path, required=True)
    args = p.parse_args()
    input_dir  = args.input_dir
    output_dir = args.output_dir; output_dir.mkdir(parents=True, exist_ok=True)

    # 3) master lists folders, broadcast
    if rank == 0:
        all_folders = sorted(f for f in input_dir.iterdir()
                             if f.is_dir() and len(f.name)==2)
    else:
        all_folders = None
    all_folders = comm.bcast(all_folders, root=0)

    # 4) split by rank
    my_folders = [fld for i,fld in enumerate(all_folders) if i % size == rank]
    configure_logging('logging_xml_pdf.log')
    process_folder(my_folders, output_dir)
      
if __name__ == "__main__":
    main()