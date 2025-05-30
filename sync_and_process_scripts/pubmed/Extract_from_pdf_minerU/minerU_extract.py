'''
SLURM accesible scripts

Strucuture of args from mineru_extract_template.sh
 1. input_data: what subset of oa_package we are looking to extract
 2. output_path: Where we are writing the data to
 3. Log_files: SLURM has their own runtime  logger script

RECOMMENDATION:
    Submit this file (using shell) multiple times, seperating the input data by n instances of 
    minerU_extract, so you can use more GPU as they become available, and 
    not have to wait for slurm to open up multiple GPUs and nodes. That can take 
    several days of waiting in the queue. The manifests are independent so 
    no worries about corruption as long as you are seperating input data at top 
    level folder. 
    
    I am keeping 1 file here as the template, so there isnt multiple copies within the 
    same repository.
'''
import logging
import time
from pathlib import Path
import argparse
import pandas as pd
import json
from concurrent.futures import ProcessPoolExecutor, as_completed
from magic_pdf.data.data_reader_writer import FileBasedDataWriter, FileBasedDataReader
from magic_pdf.data.dataset import PymuDocDataset
from magic_pdf.model.doc_analyze_by_custom_model import doc_analyze
from magic_pdf.config.enums import SupportedPdfParseMethod
from magic_pdf.data.read_api import read_local_images
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
logger = logging.getLogger(__name__)


def minerU_logic(pdf_file_name, output_path, folder_manifest) -> dict[str, bool]:
    '''
    The core of the minerU logic
    '''
    # If needed for spawned processes, you can check and (re)configure:
    try:
        manifest_col = 'minerU_extraction_needs_update'
        # Prepare environment: output directories for markdown, images, equations, and tables
        local_image_dir = output_path / Path("images")
        local_md_dir = output_path
        local_equation_dir = output_path / Path("equations")
        local_table_dir = output_path / Path("tables")

        # Create directories if they don't exist
        for directory in [local_image_dir, local_md_dir, local_equation_dir, local_table_dir]:
                directory.mkdir(parents=True, exist_ok=True)

        # Use only the basename of the image directory when needed
        image_dir = local_image_dir.name

        # Initialize the writers
        image_writer = FileBasedDataWriter(str(local_image_dir))
        md_writer = FileBasedDataWriter(str(local_md_dir))

        # Read PDF Bytes using reader
        reader1 = FileBasedDataReader("")
        pdf_bytes = reader1.read(str(pdf_file_name))  # Ensure a valid path is provided

        # Process the PDF by chaining the calls:
        ds = PymuDocDataset(pdf_bytes)
        inference_result = ds.apply(doc_analyze, ocr=True)
        pipe_result = inference_result.pipe_ocr_mode(image_writer)
        name_without_suff = pdf_file_name.name
        pipe_result.dump_md(md_writer, f"{name_without_suff}.md", image_dir)


        # --- Extending the pipeline to extract tables and inline equations ---

        # The PipeResult is stored inside the dataset.
        # Access the results (this structure is based on the documented json structure).
        pipe_result =  pipe_result._pipe_res

        # Ensure we have the expected structure
        if "pdf_info" in pipe_result:
            for page_idx, page in enumerate(pipe_result["pdf_info"]):
                # Write table blocks: these are provided in the "tables" field.
                for table_idx, table in enumerate(page.get("tables", [])):
                        table_filename = local_table_dir / f"{name_without_suff}_page{page_idx}_table{table_idx}.json"
                        with open(table_filename, "w") as f:
                            json.dump(table, f, indent=2)

                # Write interline equations: these are provided in the "interline_equations" field.
                for eq_idx, eq in enumerate(page.get("interline_equations", [])):
                        eq_filename = local_equation_dir / f"{name_without_suff}_page{page_idx}_block_eq{eq_idx}.json"
                        with open(eq_filename, "w") as f:
                            json.dump(eq, f, indent=2)

                # Write inline equations: these appear as spans with type "inline_equation" in para_blocks.
                for block in page.get("para_blocks", []):
                        for line in block.get("lines", []):
                            for span in line.get("spans", []):
                                if span.get("type") == "inline_equation":
                                # Create a filename using page and bbox info or a simple counter
                                    inline_eq_filename = local_equation_dir / f"{name_without_suff}_page{page_idx}_inline_eq.json"
                                    # You might want to aggregate inline equations per page or write each individually
                                    with open(inline_eq_filename, "a") as f:  # append mode if aggregating
                                        json.dump(span, f, indent=2)
                                        f.write("\n")  # separate entries by newline
            folder_manifest.loc[folder_manifest["article_id"] == pdf_file_name, manifest_col] = False  # or True, depending on your logic

        else:
                logger.warning("No 'pdf_info' found in the pipeline results.")
                folder_manifest.loc[folder_manifest["article_id"] == pdf_file_name, manifest_col] = True
    except Exception as e:
        logger.error('Error In MinerU Logic %s', e)
        folder_manifest.loc[folder_manifest["article_id"] == pdf_file_name, manifest_col] = True


def process_folder(first_level_folder, output_dir):
    '''
    manifest_path = /pl/active/acuna/oa_package
    '''
    oa_package_path = Path("/pl/active/acuna/oa_package")
    updates = []
    for second_level_folder in first_level_folder.iterdir():
        logger.info("Processing Folder %s", second_level_folder.name)
        manifest_path = oa_package_path / first_level_folder.name / second_level_folder.name / f"{first_level_folder.name}_{second_level_folder.name}_manifest.parquet"
        folder_manifest = pd.read_parquet(manifest_path)
        for article_folder in second_level_folder.iterdir():
            # Article_tar is used to query the dataframe by article_id
            # We need to add the 
            article_tar = article_folder.name + ".tar.gz"
            tracker = folder_manifest.query("article_id == @article_tar")
            # If not in tracker, we want to create a new entry
            if tracker.iloc[0]['minerU_extraction_needs_update'] == True:                    
                try:
                    logger.info("Processing Folder %s", article_folder)
                    files = [pdf_file for pdf_file in article_folder.iterdir()]
                    print("PDF File to be processes", files)
                    if len(files) > 1:
                            continue
                    main_pdf = files.pop()
                    output_dir = output_dir /  first_level_folder.name / second_level_folder.name / article_folder.name
                    minerU_logic(main_pdf, output_dir, folder_manifest)
                except Exception as e:
                    logger.error('Error In Processing Folder %s', e)
                    return []
        # Writing manifest updates to memory
        folder_manifest.to_parquet(manifest_path)
    return updates


def run(input_folders: list[Path], output_path: Path):
    # Parallel computing
    cpu_workers = 8
    with ProcessPoolExecutor(max_workers = cpu_workers) as executor:
        futures = [ executor.submit(process_folder, first_level_folder, output_path) for first_level_folder in input_folders if len(first_level_folder.name) == 2 \
                   and first_level_folder.is_dir()]
        for future in as_completed(futures):
            try:
                result = future.result()
                if result:
                    logger.info("Finished Processing Folder", len(result), result[:10])
            except Exception as e:
                logger.error("Error in processing folder: %s", e)


def divide_by_quarters(input_list:list[Path], n=5):
    lists = [[] for N in range(n)]
    i = 0
    for elem in input_list:
        lists[i].append(elem)
        i = (i + 1) % n
    return lists
    

def main():
    '''
    This is the function we will use in the template.py as the entry point for
    the multiprocessing logic.
    Args:
        input_data:
        output_path:
        logger? (not needed at the moment)
    '''
    # args
    p = argparse.ArgumentParser()
    p.add_argument("-i","--input_dir",  type=Path, required=True)
    p.add_argument("-o","--output_dir", type=Path, required=True)
    args = p.parse_args()
    input_dir  = args.input_dir
    output_dir = args.output_dir; output_dir.mkdir(parents=True, exist_ok=True)

    # 3)my folders, broadcast
    all_folders = sorted(f for f in input_dir.iterdir()
                             if f.is_dir() and len(f.name)==2)
    output_dir.mkdir(parents = True, exist_ok=True)
    
    # Configure logging from within main
    log_path = output_dir / "extraction_using_MinerU.log"
    configure_logging(log_path)
    
    lists = divide_by_quarters(input_list=input_dir)
    run(lists[0], output_dir)

if __name__ == "__main__":
    main()