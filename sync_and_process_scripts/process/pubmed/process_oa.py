'''
purpose of process_oa.py
- Extract images, tables, text, equations from pdf files stored in oa_package folder 
- Create a manifest to track extracted values from pdf (this may only be needed for comparative study)

To-Do's:

Manifest.parquet
[ ] Create manifest 
[ ] Populate manifest with output value counts from MinerU, and Docling

Docling:
[ X ] Get docling to run, 
[ X ] Extract values from single pdf file,
[ X ] Extract values from multiple pdf files, structuring output so each pdf file is its own folder
[  ]  Track extracted values in manifest
  
MinerU:

'''


import tarfile
from typing import List, Dict, TypedDict
from pathlib import Path
import pandas as pd
from process.pubmed.docling_scripts.docling_grobid_v2 import get_pdf_info_docling_stream, docling_parse
from processed_types import ParsedCount
import argparse


def get_tar_file_list(data_dir)->List[Path]:
    print('test')
    return [x for x in data_dir.iterdir()] 

def create_or_load_manifest(output_dir):
    pass

def get_file_name_without_suffix(file_path):
    suffix = '.tar.gz'
    name = file_path.name
    if name.endswith(suffix):
        return name[:-len(suffix)]
    return name  # Return the original name if the suffix isn't present.


def extract_pdf_info(output_base_dir, file_list, engine):
    """
    for file files:
        if engine == docling:
            image_count, table_count, equation_count = extract_data_by_docling()
        elif engine == minerU:
            image_count, table_count, equation_count = extract_data_by_miner_u
    result_stats = { "image_count": image_count,
          "table_count": table_count,
          "equation_count": equation_count,
      "time_elapsed": time_elapsed}
    """
    parsed_count: ParsedCount
    for file in file_list:
        if engine == 'docling':
            print('engine is: ', engine)
            file_stream = get_pdf_info_docling_stream(file=file)
            file_name = get_file_name_without_suffix(file)
            # return
            parsed_count = docling_parse(file_stream=file_stream, output_path=output_base_dir, tar_file_name=file_name)
    

def main():
    '''
    FILE_LIMIT = 1
    file_list = get_tar_file_list(...)
    manifest = load_article_manifest(...)
    
    if FILE_LIMIT = 0:
        FILE_LIMIT = length of file list
    
    for each file in file_list[:FILE_LIMIT]
        #use docling
        docling_stats =extract_pdf_info(output_base_dir, file.path, "docling")
         # Use MinerU
    mineru_stats = extract_pdf_info(output_base_dir, file.path, "mineru")  
    add docling_stats to manifest based on file.id
    add miner_stats to manifest based on file.id
    '''
        # Parse command line arguments
    parser = argparse.ArgumentParser(description="""
    Extracting values from PDF items including tables, equations, text, and images 
""")
    
    parser.add_argument(
        "-o", "--output_dir", help="Output base directory of folder with extracted values files"
    )
    parser.add_argument(
        "--input_dir", help="FTP host endpoint" 
    )
    
    

    arguments = parser.parse_args()
    
    
    FILE_LIMIT = 0
    data_dir = Path(arguments.input_dir)
    ouput_dir = Path(arguments.output_dir)
    
    file_list = get_tar_file_list(data_dir)
    
    #manifest = create_or_load_manifest(ouput_dir)
    
    if FILE_LIMIT == 0:
        FILE_LIMIT = len(file_list) 
    
    # begin looping through data
    extract_pdf_info(output_base_dir=ouput_dir, file_list=file_list,engine='docling')


if __name__ == "__main__":
    main()