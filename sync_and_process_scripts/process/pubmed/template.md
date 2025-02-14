# Template for how process_oa should be structured

    extract_data_by_docling():
  return 1, 1, 1
extract_pdf_info: [inputs: output_base_dir, file_path, engine<docling|mineru>]
  load_file from file path
  if engine is docling
    image_count, table_count, equation_count = extract_data_by_docling()
  else if engine is mineru
    image_count, table_count, equation_count = extract_data_by_mineru()
  result_stats = {
          "image_count": image_count,
          "table_count": table_count,
          "equation_count": equation_count,
      "time_elapsed": time_elapsed
    }
  return result_stats
def main():

Main:
FILE_LIMIT = 1
file_list = get_tar_file_list(...)
manifest = load_article_manifest(...)
if FILE_LIMIT is 0:
  FILE_LIMIT = length of file_list
for each file in file_list[:FILE_LIMIT]:
  # Use docling
  docling_stats = extract_pdf_info(output_base_dir, file.path, "docling")
  # Use MinerU
  mineru_stats = extract_pdf_info(output_base_dir, file.path, "mineru")  
  add docling_stats to manifest based on file.id
  add miner_stats to manifest based on file.id