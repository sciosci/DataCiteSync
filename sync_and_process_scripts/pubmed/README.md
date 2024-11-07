Pubmed is an FTP Server. 

List of all zip files on pubmed FTP server
https://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_file_list.txt


FTP Where all the files are stored

https://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_package/

``` python


"""
Output file structure for pubmed

oa_package/
    00/
        00/
        01/
    01/
        00/
        01/
            PMC1214.tar.gz

Get first level directories
For each first level directories
    Get second level directories
    For each second level directories
        List files within second level directory
        For each file in second level directory
            Get metadata of file in FTP server
            if (last updated data of file is greater than last ran script) or (File does not exist)
                download file
                update manifest with new updated timestamp

=======================================================================================================
                    
first_level_dir = get_first_level_dir(ftp, main_dir) 
for first_level_entry in first_level_dir:
    updated_manifest = process_first_level_dir(first_level_entry, manifest, base_output_dir)      

=======================================================================================================

process_first_level_dir: input -> first_level_entry, manifest, base_output_dir
if output_dir_for_first_level_entry does not exist
    make dir for first_level_entry

second_level_dir = get_second_level_dir(ftp, first_level_entry)
for second_level_entry in second_level_dir:
    updated_manifest = process_second_level_dir(second_level_entry, manifest, base_output_dir, first_level_entry)
=======================================================================================================
              
process_second_level_dir: input -> second_level_entry, manifest, base_output_dir, first_level_entry
if output_dir_for_second_level_entry does not exist
    make dir for second_level_entry

files_within_second_level = get_files_in_second_level_dir(ftp, second_level_entry)
for filename in files_within_second_level:
    process_file(first_level_entry, second_level_entry, manifest, filename)
=================================================================================================

Manifest Structure

Columns in table 1
- id
- start time
- end time
- records updated (count)
- records added (count)
- path to details of sync (a string to a parquet file)

Table 2 columns
- article id
- first level directory
- second level directory
- file created timestamp
- file updated timestamp
- image count
- has nxml
- has pdf



"""
```


Creating an extract data file


folder of interest: oa_package

I want to create a script that once mounted to PL from grobid, we can get the data for pubmed

How the pubmed + manifest directories currently look in my local branch

output_dir / 
   data / 
   
         00/
           00/ 
              (zip files)
                  ...
         01/ 
         02/
 
  manifest.db
                         















