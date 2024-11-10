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
                         

To do's: 
 ### High Level
 Create a copy of pubmed data to PL
    R1. identify the file structure on FTP server
        (Systematic approach of downloading a very large amount of data) 
        two digit hexidecimal name
    R2. Re-create the nested two digit hexidecimal folder structure on our computer
    
    - Method of access:
        - 

    #- How do we access the FTP server
    #    - Do we need credentials to access endpoint, (no, it is a public server)

     Do we need to track what files we have synced from pubmed.
        - Yes, tracking helps us moniter the file status in terms of it being up to date 

    R3. What is the status of each file,
        - Do we need to update it, is it new, what needs to be changed if there is anything

    R4. Sync Attempts. 
        - How long does the script run, how many files were updates, last time we synced the files
        - last time we checked
        - Needed to track our copy of pubmed against the live FTP server, 
        - lets us know how trustworthy our copy is


### What has been done:

- identified pubmed ftp structure, R1, Done


- get_pubmed.py
    - This script creates copy of pubmed on its output_dir, 
    - syncs from scratch if there is no ouput dir, R2, Done
    - creates manifest to track synced files, R3
        - articles_status: R3 Done
        - run_log: R4
    - updates manifest and data files if file is outdated, R4 WIP  
        - update manifest R3 and R4, Pending
        - update data files, R3 WIP
        

- create_remote_manifest.py : 
    - creates a manifest from already existing pubmed copy in dir, R3 WIP

### Current Errors I am facing
- update data files, R3 WIP
    - EOFError on FTPLib, 
    - Possible Cause from initial investigation: 
        FTP servers can see client as idle when reading files, if
        the files are large, disconnection from FTP server can occur
