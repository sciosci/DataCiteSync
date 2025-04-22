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
        ...
        /ff
    01/
        00/
        01/
            PMC1214.tar.gz
        ..
        /ff

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
        - Yes, tracking helps us monitor the file status in terms of it being up to date 

    R3. What is the status of each file,
        - Do we need to update it, is it new, what is the FTP path from oa_package to reach the file.What needs to be changed if there is anything

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
    [Resolved] EOFError on FTPLib, (Tentative Solution Found. )
    - Possible Cause from initial investigation: 
        FTP client disconnects due to idle time,  

---

November 13th, 2024
    # High Level:
    What are we trying to do:
        1. Create a copy of pubmed data in peta library (see above, for more information on this)
        2. Create a manifest to track the pubmed information already in PL

# Focusing on High Level #2, what is the manifest, and what will it look like? 
    ### : Creating PL manifest background
    In PL, there already is a folder named oa_package, which contains a sample 
    of the pubmed data. In the same two-digit nested hexidecimal structure.
    I am tasked with expanding the data of oa_package from a sample of pubmed to a local
    copy.

    So, what is a manifest? 
    - A manifest, is a record, which the administrators can use to track our pubmed copy in PL.

    # Note:
        For our use case a sqLite database will work. 
   

### Requirements of creating a manifest from oa_package
     Do we need to track what files we have downloaded from pubmed.
        - Yes, tracking helps us moniter the file status in terms of it being up to date 

     What is the status of each file,
        - Do we need to update it,, what is the FTP path from oa_package to reach the zip file.What needs to be changed 

     Sync Attempts. 
        - How long does the script run, how many files were updates, last time we synced the files
        - last time we checked
        - Needed to track our copy of pubmed against the live FTP server, 
        - lets us know how trustworthy our copy is

    ** Output from script, the manifest **
    Our manifest will consist of two sheets: 
    1. articles_metadata:
        '''metadata information about our zip files''' 
            path to reach file
            files contained such as pdfs, xml, gifs, images
            when was the file last updated on pubmed
            when did we download the file
    2. runtime_data:
        '''runtime information about getting data from pubmed'''
            new files added
            files updated
            date executed
            runtime start
            runtime stop
            errors caught in runtime





---
# get_pubmed continuiation

November 13th, 2024.

To refresh:

----

January Notes



