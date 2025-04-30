Pubmed is an FTP Server. 
List of all zip files on pubmed FTP server
https://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_file_list.txt
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
```
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

---
Final Notes

# MinerU
### If there are no GPUs available
MinerU can be run with CPU only, there is a key inthe magic-pdf.json
"device-type":"cuda" that needs to be switched to "cpu" before
it will recognize runs as cpu only. If this is not switched then script will error out. 
**Runtime**:
MinerU with CPU only took an average of 350 seconds per file ON 
SLURM while using 2 nodes MPI, 8 tasks per node and 128 GBs of CPU memory.
Why I did not increase the number of nodes with MPI higher?
Because % Utilization on CPU only was at most 10%,
which never made sense to me because if I would decrease the # of processes,
then time for minerU to process each PDF file would stay the same, while CPU
utilization would still stay below 10%.

### If there are GPUs available.
Why do I request only 1 GPU and one node at a time with minerU?
When I would use multiple GPUs, and multiple nodes, queue time would
regularly exceed 24 hours, due to the low amount of GPU specific resources. 
So my work around for this, is submitting multiple scripts, each requesting 1 GPU and 
1 node. So there wait time will be less than a couple hours at most, and if there is low
demand and multiple GPUs are available, we will still get the benefit of multiple GPU
processes. 

### Is there any issues with file status tracking if there are multiple scripts running?
I changed the manifest from being a single sqlite3 file, to being pandas dataframes stored as parquets within
every second level directory. 
Example: 01/02/01_02_manifest.parquet, will store the information for articles within 01/02 folder. 