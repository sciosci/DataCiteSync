## Primary goals of get_pubmed.py
 1. Re-create the nested two digit hexidecimal folder structure on our computer, with correct assignment of .tar.gz on 
  oa_package
 2. Have a way to track the .tar.gz file statuses, for bi-weekly updates
    (How can we achieve goal #2)
        Create a local manifest to track information about every tar file
        - This is a sqlite3 database in our current iteration. 

#### Observations from Mondays code review
    - Too many 'hacking solutions'. They make code reviews difficult and sustainment even more so. 
        Examples:
            - Binary download stored in memory then written to storage later
            - While Else loop for retrys
            - 


#### Changes made: 
    - Added a class FileMetadataContent(TypedDict) to easily track when I am working with .tar.gz metadata
    improve 
    - get_gzip_data_in_memory() would open the .gz files, read binary 'rb' and store binary in memory,
    along with additional .tar.gz metadata such as supplelemtal file counts and types. The read binary in stream 
    was causing increase RAM usage as well as FTP server disconnections. 
    - reduced total threads to 3, I've found with additional threads there are more disconnections more often.


Notes from Jan 28th, 2025
 - Added strict python type checking
 - retrBinary read binary (rb) is unnecesary, write binary 'wb' can download to output folder and 
 overrides old files so it takes care of tracking old files in memory to be re-written, as well as removing 
 write_binary_to_storage. ftp.mlsd() returns file_name as well as necessary metadata for tracking file versions. 
 - Moved manifest functions to a manifest_helper_functions script to reduce clutter in a single file. 
 - Using queue.Queue() to create a thread-safe object where I 
    place file_metadata that will be written to manifest.  
 
 - What needs improving:
    (All tests are run with grobid, not local machine)
    - With current threadpool setup, when I use multiple threads, the FTP object disconnects,
    closes current thread, then begins a new thread in a seperate first level folder. I have inspected CPU usage
     using `htop` while running script with up to 10 threads and CPU utilization is never above 10%.
    I do not get this issue when I use 1 thread, so current file can use 1 or many threads depending on what is commented
    out in the main() function

January 29th Feedback:
    - Decouple the sqlite manifest, give every first level folder a sqlite table
        - Get the metrics (RAM, number of files, time to complete, for every second level directory)
    - Remove the consumer thread, instead, inside the process_folder, just check length of 
    metadata list, and write it to manifest from there.   

# Version 2
1. get first level dir from FTP server
2. for each dir (parrallel oppourtunity)
    - get subdir structure (ftp connection)
    - get file list 
