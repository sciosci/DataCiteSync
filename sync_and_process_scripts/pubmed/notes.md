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

          

