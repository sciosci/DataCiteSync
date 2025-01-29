## Primary goal
 1. Re-create the nested two digit hexidecimal folder structure on our computer, with correct assignment of .tar.gz on 
  oa_package
 2. Create a manifest to track status and additional informatio about every tar file
    2A. This is a sqlite3 database in our current iteration. 

#### Observations from Mondays code review
    - Too many 'hacking solutions'. They make code reviews difficult and sustainment even more so. 
        Example:
            - Binary download Retries
            - While Else loop


#### Changes made: 
    - Added a class FileMetadataContent(TypedDict) to 
    improve 
    - get_gzip_data_in_memory() would open the .gz files, read binary 'rb' and store binary in memory,
    along with additional .tar.gz metadata such as supplelemtal file counts and types. The read binary in stream 
    was causing increase RAM usage as well as FTP server disconnections. 
    - reduced total threads to 3, I've found with additional threads there are more disconnections more often.

          

