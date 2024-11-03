import ftplib
from pathlib import Path


def connect_to_pubmed(ftp_server:object, directory:str) -> object:
   
    ftp = ftplib.FTP(ftp_server)
    # Login anonymous user and password
    ftp.login()
    # cwd is change into /dir
    ftp.cwd(directory)
    return ftp

    

def process_first_level_directory(ftp, base_output:str)->list[str]:
    # filenames = ftp.nlst()
    files = []
    # get filenames
    ftp.retrlines('NLST',files.append)
    for file in files:
        output_path = Path(f'{base_output}/{file}')
        output_path.mkdir(parents=True, exist_ok=True)
    return files

def process_second_level_directory(ftp:object, parent_dir:list[str])->list[str]:
    """
    for every parent folder, 
    create the sub folder directory
    """
    for dir in parent_dir:
        # enter into that dir in the ftp
        ftp.cwd(dir)
        folders = []
        # get all folders that belong to the parent directory
        ftp.retrlines('NLST',folders.append)
        for folder in folders:
            output_path = Path(f'{base_output}/{dir}/{folder}')
            # create child directorys
            output_path.mkdir(parents=True, exist_ok=True)
            write_files_to_directory(ftp,output_path=output_path, sub_dir=folder)
            
        # back out, to create sub directories for other folders
        ftp.cwd('..')
        # return after first folder filled to make sure function works correctly

def write_files_to_directory(ftp:object, output_path:Path, sub_dir:str ): 
    """
    download files into appropriate directory
    """
    manifest = []
    ftp.cwd(sub_dir)
    files = []
    ftp.retrlines('NLST', files.append)
    for file in files:
        #output file destination 
        output_file_path = output_path / file
        if file not in manifest:
            print(f'downloading {file}')
            with open(output_file_path, 'wb') as f:
                # download but do not extract zip files
                ftp.retrbinary(f'RETR {file}', f.write)
    #back out of sub_dir to download files for next dir
    ftp.cwd('..')   

if __name__ == "__main__":
    ftp_server = 'ftp.ncbi.nlm.nih.gov'
    main_dir = r'/pub/pmc/oa_package' 

    base_output = r''
    # Make sure FTP server is live and we can still connect to it    
    ftp = connect_to_pubmed(ftp_server=ftp_server, directory= main_dir)

    # create output top structure
    parent_folders = process_first_level_directory(ftp=ftp, base_output=base_output)

    #create second level directory
    second_level_dir = process_second_level_directory(ftp, parent_dir = parent_folders )

    # write output .gzip files per directory









    # populate output data

    # disconnect from FTP
    ftp.quit()


