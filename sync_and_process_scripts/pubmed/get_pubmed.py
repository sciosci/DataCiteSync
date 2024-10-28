import ftplib

def connect_to_pubmed():
    ftp_server = 'ftp.ncbi.nlm.nih.gov'
    ftp = ftplib.FTP(ftp_server)
    ftp.login()

    ftp.cwd('/pubmed/baseline/')
    filenames = ftp.nlst()

    print(len(filenames))



if __name__ == "__main__":
    connect_to_pubmed()