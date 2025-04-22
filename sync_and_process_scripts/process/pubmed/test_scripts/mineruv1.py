import tarfile
from pathlib import Path

from magic_pdf.data.data_reader_writer import FileBasedDataWriter, FileBasedDataReader
from magic_pdf.data.dataset import PymuDocDataset
from magic_pdf.model.doc_analyze_by_custom_model import doc_analyze


def get_files(input_data):
    file_list = []
    for folder in input_data.iterdir():
        for file in folder.iterdir():
            file_list.append(file)

    return file_list


def extract_pdf_from_tar(file_list):
    pass

def write_pdf_file(file, output_dir):

    # Open each tar.gz file in the folder
    with tarfile.open(file, "r:gz") as tar:
        # I haven't found a way to get file name, not all pdfs are names main.pdf
        for member in tar.getmembers():
            # Look for PDF files
            if member.name.endswith(".pdf"):
                tar.extract(member, path=output_dir)




def main():

    # input_data = Path('/home/dimu6211/comparative_study_mu_doc/test_data')
    input_data = Path('/home/dimu6211/comparative_study_mu_doc/miner_u_scripts/temp_file/PMC6427624')

    files = get_files(input_data)

    #temp = Path('./temp_file')
    #temp.mkdir(parents=True, exist_ok=True)
    #write_pdf_file(file= files[1],output_dir= temp)
    pdf_file_name = files[0]
    name_without_suff = pdf_file_name.stem

    # prepare enviornment
    local_image_dir = Path("output/images")
    local_md_dir = Path("output")

    # Create directories if they don't exist
    local_image_dir.mkdir(parents=True, exist_ok=True)
    local_md_dir.mkdir(parents=True, exist_ok=True)

    # use only the basename of the image directory when needed
    image_dir = local_image_dir.name

    # initialize the writers
    # FileBasedDataWriter, FileBasedDataReader
    image_writer = FileBasedDataWriter(str(local_image_dir))
    md_writer =FileBasedDataWriter(str(local_md_dir))

    # Read PDF Bytes using reader
    reader1 = FileBasedDataReader("")
    pdf_bytes = reader1.read(str(pdf_file_name)) # Ensure a valid path is provided


    # Process the PDF
    ds = PymuDocDataset(pdf_bytes)
    ds.apply(doc_analyze, ocr=True).pipe_ocr_mode(image_writer).dump_md(md_writer, f"{name_without_suff}.md", image_dir)





if __name__ == '__main__':
    main()