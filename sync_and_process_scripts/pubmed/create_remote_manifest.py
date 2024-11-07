from pathlib import Path
import argparse

def main():
    """
    Ideal outputs:
        - A manifest of the information already stored in oa_package folder in PL
        - We are going to run some tests with the current local repo's
    """
    #Entering parent directory
    parser = argparse.ArgumentParser(description="""
    Full download of latest pubmed dataset release
""")
    
    parser.add_argument(
         "--input_dir", help="Output base directory of downloaded files"
    )
    # Getting arguments passed from
    arguments = parser.parse_args()
    input_dir = arguments.input_dir
    # dir = Path(input_dir)
    dir = Path('./output_dir')
    for item in dir.iterdir():
        print(item)

if __name__ == '__main__':
    main()
