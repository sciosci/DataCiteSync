#!/bin/bash

# Initialize Conda for Bash
source ~/miniconda3/etc/profile.d/conda.sh

# Activate the Conda environment
conda activate intro-sos

# Execute the Python script
python sync_pubmed.py --o '/home/dimu6211/pl/oa_package'  --ftp_host 'ftp.ncbi.nlm.nih.gov'  --starting_pubmed_dir '/pub/pmc/oa_package'

# Deactivate the Conda environment
conda deactivate

