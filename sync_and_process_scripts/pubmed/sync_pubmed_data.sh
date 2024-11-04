#!/bin/bash

# Initialize Conda for Bash
source ~/miniconda3/etc/profile.d/conda.sh

# Activate the Conda environment
conda activate intro-sos

# Execute the Python script
python get_pubmed.py --o './output_dir/data'

# Deactivate the Conda environment
conda deactivate

