#!/bin/bash

# Initialize Conda for Bash
source ~/miniconda3/etc/profile.d/conda.sh

# Activate the Conda environment
conda activate intro-sos

# Execute the Python script
python create_remote_manifest.py --input_dir './output_dir'

# Deactivate the Conda environment
conda deactivate

