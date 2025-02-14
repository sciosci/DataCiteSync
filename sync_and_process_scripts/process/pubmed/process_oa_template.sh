#!/bin/bash

# Initialize Conda for Bash
source ~/miniconda3/etc/profile.d/conda.sh

# Activate the Conda environment
conda activate docling_env

# Execute the Python script
python process_oa_template.py  --output_dir <output_dir> --input_dir <input_dir>

# Deactivate the Conda environment
conda deactivate
