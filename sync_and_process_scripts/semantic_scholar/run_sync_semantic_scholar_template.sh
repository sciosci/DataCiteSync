#!/bin/bash

# initialize conda [source https://github.com/conda/conda/issues/7980]
source ~/miniconda3/etc/profile.d/conda.sh
# activate virtual environment
conda activate data-sync-int
# Execute script
python sync_semantic_scholar_from_scratch.py --key 'FcuPcoxxWC3ePxBABTLvkyWxqvt7v9h32sDBO4ug' -o "./dictionaryOutputTest" 
# Deactivate virtual environment
conda deactivate

