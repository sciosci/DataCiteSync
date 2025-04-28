#!/bin/bash

#SBATCH --partition=atesting_a100
#SBATCH --nodes=1
#SBATCH --ntasks=8
#SBATCH --gres=gpu:1
#SBATCH --time=1:00:00
#SBATCH --output=mineru_test.%j.out
#SBATCH --mail-type=ALL
#SBATCH --mail-user=<email>

module purge
module load anaconda/2023.09


conda activate <path_to_conda_minerU__env> 

export <path_to_minerU_config.json>

python <absolute_path_to_python_minerU_script>
