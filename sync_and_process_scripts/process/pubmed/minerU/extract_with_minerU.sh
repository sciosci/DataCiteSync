#!/bin/bash

#SBATCH --partition=<partition_name>
#SBATCH --nodes=<nodes
#SBATCH --ntasks=<tasks>
#SBATCH --gres=<gpus>
#SBATCH --time=<time>
#SBATCH --output=<time>
#SBATCH --mail-type=ALL
#SBATCH --mail-user=<email>

module purge
module load anaconda/2023.09


conda activate <path_to_conda_minerU__env> 

# this helpes my minerU script run on SLURM 
export <path_to_minerU_config.json>

python <absolute_path_to_python_minerU_script>
