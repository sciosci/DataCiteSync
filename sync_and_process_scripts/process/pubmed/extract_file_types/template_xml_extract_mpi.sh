#!/bin/bash

#SBATCH --partition=<partition_name>
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=8             # MPI ranks per node
#SBATCH --cpus-per-task=2               # cores reserved per rank (for ThreadPool)
#SBATCH --time=20:00:00
#SBATCH --output=xml_pdf_extraction.%j.out
#SBATCH --mail-type=ALL
#SBATCH --constraint=ib
#SBATCH --mail-user=<email>

# Initialize Conda for Bash

module purge
module load anaconda/2023.09


conda activate <path_to_conda_env> 
module load gcc/14.2.0
module load openmpi/5.0.6

mpirun -np 16  python <path_to_py_file>  --input_dir <input_dir> --output_dir <output_dir> 
# close conda enviornment