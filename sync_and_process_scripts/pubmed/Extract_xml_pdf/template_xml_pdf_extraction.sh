#!/bin/bash

#SBATCH --partition=<partition_name>
#SBATCH --nodes=<nodes>
#SBATCH --ntasks-per-node=<tasks_per_node> # MPI ranks per node
#SBATCH --cpus-per-task=<cpus>               # cores reserved per rank (for ThreadPool)
#SBATCH --time=<time>
#SBATCH --output=<name_of_logger_file>
#SBATCH --mail-type=<type_of_main, all, fail, none>
#SBATCH --constraint=<infiband_constraint_for_mpi>
#SBATCH --mail-user=<email>

# Initialize Conda for Bash
module purge
module load anaconda/2023.09


conda activate <path_to_conda_env> 
module load gcc/14.2.0
module load openmpi/5.0.6

mpirun -np <np_number>  python <path_to_py_file>  --input_dir <input_dir> --output_dir <output_dir> 