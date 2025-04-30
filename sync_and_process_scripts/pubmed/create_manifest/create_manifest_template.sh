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


conda activate <path_to_conda_env> 

# Execute the Python script
python create_remote_manifest.py --input_dir  <input_directory>

# Deactivate the Conda environment
conda deactivate