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

python sync_pubmed.py --o <destination_dir>  --ftp_host 'ftp.ncbi.nlm.nih.gov'  --starting_pubmed_dir '/pub/pmc/oa_package'

# Deactivate the Conda environment
conda deactivate

