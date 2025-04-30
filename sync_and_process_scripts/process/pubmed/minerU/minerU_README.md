# MinerU
### If there are no GPUs available
MinerU can be run with CPU only, there is a key inthe magic-pdf.json
"device-type":"cuda" that needs to be switched to "cpu" before
it will recognize runs as cpu only. If this is not switched then script will error out. 
**Runtime**:
MinerU with CPU only took an average of 350 seconds per file ON 
SLURM while using 2 nodes MPI, 8 tasks per node and 128 GBs of CPU memory.
Why I did not increase the number of nodes with MPI higher?
Because % Utilization on CPU only was at most 10%,
which never made sense to me because if I would decrease the # of processes,
then time for minerU to process each PDF file would stay the same, while CPU
utilization would still stay below 10%.

### If there are GPUs available.
Why do I request only 1 GPU and one node at a time with minerU?
When I would use multiple GPUs, and multiple nodes, queue time would
regularly exceed 24 hours, due to the low amount of GPU specific resources. 
So my work around for this, is submitting multiple scripts, each requesting 1 GPU and 
1 node. So there wait time will be less than a couple hours at most, and if there is low
demand and multiple GPUs are available, we will still get the benefit of multiple GPU
processes. 

### Is there any issues with file status tracking if there are multiple scripts running?
I changed the manifest from being a single sqlite3 file, to being pandas dataframes stored as parquets within
every second level directory. 
Example: 01/02/01_02_manifest.parquet, will store the information for articles within 01/02 folder. 
