#!/bin/bash
#SBATCH -N1 # Run this across 1 node
#SBATCH -c3 # Request cpus
#SBATCH --mem=10gb
#SBATCH --partition aind
#SBATCH --exclude=n69,n74
#SBATCH --tmp=64MB
#SBATCH --time=10:00:00
#SBATCH --output=/allen/aind/scratch/carson.berry/hpc_outputs/%j_zarr_upload.log
#SBATCH --mail-type=ALL
#SBATCH --mail-user=carson.berry@alleninstitute.org

set -e

pwd; date
[[ -f "/allen/programs/mindscope/workgroups/omfish/carsonb/miniconda/bin/activate" ]] && source "/allen/programs/mindscope/workgroups/omfish/carsonb/miniconda/bin/activate" adt-upload-clone

python -m aind_data_transfer.scripts.ispim_chron_job --config_file /allen/aind/scratch/carson.berry/aind-data-transfer/scritps/chron_job_configs.yml

echo "Done"
date