#!/bin/bash

#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2# Request cpus
#SBATCH --mem=10gb
#SBATCH --partition aind
#SBATCH --exclude=n69,n74
#SBATCH --tmp=64MB
#SBATCH --time=10:00:00
#SBATCH --output=/allen/aind/scratch/carson.berry/hpc_outputs/%j_zarr_upload.log
#SBATCH --mail-type=ALL
#SBATCH --mail-user=carson.berry@alleninstitute.org

set -e
echo "Starting"
pwd; date
[[ -f "/allen/programs/mindscope/workgroups/omfish/carsonb/miniconda/bin/activate" ]] && source "/allen/programs/mindscope/workgroups/omfish/carsonb/miniconda/bin/activate" adt-upload-clone

python /allen/aind/scratch/carson.berry/aind-data-transfer/scripts/ispim_cron_job.py --config_file /allen/aind/scratch/carson.berry/aind-data-transfer/scripts/chron_job_configs.yml

echo "Done"
date
