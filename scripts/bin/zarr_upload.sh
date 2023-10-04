#!/bin/bash

#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=8000
#SBATCH --exclude=n69,n74
#SBATCH --tmp=64MB
#SBATCH --time=01:00:00
#SBATCH --partition=aind
#SBATCH --output=/allen/programs/mindscope/workgroups/omfish/carsonb/hpc_outputs/%j_zarr_test.log
#SBATCH --mail-type=ALL
#SBATCH --mail-user=carson.berry@alleninstitute.org
#SBATCH --ntasks=64
#SBATCH --nodelist=n111

set -e

pwd; date
[[ -f "/allen/programs/mindscope/workgroups/omfish/carsonb/miniconda/bin/activate" ]] && source "/allen/programs/mindscope/workgroups/omfish/carsonb/miniconda/bin/activate" adt-upload-clone

module purge
module load mpi/mpich-3.2-x86_64

# Add 2 processes more than we have tasks, so that rank 0 (coordinator) and 1 (serial process)
# are not sitting idle while the workers (rank 2...N) work
# See https://edbennett.github.io/high-performance-python/11-dask/ for details.
mpiexec -np $(( SLURM_NTASKS + 2 )) python -m aind_data_transfer.jobs.zarr_upload_job --json-args '{"s3_bucket": "aind-open-data", "experiment_type": "diSPIM", "modalities":[{"modality": "DISPIM","source":"/allen/programs/mindscope/workgroups/omfish/mfish/temp_raw/diSPIM_667829-ID_2023-08-25_14-16-51, "extra_configs": "/allen/programs/mindscope/workgroups/omfish/mfish/temp_raw/zarr_config.yml"}], "subject_id": "667829", "acq_date": "2023-08-25", "acq_time": "14-16-51", "force_cloud_sync": "true", "codeocean_domain": "https://codeocean.allenneuraldynamics.org", "metadata_service_domain": "http://aind-metadata-service", "aind_data_transfer_repo_location": "https://github.com/AllenNeuralDynamics/aind-data-transfer", "log_level": "INFO"}'

echo "Done"

date