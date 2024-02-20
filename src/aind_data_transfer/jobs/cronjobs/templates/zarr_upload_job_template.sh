#!/bin/bash

set -e

pwd; date

source ~/.bash_profile
source /etc/profile.d/modules.sh

module purge
module load mpi/mpich-3.2-x86_64

mpiexec -np $(( SLURM_NTASKS + 2 )) singularity exec {sif_path} python -m aind_data_transfer.jobs.zarr_upload_job --json-args '{json_args}'

echo "Done"
date
