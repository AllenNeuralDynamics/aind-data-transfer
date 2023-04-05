#!/bin/bash

# ----------------------------------------------------------------------------
# This script submits a job to the SLURM cluster scheduler.
#
# The template populated parameters are:
#   conda_activate        @{conda_activate}
#   conda_env             @{conda_env}
#   job_cmd:              @{job_cmd}
#   job_log_dir:          @{job_log_dir}
#   mail_user:            @{mail_user}
#   walltime:             @{walltime}
#   partition:            @{partition}
#   nodes:                @{nodes}
#   ntasks_per_node:      @{ntasks_per_node}
#   cpus_per_task:        @{cpus_per_task}
#   mem_per_cpu:          @{mem_per_cpu}
#   tmp_space:            @{tmp_space}
#   dask_conf_file        @{dask_conf_file}
# ----------------------------------------------------------------------------

#SBATCH --output=@{job_log_dir}/output.log
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --mail-user=@{mail_user}
#SBATCH --partition=@{partition}
#SBATCH --mem-per-cpu=@{mem_per_cpu}
#SBATCH --time=@{walltime}
#SBATCH --nodes=@{nodes}
#SBATCH --ntasks-per-node=@{ntasks_per_node}
#SBATCH --cpus-per-task=@{cpus_per_task}
#SBATCH --tmp=@{tmp_space}

set -e

pwd; date

[[ -f "@{conda_activate}" ]] && source "@{conda_activate}" @{conda_env}

module purge
module load mpi/mpich-3.2-x86_64

export DASK_CONFIG=@{dask_conf_file}

echo "Running \"@{job_cmd}\""

# Add 2 processes more than we have tasks, so that rank 0 (coordinator) and 1 (serial process)
# are not sitting idle while the workers (rank 2...N) work
# See https://edbennett.github.io/high-performance-python/11-dask/ for details.
mpiexec -np $(( SLURM_NTASKS + 2 )) @{job_cmd}

echo "Done"

date
