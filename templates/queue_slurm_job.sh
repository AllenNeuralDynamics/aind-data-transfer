#!/bin/bash

# ----------------------------------------------------------------------------
# This script submits a job to the SLURM cluster scheduler.
#
# The template populated parameters are:
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

pwd; hostname; date

# FIXME
ANACONDA=/allen/programs/aind/workgroups/msma/cameron.arshadi/miniconda3
CONDA_ENV=@{conda_env}

source "${ANACONDA}/bin/activate" $CONDA_ENV

echo "Running \"@{job_cmd}\""

# FIXME The SBATCH parameters need to also be passed to the Dask SLURMCluster instance
@{job_cmd} \
--partition=@{partition} \
--nodes=@{nodes} \
--ntasks_per_node=@{ntasks_per_node} \
--cpus_per_task=@{cpus_per_task} \
--mem_per_cpu=@{mem_per_cpu} \
--walltime=@{walltime} \
--local_dir=/scratch/fast/$SLURM_JOB_ID \
--log_dir=@{job_log_dir}

echo "Done"

date
