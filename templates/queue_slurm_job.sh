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

echo "Starting the dask scheduler on node ${HOSTNAME}"
echo "Access the dashboard with \"ssh -L 8787:${HOSTNAME}:8787 ${USER}@@hpc-login\""

[[ -f "@{conda_activate}" ]] && source "@{conda_activate}" @{conda_env}

echo "Running \"@{job_cmd}\""

@{job_cmd}

echo "Done"

date
