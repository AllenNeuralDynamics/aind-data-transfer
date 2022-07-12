"""Submit jobs to a cluster. Adapted from from https://github.com/JaneliaSciComp/spark-janelia"""

import argparse
import collections
import os
import string
import time
import datetime
from pathlib import Path


class SlurmTemplate(string.Template):
    delimiter = "@"
    idpattern = r"[a-z][_a-z0-9]*"


def render_template(template_path, to_path, **kwargs):
    with open(template_path, "r") as template_file:
        template_contents = template_file.read()

    template = SlurmTemplate(template_contents)
    populated_template = template.substitute(kwargs)

    with open(to_path, "w") as file:
        print(populated_template, file=file)

    return to_path


def write_slurm_scripts(args, run_info):
    template_path = os.path.join(
        Path(os.path.dirname(__file__)).parent.parent.absolute(),
        "templates/queue_slurm_job.sh",
    )
    script_path = render_template(
        template_path,
        run_info.launch_script,
        conda_env=args.conda_env,
        job_cmd=args.job_cmd,
        mail_user=args.mail_user,
        job_log_dir=run_info.logs_dir,
        walltime=args.walltime,
        partition=args.queue,
        nodes=args.nodes,
        ntasks_per_node=args.ntasks_per_node,
        cpus_per_task=args.cpus_per_task,
        mem_per_cpu=args.mem_per_cpu,
        tmp_space=args.tmp_space,
    )
    os.chmod(script_path, 0o755)


RunInfo = collections.namedtuple(
    "RunInfo", "dir, logs_dir, scripts_dir, launch_script, job_name_prefix"
)


def create_run(args):

    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = f"{args.run_parent_dir}/{timestamp}"

    try:
        os.makedirs(run_dir)
    except FileExistsError:
        # retry once to work around rare case of concurrent run by same user
        time.sleep(1)
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        run_dir = f"{args.run_parent_dir}/{timestamp}"

    run_logs_dir = f"{run_dir}/logs"
    run_scripts_dir = f"{run_dir}/scripts"
    launch_script = f"{run_scripts_dir}/queue-slurm-jobs.sh"

    user_name = os.getenv("USER")
    job_name_prefix = f"slurm_{user_name}_{timestamp}"

    os.makedirs(run_logs_dir)
    os.makedirs(run_scripts_dir)

    print(f"\nCreated:\n {run_logs_dir}\n  {run_scripts_dir}\n")

    return RunInfo(
        run_dir, run_logs_dir, run_scripts_dir, launch_script, job_name_prefix
    )


def main():
    home_dir = os.getenv("HOME")

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--task",
        choices=["generate-run", "generate-and-launch-run"],
        help="use generate-run if you want to review scripts before launching",
    )
    parser.add_argument(
        "--run_parent_dir", type=str, default=f"{home_dir}/.slurm"
    )
    parser.add_argument(
        "--conda_env", type=str, help="path to conda environment"
    )
    parser.add_argument(
        "--job_cmd",
        type=str,
        required=True,
        help="the job command to submit to the scheduler",
    )
    parser.add_argument("--queue", type=str, default="aind")
    parser.add_argument(
        "--ntasks_per_node",
        type=int,
        default=1,
        help="number of tasks per node",
    )
    parser.add_argument(
        "--nodes", type=int, default=1, help="number of HPC nodes"
    )
    parser.add_argument(
        "--cpus_per_task",
        type=int,
        default=1,
        help="num threads to use for upload",
    )
    parser.add_argument(
        "--mem_per_cpu", type=int, default=4000, help="memory per job in MB"
    )
    parser.add_argument(
        "--tmp_space",
        type=str,
        default="500MB",
        help="amount of space on node-local storage for Dask worker spilling",
    )
    parser.add_argument(
        "--walltime", type=str, default="01:00:00", help="SLURM job walltime"
    )
    parser.add_argument(
        "--mail_user", type=str, help="notify user of job status"
    )

    args = parser.parse_args()

    run_info = create_run(args)
    write_slurm_scripts(args, run_info)

    if args.task == "generate-and-launch-run":
        print(f"Running:\n  {run_info.launch_script}\n")
        os.system(f"sbatch {run_info.launch_script}")
    else:
        print(f"To launch jobs, run:\n  {run_info.launch_script}\n")


if __name__ == "__main__":
    main()
