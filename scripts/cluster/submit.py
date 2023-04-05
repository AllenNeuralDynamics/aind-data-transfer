"""
Submit jobs to a cluster.
Adapted from from https://github.com/JaneliaSciComp/spark-janelia
"""

import argparse
import collections
import datetime
import os
import shutil
import string
import time
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
        conda_activate=args.conda_activate,
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
        dask_conf_file=run_info.dask_conf_file
    )
    os.chmod(script_path, 0o755)


def copy_dask_config(run_info):
    conf_file = Path(__file__).parent.parent.parent / "conf/dask/dask-config.yaml"
    assert conf_file.is_file()
    # Copy to the run directory.
    # The DASK_CONFIG environment variable is set to this path
    shutil.copyfile(conf_file, run_info.dask_conf_file)


RunInfo = collections.namedtuple(
    "RunInfo",
    "dir, logs_dir, scripts_dir, launch_script, "
    "job_name_prefix, dask_conf_file",
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
    run_conf_dir = f"{run_dir}/conf"
    launch_script = f"{run_scripts_dir}/queue-slurm-jobs.sh"

    dask_conf_file = f"{run_conf_dir}/dask-config.yaml"

    user_name = os.getenv("USER")
    job_name_prefix = f"slurm_{user_name}_{timestamp}"

    os.makedirs(run_logs_dir)
    os.makedirs(run_scripts_dir)
    os.makedirs(run_conf_dir)

    print(f"\nCreated:\n {run_logs_dir}\n  {run_scripts_dir}\n")

    return RunInfo(
        run_dir,
        run_logs_dir,
        run_scripts_dir,
        launch_script,
        job_name_prefix,
        dask_conf_file,
    )


def parse_args():
    home_dir = os.getenv("HOME")

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "task",
        choices=["generate-run", "generate-and-launch-run"],
        help="use generate-run if you want to review scripts before launching",
    )
    parser.add_argument(
        "-j",
        "--job_cmd",
        type=str,
        required=True,
        help="the job command to submit to the scheduler",
    )
    parser.add_argument(
        "--run_parent_dir",
        type=str,
        default=f"{home_dir}/.slurm",
        help="directory to store all logs, scripts, "
        "and config files for a run",
    )
    parser.add_argument(
        "--conda_activate",
        type=str,
        help="path to conda env activation script",
    )
    parser.add_argument(
        "--conda_env", type=str, help="name of conda environment to activate"
    )
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
        help="number of cpus per job",
    )
    parser.add_argument(
        "--mem_per_cpu", type=int, default=500, help="memory per cpu in MB"
    )
    parser.add_argument(
        "--tmp_space",
        type=str,
        default="512MB",
        help="amount of space on node-local storage for "
        "Dask worker spilling, e.g., 4GB",
    )
    parser.add_argument(
        "--walltime", type=str, default="00:10:00", help="job walltime"
    )
    parser.add_argument(
        "--queue", type=str, default="aind", help="queue for scheduler"
    )
    parser.add_argument(
        "-m",
        "--mail_user",
        type=str,
        required=True,
        help="notify user of job status",
    )
    parser.add_argument(
        "--deployment",
        type=str,
        choices=["slurm"],
        default="slurm",
        help="which cluster manager to use",
    )
    parser.add_argument(
        "--local_dir",
        type=str,
        default="/scratch/fast/$SLURM_JOB_ID",
        help="local directory for dask worker file spilling. "
        "This should be fast, node-local storage.",
    )
    parser.add_argument(
        "--wait",
        default=False,
        action="store_true",
        help="do not exit until the submitted job terminates. See the --wait flag for sbatch.",
    )

    args = parser.parse_args()

    return args


def main():
    args = parse_args()

    run_info = create_run(args)

    if args.deployment == "slurm":
        write_slurm_scripts(args, run_info)
    else:
        raise ValueError("Only Slurm is currently supported")

    copy_dask_config(run_info)

    if args.task == "generate-and-launch-run":
        cmd = f"sbatch "
        if args.wait:
            cmd += "--wait "
        cmd += run_info.launch_script
        print(f"Running:\n  {cmd}\n")
        os.system(cmd)
    else:
        print(f"To launch jobs, run:\n  {run_info.launch_script}\n")


if __name__ == "__main__":
    main()
