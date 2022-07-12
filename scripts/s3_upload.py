import argparse
import logging
import os
import time
from pathlib import PurePath

import numpy as np
from distributed import Client
from s3transfer.constants import MB, GB
from transfer.s3 import S3Uploader

from cluster.clusters import get_slurm_cluster

LOG_FMT = "%(asctime)s %(message)s"
LOG_DATE_FMT = "%Y-%m-%d %H:%M"

logging.basicConfig(format=LOG_FMT, datefmt=LOG_DATE_FMT)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def chunk_files(input_dir, ntasks):
    filepaths = [
        os.path.join(input_dir, f)
        for f in os.listdir(input_dir)
        if os.path.isfile(os.path.join(input_dir, f))
    ]
    return np.array_split(filepaths, ntasks)


def upload_files_job(
    files, bucket, s3_path, n_threads, target_throughput, part_size, timeout
):
    uploader = S3Uploader(
        num_threads=n_threads,
        target_throughput=target_throughput,
        part_size=part_size,
        upload_timeout=timeout,
    )
    uploader.upload_files(files, bucket, s3_path)


def run_cluster_job(
    input_dir,
    bucket,
    s3_path,
    ntasks,
    cpus_per_task,
    mem_per_task,
    queue,
    walltime,
    target_throughput,
    part_size,
    timeout,
    **kwargs,
):
    chunked_files = chunk_files(input_dir, ntasks * 3)
    logger.info(f"Split files into {len(chunked_files)} chunks")
    cluster = get_slurm_cluster(
        n_workers=ntasks,
        cores=cpus_per_task,
        processes=1,
        memory=mem_per_task,
        queue=queue,
        walltime=walltime,
        **kwargs,
    )
    logger.info(cluster.job_script())
    client = Client(cluster)
    futures = []
    for chunk in chunked_files:
        futures.append(
            client.submit(
                upload_files_job,
                chunk,
                bucket,
                s3_path,
                cpus_per_task,
                target_throughput,
                part_size,
                timeout,
            )
        )
    client.gather(futures)
    client.close()


def run_local_job(
    input_dir, bucket, s3_path, nthreads, target_throughput, part_size, timeout
):
    uploader = S3Uploader(
        num_threads=nthreads,
        target_throughput=target_throughput,
        part_size=part_size,
        upload_timeout=timeout,
    )
    if os.path.isdir(input_dir):
        uploader.upload_folder(input_dir, bucket, s3_path)
    elif os.path.isfile(input_dir):
        uploader.upload_file(input_dir, bucket, s3_path)
    else:
        raise ValueError(
            f"Invalid value for --input: {input_dir} does not exist"
        )


def main():
    home_dir = os.getenv("HOME")
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-i",
        "--input",
        default=r"/allen/scratch/aindtemp/cameron.arshadi/test-file-res0-12stack.zarr",
        type=str,
        help="folder to upload",
    )
    parser.add_argument(
        "-b",
        "--bucket",
        default="aind-transfer-test",
        type=str,
        help="s3 bucket",
    )
    parser.add_argument(
        "--s3_path",
        type=str,
        default=None,
        help='s3 path relative to bucket, e.g., "folder/data.h5"',
    )
    parser.add_argument(
        "--target_throughput",
        type=float,
        default=100 * GB / 8,
        help="target throughput (bytes)",
    )
    parser.add_argument(
        "--part_size",
        type=int,
        default=64 * MB,
        help="part size for s3 multipart uploads (bytes)",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=None,
        help="per-file upload timeout (s). Default is None.",
    )

    parser.add_argument(
        "--slurm",
        default=False,
        action="store_true",
        help="run on SLURM cluster",
    )

    parser.add_argument(
        "--local_dir",
        type=str,
        default=f"{home_dir}/.dask_worker",
        help="fast storage for Dask worker file spilling",
    )
    parser.add_argument(
        "--log_dir",
        type=str,
        default=f"{home_dir}/.dask_distributed-test",
        help="log directory for dask workers",
    )

    # Slurm parameters are populated automatically with the job template
    parser.add_argument("--partition", type=str, default="aind")
    parser.add_argument(
        "--ntasks_per_node",
        type=int,
        default=1,
        help="number of tasks to split the work",
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
        "--walltime", type=str, default="01:00:00", help="SLURM job walltime"
    )

    args = parser.parse_args()

    input_path = args.input
    bucket = args.bucket

    s3_path = args.s3_path
    if s3_path is None:
        s3_path = PurePath(args.input).name
    # remove leading slash since it will result in the structure "bucket/'/'/s3_path" instead of "bucket/s3_path"
    s3_path = s3_path.strip("/")
    logger.info(f"Will upload to {args.bucket}/{s3_path}")

    t0 = time.time()
    if args.slurm:
        my_slurm_kwargs = {}

        os.makedirs(args.local_dir, exist_ok=True)
        my_slurm_kwargs["local_directory"] = args.local_dir

        log_dir = os.path.join(args.log_dir, "dask-worker-logs")
        os.makedirs(log_dir, exist_ok=True)
        my_slurm_kwargs["log_directory"] = log_dir

        logger.info(my_slurm_kwargs)

        run_cluster_job(
            input_dir=input_path,
            bucket=bucket,
            s3_path=s3_path,
            ntasks=args.ntasks_per_node * args.nodes,
            cpus_per_task=args.cpus_per_task,
            mem_per_task=f"{args.mem_per_cpu * args.cpus_per_task}MB",
            queue=args.partition,
            walltime=args.walltime,
            target_throughput=args.target_throughput,
            part_size=args.part_size,
            timeout=args.timeout,
            **my_slurm_kwargs,
        )
    else:
        run_local_job(
            input_dir=input_path,
            bucket=bucket,
            s3_path=s3_path,
            nthreads=args.cpus_per_task,
            target_throughput=args.target_throughput,
            part_size=args.part_size,
            timeout=args.timeout,
        )

    logger.info(f"Upload done. Took {time.time() - t0}s ")


if __name__ == "__main__":
    main()
