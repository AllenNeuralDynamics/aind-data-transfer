import argparse
import logging
import os
import time
from pathlib import PurePath

import numpy as np
from cluster.config import load_jobqueue_config
from dask_jobqueue import SLURMCluster
from distributed import Client
from s3transfer.constants import GB, MB

from transfer.s3 import S3Uploader

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


def get_client():
    config = load_jobqueue_config()
    slurm_config = config["jobqueue"]["slurm"]
    # cluster config is automatically populated from ~/.config/dask/jobqueue.yaml
    cluster = SLURMCluster()
    cluster.scale(slurm_config["n_workers"])
    logger.info(cluster.job_script())
    client = Client(cluster)
    return client, slurm_config


def run_cluster_job(
    input_dir,
    bucket,
    s3_path,
    target_throughput,
    part_size,
    timeout,
    parallelism,
):
    client, config = get_client()
    ntasks = config["n_workers"]
    cores = config["cores"]

    chunked_files = chunk_files(input_dir, ntasks * parallelism)
    logger.info(
        f"Split files into {len(chunked_files)} chunks with {len(chunked_files[0])} files each"
    )

    futures = []
    for chunk in chunked_files:
        futures.append(
            client.submit(
                upload_files_job,
                files=chunk,
                bucket=bucket,
                s3_path=s3_path,
                n_threads=cores,
                target_throughput=target_throughput,
                part_size=part_size,
                timeout=timeout,
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
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-i",
        "--input",
        type=str,
        help="folder or file to upload",
    )
    parser.add_argument(
        "-b",
        "--bucket",
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
        "--cluster",
        default=False,
        action="store_true",
        help="run in cluster mode",
    )
    parser.add_argument(
        "--batch_num",
        type=int,
        default=3,
        help="number of tasks per job. Increase this if you run into worker memory issues",
    )
    parser.add_argument(
        "--nthreads",
        type=int,
        default=1,
        help="num threads to use if running locally",
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
    if args.cluster:
        run_cluster_job(
            input_dir=input_path,
            bucket=bucket,
            s3_path=s3_path,
            target_throughput=args.target_throughput,
            part_size=args.part_size,
            timeout=args.timeout,
            parallelism=args.batch_num,
        )
    else:
        run_local_job(
            input_dir=input_path,
            bucket=bucket,
            s3_path=s3_path,
            nthreads=args.nthreads,
            target_throughput=args.target_throughput,
            part_size=args.part_size,
            timeout=args.timeout,
        )

    logger.info(f"Upload done. Took {time.time() - t0}s ")


if __name__ == "__main__":
    main()
