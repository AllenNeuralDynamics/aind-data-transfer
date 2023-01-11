import argparse
import itertools
import json
import logging
import os
import time
from datetime import datetime
from pathlib import PurePath

import numpy as np
from s3transfer.constants import GB, MB
from aind_codeocean_api.codeocean import CodeOceanClient
from aind_codeocean_api.credentials import CodeOceanCredentials

from aind_data_transfer.s3 import S3Uploader
from aind_data_transfer.util import file_utils
from aind_data_transfer.util.file_utils import collect_filepaths
from aind_data_transfer.util.dask_utils import log_dashboard_address, get_client

LOG_FMT = "%(asctime)s %(message)s"
LOG_DATE_FMT = "%Y-%m-%d %H:%M"

logging.basicConfig(format=LOG_FMT, datefmt=LOG_DATE_FMT)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def chunk_files(input_dir, ntasks, recursive=True, exclude_dirs=None):
    filepaths = collect_filepaths(input_dir, recursive, exclude_dirs)
    logger.info(f"Collected {len(filepaths)} files")
    return np.array_split(filepaths, ntasks)


def upload_files_job(
    input_dir,
    files,
    bucket,
    s3_path,
    n_threads,
    target_throughput,
    part_size,
    timeout,
):
    uploader = S3Uploader(
        num_threads=n_threads,
        target_throughput=target_throughput,
        part_size=part_size,
        upload_timeout=timeout,
    )
    return uploader.upload_files(files, bucket, s3_path, root=input_dir)


def run_cluster_job(
    input_dir,
    bucket,
    s3_path,
    target_throughput,
    part_size,
    timeout,
    chunks_per_worker,
    recursive,
    exclude_dirs=None,
):
    client, ntasks = get_client(deployment="slurm")
    logger.info(f"Client has {ntasks} registered workers")

    chunked_files = chunk_files(input_dir, ntasks * chunks_per_worker, recursive, exclude_dirs)
    logger.info(
        f"Split files into {len(chunked_files)} chunks with "
        f"{len(chunked_files[0])} files each"
    )

    futures = []
    for chunk in chunked_files:
        futures.append(
            client.submit(
                upload_files_job,
                input_dir=input_dir,
                files=chunk,
                bucket=bucket,
                s3_path=s3_path,
                n_threads=1,
                target_throughput=target_throughput,
                part_size=part_size,
                timeout=timeout,
            )
        )
    failed_uploads = list(itertools.chain(*client.gather(futures)))
    n_failed_uploads = len(failed_uploads)
    logger.info(f"{n_failed_uploads} failed uploads:\n{failed_uploads}")
    client.close()

    return n_failed_uploads


def run_local_job(
    input_dir,
    bucket,
    s3_path,
    nthreads,
    target_throughput,
    part_size,
    timeout,
    recursive,
    exclude_dirs=None,
):
    uploader = S3Uploader(
        num_threads=nthreads,
        target_throughput=target_throughput,
        part_size=part_size,
        upload_timeout=timeout,
    )
    if os.path.isdir(input_dir):
        failed_uploads = uploader.upload_folder(
            input_dir, bucket, s3_path, recursive, exclude_dirs
        )
    elif os.path.isfile(input_dir):
        failed_uploads = uploader.upload_file(input_dir, bucket, s3_path)
    else:
        raise ValueError(
            f"Invalid value for --input: {input_dir} does not exist"
        )

    n_failed_uploads = len(failed_uploads)
    logger.info(f"{n_failed_uploads} failed uploads:\n{failed_uploads}")

    return n_failed_uploads


# Check dataset status
STATUS = ["PENDING", "UPLOADED"]
STATUS_FILENAME = "DATASET_STATUS.txt"


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
        help="number of tasks per job. "
        "Increase this if you run into worker memory issues",
    )
    parser.add_argument(
        "--nthreads",
        type=int,
        default=1,
        help="num threads to use if running locally",
    )
    parser.add_argument(
        "--recursive",
        default=False,
        action="store_true",
        help="upload a directory recursively",
    )
    parser.add_argument(
        "--exclude_dirs",
        type=str,
        nargs="+",
        default=None,
        help="directories to exclude from upload",
    )
    parser.add_argument(
        "--trigger_code_ocean",
        default=False,
        action="store_true",
        help="upload a directory recursively",
    )
    parser.add_argument(
        "--capsule_id",
        type=str,
        default="",
        help="Capsule ID to execute dataset once is uploaded",
    )

    args = parser.parse_args()

    input_path = args.input
    bucket = args.bucket
    n_failed_uploads = -1
    trigger_code_ocean = args.trigger_code_ocean
    capsule_id = args.capsule_id

    s3_path = args.s3_path
    if s3_path is None:
        s3_path = PurePath(args.input).name
    # remove leading slash since it will result in the structure
    # "bucket/'/'/s3_path" instead of "bucket/s3_path"
    s3_path = s3_path.strip("/")
    logger.info(f"Will upload to {args.bucket}/{s3_path}")

    t0 = time.time()
    if args.cluster:
        n_failed_uploads = run_cluster_job(
            input_dir=input_path,
            bucket=bucket,
            s3_path=s3_path,
            target_throughput=args.target_throughput,
            part_size=args.part_size,
            timeout=args.timeout,
            chunks_per_worker=args.batch_num,
            recursive=args.recursive,
            exclude_dirs=args.exclude_dirs,
        )
    else:
        n_failed_uploads = run_local_job(
            input_dir=input_path,
            bucket=bucket,
            s3_path=s3_path,
            nthreads=args.nthreads,
            target_throughput=args.target_throughput,
            part_size=args.part_size,
            timeout=args.timeout,
            recursive=args.recursive,
            exclude_dirs=args.exclude_dirs,
        )

    logger.info(f"Upload done. Took {time.time() - t0}s ")

    if not n_failed_uploads:
        now_datetime = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        file_utils.write_list_to_txt(
            str(PurePath(input_path).joinpath(STATUS_FILENAME)),
            ["UPLOADED", f"Upload time: {now_datetime}", f"Bucket: {bucket}"],
        )

    if trigger_code_ocean and not n_failed_uploads and len(capsule_id):

        asset_name = PurePath(s3_path).stem

        job_configs = {
            "trigger_codeocean_job": {
                "job_type": "smartspim",
                "bucket": bucket,
                "prefix": asset_name,
                "registration": {
                    "channel": "Ex_488_Em_525.zarr",
                    "input_scale": "3",
                },
            }
        }

        try:
            co_cred = CodeOceanCredentials().credentials

            co_api = CodeOceanClient(
                domain=co_cred["domain"], token=co_cred["token"]
            )

            run_response = co_api.run_capsule(
                capsule_id=capsule_id,
                data_assets=[],
                parameters=[json.dumps(job_configs)],
            )
            logger.info(f"Run response: {run_response.json()}")

        except ValueError as err:
            logger.error(f"Error communicating with Code Ocean API {err}")


if __name__ == "__main__":
    main()
