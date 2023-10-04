import argparse
import itertools
import json
import logging
import os
import time
from datetime import datetime
from pathlib import PurePath

from aind_codeocean_api.codeocean import CodeOceanClient
from aind_codeocean_api.credentials import CodeOceanCredentials
from s3transfer.constants import GB, MB

from aind_data_transfer.s3 import S3Uploader
from aind_data_transfer.util import file_utils
from aind_data_transfer.util.dask_utils import get_client
from aind_data_transfer.util.file_utils import collect_filepaths, batch_files_by_size


LOG_FMT = "%(asctime)s %(message)s"
LOG_DATE_FMT = "%Y-%m-%d %H:%M"

logging.basicConfig(format=LOG_FMT, datefmt=LOG_DATE_FMT)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


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
    batch_size=GB,
    recursive=True,
    exclude_dirs=None,
):
    client, ntasks = get_client(deployment="slurm")
    logger.info(f"Client has {ntasks} registered workers")

    futures = []
    for batch in batch_files_by_size(input_dir, batch_size, recursive, exclude_dirs=exclude_dirs):
        futures.append(
            client.submit(
                upload_files_job,
                input_dir=input_dir,
                files=batch,
                bucket=bucket,
                s3_path=s3_path,
                n_threads=1,
                target_throughput=target_throughput,
                part_size=part_size,
                timeout=timeout,
            )
        )
    if not futures:
        logger.error(f"No files found!")
        client.close()
        return -1

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


def get_smartspim_config(job_config: dict, pipeline_config: dict) -> dict:
    def get_key(job_cnf, key):
        if key in pipeline_config:
            job_cnf[key] = pipeline_config[key]

        return job_cnf

    for step in ["stitching", "registration", "segmentation"]:
        job_config = get_key(job_config, step)

    return job_config


def post_upload_smartspim(args: dict, s3_path: str, n_failed_uploads: int):
    """
    Post upload function for the smartspim
    datasets

    Parameters
    ----------
    args: dict
        Dictionary with the script
        arguments

    s3_path: str
        s3 path where the data was
        uploaded

    n_failed_uploads: int
        Number of failed uploads
    """

    if not n_failed_uploads:
        # Unpackaging args for easy code reading
        input_path = args.input
        bucket = args.bucket
        trigger_code_ocean = args.trigger_code_ocean
        pipeline_config = None

        if len(args.pipeline_config):
            pipeline_config = args.pipeline_config.replace("[token]", '"')
            pipeline_config = json.loads(pipeline_config)

        # Updating dataset_status in processing manifest
        now_datetime = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

        processing_manifest_path = "derivatives/processing_manifest.json"
        dataset_config_path = PurePath(input_path).joinpath(
            processing_manifest_path
        )
        msg = f"uploaded - Upload time: {now_datetime} - Bucket: {bucket}"

        file_utils.update_json_key(
            json_path=dataset_config_path, key="dataset_status", new_value=msg
        )

        # Triggering code ocean if necessary
        if trigger_code_ocean and pipeline_config:
            logger.info(f"Triggering code ocean {pipeline_config}")
            asset_name = PurePath(s3_path).stem

            job_configs = {
                "trigger_codeocean_job": {
                    "job_type": "smartspim",
                    "bucket": bucket,
                    "prefix": asset_name,
                }
            }

            job_configs["trigger_codeocean_job"] = get_smartspim_config(
                job_configs["trigger_codeocean_job"],
                pipeline_config["pipeline_processing"],
            )

            capsule_id = pipeline_config["co_capsule_id"]

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

        else:
            logger.warning(
                "Code ocean was not triggered. If this is an error, check your parameters"
            )

    else:
        logger.error(
            f"n failed uploads: {n_failed_uploads}. Please, validate."
        )
        logger.error(f"Skipping code ocean execution!")


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
        default=1000 * GB / 8,
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
        "--batch_size",
        type=int,
        default=256 * MB,
        help="Target total bytes for each batch of files."
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
        "--type_spim",
        type=str,
        default="",
        help="Type of SPIM dataset. e.g., SmartSPIM, ExASPIM, diSPIM...",
    )
    parser.add_argument(
        "--trigger_code_ocean",
        default=False,
        action="store_true",
        help="upload a directory recursively",
    )
    parser.add_argument(
        "--pipeline_config",
        type=str,
        default="",
        help="Configuration to pipeline in Code Ocean",
    )

    args = parser.parse_args()

    input_path = args.input
    bucket = args.bucket
    type_spim = args.type_spim.casefold()
    s3_path = args.s3_path
    if s3_path is None:
        s3_path = PurePath(args.input).name
    # remove leading slash since it will result in the structure
    # "bucket/'/'/s3_path" instead of "bucket/s3_path"
    s3_path = s3_path.strip("/")
    logger.info(f"Will upload to {args.bucket}/{s3_path}")

    t0 = time.time()

    # Uploading data
    if args.cluster:
        n_failed_uploads = run_cluster_job(
            input_dir=input_path,
            bucket=bucket,
            s3_path=s3_path,
            target_throughput=args.target_throughput,
            part_size=args.part_size,
            timeout=args.timeout,
            batch_size=args.batch_size,
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

    # Different post processing executions for spim data
    if type_spim == "smartspim":
        post_upload_smartspim(args, s3_path, n_failed_uploads)


if __name__ == "__main__":
    main()
