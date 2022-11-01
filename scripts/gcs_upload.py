import argparse
import itertools
import logging
import multiprocessing
import os
import errno
import subprocess
import time
from datetime import datetime
from pathlib import PurePath

import numpy as np
from dask_jobqueue import SLURMCluster
from distributed import Client
from google.cloud.storage import Blob
from aind_data_transfer.gcs import create_client, GCSUploader
from aind_data_transfer.util.file_utils import collect_filepaths, make_cloud_paths

from cluster.config import load_jobqueue_config

logging.basicConfig(format="%(asctime)s %(message)s", datefmt="%Y-%m-%d %H:%M")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


GCLOUD_TOOLS = ['gcloud alpha storage', 'gsutil']


def get_client(deployment="slurm"):
    """
    Args:
        deployment (str): type of cluster
    Returns:
        the dask Client and its configuration dict
    """
    base_config = load_jobqueue_config()
    if deployment == "slurm":
        config = base_config["jobqueue"]["slurm"]
        # cluster config is automatically populated from
        # ~/.config/dask/jobqueue.yaml
        cluster = SLURMCluster()
        cluster.scale(config["n_workers"])
    else:
        raise NotImplementedError
    logger.info(cluster.job_script())
    client = Client(cluster)
    return client, config


def _chunk_files(filepaths, n_workers, tasks_per_worker):
    """
    Partition a list of filepaths into chunks.
    Our target is ~tasks_per_worker chunks of files for each dask worker to upload.
    The way each job (upload of a single chunk) is actually distributed is up to the scheduler.
    Some workers may get many jobs while others get few.
    Increasing tasks_per_worker can help the scheduler balance jobs among workers.
    Args:
        filepaths (list): list of absolute paths to chunk
        n_workers (int): the number of dask workers
        tasks_per_worker (int): target # tasks per dask worker
    Returns:
        a list of lists, where each sublist contains a list of filepaths
    """
    n = min(n_workers * tasks_per_worker, len(filepaths))
    chunked_files = np.array_split(filepaths, n)
    logger.info(
        f"Split files into {len(chunked_files)} chunks with "
        f"{len(chunked_files[0])} files each"
    )
    return chunked_files


def _python_upload_worker(
    bucket_name, files, gcs_path, root=None, chunk_size=256 * 1024 * 1024
):
    """
    Uploads a list of filepaths to GCS using the google-cloud-storage Python API
    Args:
        bucket_name (str): name of bucket
        files (list): list of absolute paths to upload to bucket
        gcs_path (str): google cloud storage path to upload files to
        root (str): directory shared by files to serve as root directory
        chunk_size (int): set the blob chunk size (bytes).
                          Increasing this can speed up transfers.
    """
    uploader = GCSUploader(bucket_name)
    return uploader.upload_files(files, gcs_path, root, chunk_size=chunk_size)


def run_cluster_job(
    filepaths,
    bucket,
    gcs_path,
    tasks_per_worker,
    root=None,
    chunk_size=256 * 1024 * 1024,
):
    """
    Upload a list of filepaths using multiple distributed instances of the google-cloud-storage Python API
    Args:
        filepaths (list): list of absolute paths to upload
        bucket (str): name of the bucket
        gcs_path (str): cloud storage location to store uploaded files
        tasks_per_worker (int): target number of chunks for each worker to process
        root (str): shared directory for filepaths to serve as root folder in the bucket
        chunk_size (int): set the blob chunk size (bytes).
                          Increasing this can speed up transfers.
    """
    client, config = get_client()
    ntasks = config["n_workers"]

    chunked_files = _chunk_files(filepaths, ntasks, tasks_per_worker)

    futures = []
    for i, chunk in enumerate(chunked_files):
        futures.append(
            client.submit(
                _python_upload_worker,
                bucket_name=bucket,
                files=chunk,
                gcs_path=gcs_path,
                root=root,
                chunk_size=chunk_size,
            )
        )
    failed_uploads = list(itertools.chain(*client.gather(futures)))
    logger.info(f"{len(failed_uploads)} failed uploads:\n{failed_uploads}")
    client.shutdown()


def run_python_local_job(
    input_dir, bucket, gcs_path, n_workers=4, chunk_size=256 * 1024 * 1024, exclude_dirs=None
):
    """
    Upload a directory using the google-cloud-storage Python API and multiprocessing
    Args:
        input_dir (str): directory to upload
        bucket (str): name of the bucket
        gcs_path (str): cloud storage location to store uploaded files
        n_workers (int): number of workers
        chunk_size (int): set the blob chunk size (bytes).
                          Increasing this can speed up transfers.
        exclude_dirs (list): list of directory names to exclude, e.g., ["dir1", "dir2"]
    """
    files = collect_filepaths(input_dir, recursive=True, exclude_dirs=exclude_dirs)
    chunked_files = _chunk_files(files, n_workers, tasks_per_worker=1)
    args = zip(
        itertools.repeat(bucket),
        chunked_files,
        itertools.repeat(gcs_path),
        itertools.repeat(input_dir),
        itertools.repeat(chunk_size),
    )
    with multiprocessing.Pool(n_workers) as pool:
        failed_uploads = list(
            itertools.chain(*pool.starmap(_python_upload_worker, args))
        )
    logger.info(f"{len(failed_uploads)} failed uploads:\n{failed_uploads}")


def _parse_cp_failures(log_path):
    """
    Parse the gsutil cp log for any transfers with a non-OK status code
    Args:
        log_path (str): path to the log file created by gsutil,
                        e.g., gsutil cp -L log_path.log ...
    Returns:
        a list of filepaths that resulted in an error during transfer
    """
    failures = []
    with open(log_path, "r") as f:
        next(f)  # skip header
        for line in f:
            parts = line.strip().split(",")
            src = parts[0]
            status = parts[-2]
            if status != "OK":
                failures.append(src)
    return failures


def _make_boto_options(n_threads):
    """Create Boto options to pass to gsutil
    See https://cloud.google.com/storage/docs/boto-gsutil
    Args:
        n_threads (int): number of threads to use for parallel uploads
    Returns:
        a list of option strings
    """
    options = []
    # Use the same process / thread ratio as the ~/.boto defaults
    # These defaults are different on Windows (1 process, many threads)
    processes = max(1, n_threads - 1)
    options.append(f"Boto:parallel_thread_count={n_threads}")
    options.append(f"Boto:parallel_process_count={processes}")
    return options


def build_gcloud_cmd(tool, func, input_dir, bucket, gcs_path, nthreads, recursive=True, logfile=None):
    if tool not in GCLOUD_TOOLS:
        raise ValueError(f"Invalid tool {tool}")
    cmd = f"{tool}"
    if tool == "gsutil" and nthreads > 1:
        sep = " -o "
        cmd += f" -m {sep}{sep.join(_make_boto_options(nthreads))}"
    cmd += f" {func}"
    if logfile is not None:
        cmd += f" -L {logfile}"
    if recursive:
        cmd += " -r"
    cmd += f" {input_dir} gs://{bucket}/{gcs_path}"
    logger.debug(f"Built command: {cmd}")
    return cmd


def run_gcloud_local_job(tool, input_dir, bucket, gcs_path, nthreads):
    """
    Upload a directory using the gcloud storage CLI
    Args:
        tool (str): either "gcloud alpha storage" or "gsutil"
        input_dir (str): directory to upload
        bucket (str): name of the bucket
        gcs_path (str): cloud storage location to store uploaded files
        nthreads (int): num threads to use for upload. Only applies to gsutil
    """
    # assumes there will only be a single space, which should always be the case
    logfile = f"{tool.replace(' ', '_')}-log-{datetime.utcnow().strftime('%Y%m%d%H%M%S%f')}.log"
    cmd = build_gcloud_cmd(
        tool,
        func="cp",
        input_dir=input_dir,
        bucket=bucket,
        gcs_path=gcs_path,
        nthreads=nthreads,
        logfile=logfile
    )
    ret = subprocess.run(cmd, shell=True)
    if ret.returncode != 0:
        logger.error(f"{tool} exited with code {ret.returncode}")
        failed_uploads = _parse_cp_failures(logfile)
        logger.error(f"{len(failed_uploads)} failed uploads:\n{failed_uploads}")
    try:
        os.remove(logfile)
    except OSError as e:
        if e.errno != errno.ENOENT:
            raise


def validate_blobs(bucket_name, target_paths):
    """
    Ensure that all blobs were uploaded correctly
    Args:
        bucket_name (str): bucket name
        target_paths (list): list of cloud paths to validate
    Returns:
        list of cloud storage paths that are missing a Blob
    """
    storage_client = create_client()
    bucket = storage_client.get_bucket(bucket_name)
    missing_paths = []
    for path in target_paths:
        if not Blob(path, bucket).exists():
            missing_paths.append(path)
    return missing_paths


def parse_args():
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
        "--gcs_path",
        type=str,
        default=None,
        help='gcs path relative to bucket, e.g., "folder/data.h5"',
    )
    parser.add_argument(
        "--recursive",
        default=False,
        action="store_true",
        help="upload a folder recursively",
    )
    parser.add_argument(
        "--tasks_per_worker",
        type=int,
        default=3,
        help="Split the work so that each worker has this many jobs. "
        "Increasing this can help with worker/task imbalance.",
    )
    parser.add_argument(
        "--cluster",
        default=False,
        action="store_true",
        help="run in cluster mode",
    )
    parser.add_argument(
        "--nthreads",
        type=int,
        default=1,
        help="number of threads if running locally",
    )
    parser.add_argument(
        "--chunk_size",
        type=int,
        default=256,
        help="upload files in chunks of this size (MB)",
    )
    parser.add_argument(
        "--method",
        choices=['python', 'gsutil', 'gcloud_alpha_storage'],
        default="python",
        help="use either gsutil, gcloud storage or the google-cloud-storage Python API for local upload."
             "This does not apply when --cluster is used.",
    )
    parser.add_argument(
        "--validate",
        default=False,
        action="store_true",
        help="Validate that all uploads were successful. This can take a while if there are many files."
        "This is an additional billed API request for each uploaded object.",
    )
    parser.add_argument(
        "--exclude_dirs",
        type=str,
        nargs="+",
        default=None,
        help="directories to exclude from upload"
    )
    args = parser.parse_args()
    return args


def main():
    args = parse_args()

    gcs_path = args.gcs_path
    if gcs_path is None:
        gcs_path = PurePath(args.input).name
    # remove leading slash since it will result in the structure
    # "bucket/'/'/s3_path" instead of "bucket/s3_path"
    gcs_path = gcs_path.strip("/")
    logger.info(f"Will upload to {args.bucket}/{gcs_path}")

    filepaths = collect_filepaths(args.input, recursive=args.recursive, exclude_dirs=args.exclude_dirs)
    cloud_paths = make_cloud_paths(filepaths, args.gcs_path, args.input)

    t0 = time.time()
    if args.cluster:
        chunk_size = args.chunk_size * 1024 * 1024  # MB to bytes
        run_cluster_job(
            filepaths=filepaths,
            bucket=args.bucket,
            gcs_path=args.gcs_path,
            tasks_per_worker=args.tasks_per_worker,
            root=args.input,
            chunk_size=chunk_size,
        )
    else:
        # replace CLI friendly string (no spaces) with actual gcloud command string
        norm_method = args.method.replace("_", " ")
        if norm_method in GCLOUD_TOOLS:
            # TODO: add exclusions
            run_gcloud_local_job(
                norm_method, args.input, args.bucket, args.gcs_path, args.nthreads
            )
        elif norm_method == "python":
            run_python_local_job(
                args.input, args.bucket, args.gcs_path, args.nthreads, exclude_dirs=args.exclude_dirs
            )
        else:
            raise ValueError(f"Unsupported method {norm_method}")

    logger.info(f"Upload done. Took {time.time() - t0}s ")

    if args.validate:
        missing_blobs = validate_blobs(
            bucket_name=args.bucket, target_paths=cloud_paths
        )
        if not missing_blobs:
            logger.info("All uploads were successful")
        else:
            logger.info(
                f"{len(missing_blobs)} blobs missing.\n{missing_blobs}"
            )


if __name__ == "__main__":
    main()
