import argparse
import asyncio
import itertools
import logging
import multiprocessing
import os
import shutil
import subprocess
import tempfile
import time
from collections import defaultdict
from pathlib import PurePath

import google.api_core.exceptions
import numpy as np
from dask_jobqueue import SLURMCluster
from distributed import Client
from google.cloud.storage import Blob
from transfer.gcs import create_client, GCSUploader
from transfer.util.fileutils import collect_filepaths, make_cloud_paths

from cluster.config import load_jobqueue_config

logging.basicConfig(format="%(asctime)s %(message)s", datefmt="%Y-%m-%d %H:%M")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


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


class EnvironmentVariableError(Exception):
    pass


def _set_gsutil_tmpdir():
    """
    Sets the temporary directory used by gsutil to the
    fast node-local directory created for the
    job this script is running under.
    """
    ev = "SLURM_JOB_ID"
    if ev not in os.environ:
        raise EnvironmentVariableError(
            "SLURM_JOB_ID is not set. "
            "Are you running from a cluster environment?"
        )
    job_id = os.environ[ev]
    if job_id == "":
        raise EnvironmentVariableError("SLURM_JOB_ID is an empty string")
    # gsutil writes temporary data to this directory
    os.environ["TMPDIR"] = f"/scratch/fast/{job_id}/gsutil-tmpdir"


async def _rename_blob(bucket, blob, target_path):
    """
    Rename the blob to target_path
    Args:
        bucket (google.cloud.storage.Bucket) the bucket instance
        blob (google.cloud.storage.Blob) the blob instance
        target_path (str) the new name for the blob
    """
    try:
        bucket.rename_blob(blob, target_path)
    except google.api_core.exceptions.NotFound as e:
        # This seems to happen if the target blob already exists??
        logger.warning(
            f"Error renaming blob {blob.name} to {target_path}\n"
            f"code: {e.code}\n"
            f"errors: {e.errors}\n"
            f"message: {e.message}\n"
            f"details: {e.details}"
        )


async def _rename_blobs(files, bucket_name, gcs_path, cloud_paths_map):
    """
    Args:
        files (list): list of local filepaths that were uploaded
        bucket_name (str): the name of the bucket where the files were uploaded
        gcs_path (str): the cloud storage path where files were uploaded
        cloud_paths_map (dict): a dictionary mapping uploaded file names (not paths)
                                to their desired cloud paths
    """
    # convert flat list of blobs to original directory structure
    # each rename operation is blocking, so fire all the http requests asynchronously then wait
    storage_client = create_client()
    bucket = storage_client.get_bucket(bucket_name)
    tasks = []
    for f in files:
        name = PurePath(f).name
        blob = bucket.get_blob(gcs_path + "/" + name)
        target_path = cloud_paths_map[name]
        if blob.name == target_path:
            continue
        logger.info(f"renaming blob from {blob.name} to {target_path}")
        tasks.append(
            _rename_blob(bucket=bucket, blob=blob, target_path=target_path)
        )
    await asyncio.gather(*tasks)


def _gsutil_upload_worker(
    bucket_name,
    files,
    gcs_path,
    cloud_paths_map,
    worker_id,
    boto_options=None,
):
    """
    Uploads a list of filepaths to GCS using gsutil
    Args:
        bucket_name (str): name of bucket to upload to
        files (list): list of absolute filepaths to upload to the bucket
        gcs_path (str): the cloud storage path to upload the files to
        cloud_paths_map (dict): a dictionary mapping uploaded file names
                                (not paths) to their target cloud paths
        worker_id (int): A unique ID for this worker
        boto_options (list): list of Boto options to pass to gsutil
    """
    options_str = ""
    if boto_options is not None:
        sep = " -o "
        options_str = f"{sep}{sep.join(boto_options)}"

    with tempfile.TemporaryDirectory() as tmpdir:
        logfile = os.path.join(tmpdir, f"gsutil-cp-worker-{worker_id}-log.log")
        # pass file list to subprocess as a string via stdin
        files_str = "\n".join(files) + "\n"  # carriage return
        cmd = f"gsutil -m {options_str} cp -L {logfile} -I gs://{bucket_name}/{gcs_path}"
        ret = subprocess.run(cmd, shell=True, text=True, input=files_str)

        failed_uploads = []
        if ret.returncode != 0:
            logger.error(
                f"Error uploading files, gsutil exit code {ret.returncode}"
            )
            failed_uploads.extend(_parse_cp_failures(logfile))

    asyncio.run(_rename_blobs(files, bucket_name, gcs_path, cloud_paths_map))

    return failed_uploads


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


def _symlink_duplicate_filenames(filepaths, symlink_dir):
    """If any files in filepaths have the same name (even if from different directories),
    create symlinks with an index-based suffix to disambiguate them. This is necessary since
    gsutil uploads all files to the same directory, and will overwrite any files with the same name.
    Args:
        filepaths (list): list of absolute filepaths
        symlink_dir (str): temporary directory to store symlinks
    Returns:
        a list of disambiguated filepaths
    """
    max_ids = defaultdict(int)
    seen_names = set()
    disambiguated_paths = []
    for fpath in filepaths:
        fname = PurePath(fpath).name
        if fname in seen_names:
            max_ids[fname] += 1
            name, ext = os.path.splitext(fname)
            new_name = f"{name}-{max_ids[fname]}{ext}"
            new_path = os.path.join(symlink_dir, new_name)
            os.symlink(fpath, new_path)
            disambiguated_paths.append(new_path)
        else:
            seen_names.add(fname)
            disambiguated_paths.append(fpath)

    return disambiguated_paths


def _map_filenames_to_cloud_paths(filepaths, cloud_paths):
    """
    Create a dictionary associating file names (not paths) to their target
    cloud paths. File names should be unique.
    Args:
        filepaths (list): list of local absolute paths
        cloud_paths (list): list of corresponding cloud path for each file
    Returns:
        a dictionary mapping filepaths to cloud paths
    """
    fnames = [PurePath(f).name for f in filepaths]
    return dict(zip(fnames, cloud_paths))


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


def run_python_cluster_job(
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


def run_gsutil_cluster_job(
    filepaths,
    bucket,
    gcs_path,
    cloud_paths_map,
    tasks_per_worker,
):
    """
    Upload a list of filepaths using multiple distributed instances of gsutil
    Args:
        filepaths (list): list of absolute paths to upload
        bucket (str): name of the bucket
        gcs_path (str): cloud storage location to store uploaded files
        cloud_paths_map (dict): dictionary mapping file names (not paths) to target cloud paths
        tasks_per_worker (int): target number of chunks for each worker to process
    """
    client, config = get_client()
    ntasks = config["n_workers"]

    chunked_files = _chunk_files(filepaths, ntasks, tasks_per_worker)

    _set_gsutil_tmpdir()
    options = _make_boto_options(config["cores"])

    futures = []
    for i, chunk in enumerate(chunked_files):
        futures.append(
            client.submit(
                _gsutil_upload_worker,
                bucket_name=bucket,
                files=chunk,
                gcs_path=gcs_path,
                cloud_paths_map=cloud_paths_map,
                worker_id=i,
                boto_options=options,
            )
        )

    failed_uploads = list(itertools.chain(*client.gather(futures)))
    logger.info(f"{len(failed_uploads)} failed uploads:\n{failed_uploads}")
    client.shutdown()


def run_gsutil_local_job(input_dir, bucket, gcs_path, n_threads):
    """
    Upload a directory using multithreaded gsutil
    Args:
        input_dir (str): directory to upload
        bucket (str): name of the bucket
        gcs_path (str): cloud storage location to store uploaded files
        n_threads (int): number of threads to use for gsutil
    """
    sep = " -o "
    options_str = f"{sep}{sep.join(_make_boto_options(n_threads))}"
    cmd = f"gsutil -m {options_str} cp -r {input_dir} gs://{bucket}/{gcs_path}"
    ret = subprocess.run(cmd, shell=True)
    if ret.returncode != 0:
        logger.error(f"gsutil exited with code {ret.returncode}")


def run_python_local_job(
    input_dir, bucket, gcs_path, n_workers=4, chunk_size=256 * 1024 * 1024
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
    """
    files = collect_filepaths(input_dir, recursive=True)
    chunked_files = _chunk_files(files, n_workers, tasks_per_worker=1)
    args = zip(
        itertools.repeat(bucket),
        chunked_files,
        itertools.repeat(gcs_path),
        itertools.repeat(chunk_size),
    )
    with multiprocessing.Pool(n_workers) as pool:
        failed_uploads = list(
            itertools.chain(*pool.starmap(_python_upload_worker, args))
        )
    logger.info(f"{len(failed_uploads)} failed uploads:\n{failed_uploads}")


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
        choices=["gsutil", "python"],
        default="python",
        help="use either gsutil or the google-cloud-storage Python API for upload. "
        "gsutil is much faster (~1.25X) in the case of a flat directory, but potentially slower otherwise"
        "due to having to rename every blob to obtain the original directory structure. Renaming blobs"
        "also incurs additional billed API requests.",
    )
    parser.add_argument(
        "--validate",
        default=False,
        action="store_true",
        help="Validate that all uploads were successful. This can take a while if there are many files."
        "This is an additional billed API request for each uploaded object.",
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

    filepaths = collect_filepaths(args.input, recursive=args.recursive)
    cloud_paths = make_cloud_paths(filepaths, args.gcs_path, args.input)

    t0 = time.time()
    if args.cluster:

        if args.method == "gsutil":
            symlink_dir = os.path.join(os.getcwd(), f"symlinks-{time.time()}")
            if os.path.isdir(symlink_dir):
                shutil.rmtree(symlink_dir)
            os.makedirs(symlink_dir)

            disambiguated_filepaths = _symlink_duplicate_filenames(
                filepaths, symlink_dir
            )
            cloud_paths_map = _map_filenames_to_cloud_paths(
                disambiguated_filepaths, cloud_paths
            )

            run_gsutil_cluster_job(
                filepaths=disambiguated_filepaths,
                bucket=args.bucket,
                gcs_path=args.gcs_path,
                cloud_paths_map=cloud_paths_map,
                tasks_per_worker=args.tasks_per_worker,
            )
            # clean up any symlinks
            shutil.rmtree(symlink_dir)

        elif args.method == "python":
            chunk_size = args.chunk_size * 1024 * 1024  # MB to bytes
            run_python_cluster_job(
                filepaths=filepaths,
                bucket=args.bucket,
                gcs_path=args.gcs_path,
                tasks_per_worker=args.tasks_per_worker,
                root=args.input,
                chunk_size=chunk_size,
            )
        else:
            raise ValueError(f"Unsupported method {args.method}")
    else:
        if args.method == "gsutil":
            run_gsutil_local_job(
                args.input, args.bucket, args.gcs_path, args.nthreads
            )
        elif args.method == "python":
            run_python_local_job(
                args.input, args.bucket, args.gcs_path, args.nthreads
            )
        else:
            raise ValueError(f"Unsupported method {args.method}")

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
