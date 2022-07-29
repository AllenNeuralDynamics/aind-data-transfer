import argparse
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
from transfer.gcs import create_client, GCSUploader
from util.fileutils import collect_filepaths, make_cloud_paths

from cluster.config import load_jobqueue_config

logging.basicConfig(format="%(asctime)s %(message)s", datefmt="%Y-%m-%d %H:%M")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def _make_boto_options(n_threads):
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


def _gsutil_upload_worker(
    bucket_name,
    files,
    gcs_path,
    cloud_paths_map,
    worker_id,
    boto_options=None,
):
    options_str = ""
    if boto_options is not None:
        sep = " -o "
        options_str = f"{sep}{sep.join(boto_options)}"

    with tempfile.TemporaryDirectory() as tmpdir:
        logfile = os.path.join(tmpdir, f"gsutil-cp-worker-{worker_id}-log.log")
        catfile = os.path.join(tmpdir, f"file-subset-{worker_id}.txt")
        with open(catfile, "w") as f:
            for file in files:
                f.write(file + "\n")
        cmd = f"cat {catfile} | gsutil -m {options_str} cp -L {logfile} -I gs://{bucket_name}/{gcs_path}"
        ret = subprocess.run(cmd, shell=True)

        failed_uploads = []
        if ret.returncode != 0:
            logger.error(
                f"Error uploading files, gsutil exit code {ret.returncode}"
            )
            failed_uploads.extend(_parse_cp_failures(logfile))

    # convert flat list of blobs to original directory structure
    storage_client = create_client()
    bucket = storage_client.get_bucket(bucket_name)
    for f in files:
        name = PurePath(f).name
        blob = bucket.get_blob(gcs_path + "/" + name)
        target_path = cloud_paths_map[name]
        if blob.name == target_path:
            continue
        logger.info(f"renaming blob from {blob.name} to {target_path}")
        try:
            bucket.rename_blob(blob, target_path)
        except google.api_core.exceptions.NotFound:
            # This seems to happen if the target blob already exists??
            continue

    return failed_uploads


def _python_upload_worker(
    bucket_name, files, gcs_path, root=None, chunk_size=256 * 1024 * 1024
):
    uploader = GCSUploader(bucket_name)
    return uploader.upload_files(files, gcs_path, root, chunk_size=chunk_size)


def _symlink_duplicate_filenames(files, cloud_paths, symlink_dir):
    max_ids = defaultdict(int)
    corrected_paths = []
    cloud_paths_map = {}
    for fpath, cpath in zip(files, cloud_paths):
        fname = PurePath(fpath).name
        if fname in cloud_paths_map:
            max_ids[fname] += 1
            name, ext = os.path.splitext(fname)
            new_name = f"{name}-{max_ids[fname]}{ext}"
            new_path = os.path.join(symlink_dir, new_name)
            os.symlink(fpath, new_path)
            corrected_paths.append(new_path)
            cloud_paths_map[new_name] = cpath
        else:
            cloud_paths_map[fname] = cpath
            corrected_paths.append(fpath)

    return corrected_paths, cloud_paths_map


def _parse_cp_failures(log_path):
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
    # Our target is ~tasks_per_worker chunks of files for each dask worker to upload.
    # The way each job (upload of a single chunk) is actually distributed is up to the scheduler.
    # Some workers may get many jobs while others get few.
    # Increasing tasks_per_worker can help the scheduler balance jobs among workers.
    n = min(n_workers * tasks_per_worker, len(filepaths))
    chunked_files = np.array_split(filepaths, n)
    logger.info(
        f"Split files into {len(chunked_files)} chunks with "
        f"{len(chunked_files[0])} files each"
    )
    return chunked_files


def run_python_cluster_job(
    filepaths, bucket, gcs_path, tasks_per_worker, root=None
):
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
    sep = " -o "
    options_str = f"{sep}{sep.join(_make_boto_options(n_threads))}"
    cmd = f"gsutil -m {options_str} cp -r {input_dir} gs://{bucket}/{gcs_path}"
    ret = subprocess.run(cmd, shell=True)
    if ret.returncode != 0:
        logger.error(f"gsutil exited with code {ret.returncode}")


def run_python_local_job(
    input_dir, bucket, path, n_workers=4, chunk_size=256 * 1024 * 1024
):
    files = collect_filepaths(input_dir, recursive=True)
    chunked_files = _chunk_files(files, n_workers, tasks_per_worker=1)
    args = zip(
        itertools.repeat(bucket),
        chunked_files,
        itertools.repeat(path),
        itertools.repeat(chunk_size),
    )
    with multiprocessing.Pool(n_workers) as pool:
        failed_uploads = list(
            itertools.chain(*pool.starmap(_python_upload_worker, args))
        )
    logger.info(f"{len(failed_uploads)} failed uploads:\n{failed_uploads}")


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
        "--method",
        choices=["gsutil", "python"],
        default="python",
        help="use either gsutil or the google-cloud-storage Python API for upload. "
        "gsutil is much faster, but less stable.",
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

    t0 = time.time()
    if args.cluster:

        files = collect_filepaths(args.input, recursive=args.recursive)

        if args.method == "gsutil":
            symlink_dir = os.path.join(os.getcwd(), f"symlinks-{time.time()}")
            if os.path.isdir(symlink_dir):
                shutil.rmtree(symlink_dir)
            os.makedirs(symlink_dir)

            cloud_paths = make_cloud_paths(files, args.gcs_path, args.input)

            fixed_files, cloud_paths_map = _symlink_duplicate_filenames(
                files, cloud_paths, symlink_dir
            )

            run_gsutil_cluster_job(
                filepaths=fixed_files,
                bucket=args.bucket,
                gcs_path=args.gcs_path,
                cloud_paths_map=cloud_paths_map,
                tasks_per_worker=args.tasks_per_worker,
            )
            # clean up any symlinks
            shutil.rmtree(symlink_dir)

        elif args.method == "python":
            run_python_cluster_job(
                filepaths=files,
                bucket=args.bucket,
                gcs_path=args.gcs_path,
                tasks_per_worker=args.tasks_per_worker,
                root=args.input,
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


if __name__ == "__main__":
    main()
