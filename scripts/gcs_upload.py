import argparse
import itertools
import logging
import os
import shutil
import subprocess
import time
import tempfile
from collections import defaultdict
from pathlib import PurePath, Path

import google.api_core.exceptions
import numpy as np
from cluster.config import load_jobqueue_config
from dask_jobqueue import SLURMCluster
from distributed import Client

from transfer.gcs import create_client
from util.fileutils import collect_filepaths, make_cloud_paths

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


def _set_gsutil_tmpdir(tmpdir):
    # gsutil writes temporary data to this directory
    os.environ["TMPDIR"] = tmpdir


def _gsutil_upload_worker(
    bucket_name, files, gcs_path, worker_id, boto_options=None
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

    return failed_uploads


def _gsutil_upload_worker2(bucket_name, files, gcs_paths, worker_id):
    """This is unexpectedly slow. There must be a lot of overhead with each call
    to gsutil"""
    failed_uploads = []
    with tempfile.TemporaryDirectory() as tmpdir:
        logfile = os.path.join(tmpdir, f"gsutil-cp-worker-{worker_id}-log.log")
        for fpath, gpath in zip(files, gcs_paths):
            cmd = f"gsutil cp {fpath} gs://{bucket_name}/{gpath}"
            status_code = os.system(cmd)
            if status_code != 0:
                logger.error(
                    f"Error uploading files, gsutil exit code {status_code}"
                )
                failed_uploads.append(fpath)

    return failed_uploads


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
    with open(log_path, 'r') as f:
        next(f)  # skip header
        for line in f:
            parts = line.strip().split(',')
            src = parts[0]
            status = parts[-2]
            if status != "OK":
                failures.append(src)
    return failures


def _fix_blob_paths(bucket_name, path, cloud_paths_map):
    # FIXME: having to do this at the end makes this approach not any faster
    #  than just using the google-cloud-storage API. This might be faster
    #  if broken into dask jobs.
    client = create_client()
    bucket = client.get_bucket(bucket_name)
    for blob in client.list_blobs(bucket_name, prefix=path):
        name = PurePath(blob.name).name
        if name not in cloud_paths_map:
            raise Exception("Blob name not found in cloud_paths_map")
        target_path = cloud_paths_map[name]
        logger.info(f"renaming blob from {blob.name} to {target_path}")
        try:
            bucket.rename_blob(blob, target_path)
        except google.api_core.exceptions.NotFound:
            # This seems to happen if the target blob already exists??
            continue


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


def run_cluster_job(
    filepaths,
    bucket,
    gcs_path,
    tasks_per_worker,
):
    client, config = get_client()
    ntasks = config["n_workers"]
    # Our target is ~tasks_per_worker chunks of files for each dask worker to upload.
    # The way each job (upload of a single chunk) is actually distributed is up to the scheduler.
    # Some workers may get many jobs while others get few.
    # Increasing tasks_per_worker can help the scheduler balance jobs among workers.
    n = min(ntasks * tasks_per_worker, len(filepaths))
    chunked_files = np.array_split(filepaths, n)
    logger.info(
        f"Split files into {len(chunked_files)} chunks with "
        f"{len(chunked_files[0])} files each"
    )

    _set_gsutil_tmpdir(config['local_directory'])

    options = _make_boto_options(config["cores"])

    futures = []
    for i, chunk in enumerate(chunked_files):
        futures.append(
            client.submit(
                _gsutil_upload_worker,
                bucket_name=bucket,
                files=chunk,
                gcs_path=gcs_path,
                worker_id=i,
            )
        )
    failed_uploads = list(itertools.chain(*client.gather(futures)))
    logger.info(f"{len(failed_uploads)} failed uploads:\n{failed_uploads}")
    client.shutdown()


def run_local_job(files, bucket, gcs_path, n_threads):
    options = _make_boto_options(n_threads)
    failed_uploads = _gsutil_upload_worker(
        bucket_name=bucket, files=files, gcs_path=gcs_path, worker_id=0
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

    files = collect_filepaths(args.input, recursive=args.recursive)
    cloud_paths = make_cloud_paths(files, args.gcs_path, args.input)

    symlink_dir = os.path.join(os.getcwd(), f'symlinks-{time.time()}')
    if os.path.isdir(symlink_dir):
        shutil.rmtree(symlink_dir)
    os.makedirs(symlink_dir)

    fixed_files, cloud_paths_map = _symlink_duplicate_filenames(files, cloud_paths, symlink_dir)

    t0 = time.time()
    if args.cluster:
        run_cluster_job(
            filepaths=fixed_files,
            bucket=args.bucket,
            gcs_path=args.gcs_path,
            tasks_per_worker=args.tasks_per_worker,
        )
    else:
        print("Running local job")
        run_local_job(fixed_files, args.bucket, args.gcs_path, args.nthreads)

    _fix_blob_paths(args.bucket, args.gcs_path, cloud_paths_map)

    logger.info(f"Upload done. Took {time.time() - t0}s ")

    # clean up any symlinks
    shutil.rmtree(symlink_dir)


if __name__ == "__main__":
    main()
