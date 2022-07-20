import argparse
import itertools
import logging
import multiprocessing
import os
import random
import string
import subprocess
import time
from pathlib import PurePath

import numpy as np
from cluster.config import load_jobqueue_config
from dask_jobqueue import SLURMCluster
from distributed import Client

from transfer.gcs import GCSUploader
from util.fileutils import collect_filepaths, make_cloud_paths

logging.basicConfig(format="%(asctime)s %(message)s", datefmt="%Y-%m-%d %H:%M")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def _file_upload_worker(bucket_name, filename, gcs_path):
    uploader = GCSUploader(bucket_name)
    uploader.upload_file(filename, gcs_path)
    print(f"uploaded {filename}")
    return os.stat(filename).st_size


def _files_upload_worker(bucket_name, files, gcs_path):
    gcs_paths = make_cloud_paths(files, gcs_path)
    # HACK: add random string as prefix to distribute connections across shards.
    # This doesn't seem to help much.
    # see https://cloud.google.com/blog/products/gcp/optimizing-your-cloud-storage-performance-google-cloud-performance-atlas
    prefix = id_generator()
    prefixed_gcs_paths = [prefix + "-" + path for path in gcs_paths]
    uploader = GCSUploader(bucket_name)
    return uploader.upload_files(files, prefixed_gcs_paths)


def _gsutil_upload_worker(bucket_name, files, gcs_path, worker_id):
    import tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        catfile = os.path.join(tmpdir, f"file-subset-{worker_id}.txt")
        with open(catfile, "w") as f:
            for file in files:
                f.write(file + "\n")
        # TODO: check if -m flag respects SLURM allocated resources.
        #  wouldn't be surprised if it tries to use all cores on a node.
        cmd = f"cat {catfile} | gsutil -m cp -I gs://{bucket_name}/{gcs_path}"
        subprocess.call(cmd, shell=True)
    # TODO: get failed uploads from gsutil?
    return []


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


def id_generator(size=16, chars=string.ascii_lowercase + string.digits):
    return "".join(random.choice(chars) for _ in range(size))


def run_cluster_job(
    input_dir,
    bucket,
    gcs_path,
    parallelism,
    recursive,
):
    client, config = get_client()
    ntasks = config["n_workers"]
    filepaths = collect_filepaths(input_dir, recursive)
    n = min(ntasks * parallelism, len(filepaths))
    chunked_files = np.array_split(filepaths, n)
    logger.info(
        f"Split files into {len(chunked_files)} chunks with "
        f"{len(chunked_files[0])} files each"
    )

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
    client.close()


def run_local_job(input_dir, bucket, path, n_workers=4):
    files = collect_filepaths(input_dir, recursive=True)
    gcs_paths = make_cloud_paths(files, path, input_dir)
    args = zip(itertools.repeat(bucket), files, gcs_paths)
    t0 = time.time()
    with multiprocessing.Pool(n_workers) as pool:
        results = pool.starmap(_file_upload_worker, args)
    write_time = time.time() - t0
    bytes_uploaded = sum(results)
    print(f"Done. Took {write_time}s")
    print(f"{bytes_uploaded / write_time / (1024 ** 2)}MiB/s")


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
        default=1,
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
        "--service-account",
        type=str,
        help="Path to Google application credentials json key file",
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
        run_cluster_job(
            input_dir=args.input,
            bucket=args.bucket,
            gcs_path=gcs_path,
            parallelism=args.tasks_per_worker,
            recursive=args.recursive,
        )
    else:
        run_local_job(args.input, args.bucket, args.gcs_path, args.nthreads)
    logger.info(f"Upload done. Took {time.time() - t0}s ")


if __name__ == "__main__":
    main()
