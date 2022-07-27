import argparse
import itertools
import logging
import os
import subprocess
import time
import tempfile
from pathlib import PurePath

import numpy as np
from cluster.config import load_jobqueue_config
from dask_jobqueue import SLURMCluster
from distributed import Client

from util.fileutils import collect_filepaths

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
        catfile = os.path.join(tmpdir, f"file-subset-{worker_id}.txt")
        with open(catfile, "w") as f:
            for file in files:
                f.write(file + "\n")
        cmd = f"cat {catfile} | gsutil -m {options_str} cp -I gs://{bucket_name}/{gcs_path}"
        ret = subprocess.run(cmd, shell=True)

    failed_uploads = []
    if ret.returncode != 0:
        logger.error(
            f"Error uploading files, gsutil exit code {ret.returncode}"
        )
        # TODO get the specific paths that failed from the log?
        failed_uploads.extend(files)

    return failed_uploads


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
    input_dir,
    bucket,
    gcs_path,
    tasks_per_worker,
    recursive,
):
    client, config = get_client()
    ntasks = config["n_workers"]
    filepaths = collect_filepaths(input_dir, recursive)
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
                boto_options=options,
            )
        )
    failed_uploads = list(itertools.chain(*client.gather(futures)))
    logger.info(f"{len(failed_uploads)} failed uploads:\n{failed_uploads}")
    client.close()


def run_local_job(input_dir, bucket, path, n_threads):
    files = collect_filepaths(input_dir, recursive=True)
    options = _make_boto_options(n_threads)
    failed_uploads = _gsutil_upload_worker(
        bucket_name=bucket, files=files, gcs_path=path, worker_id=0, boto_options=options
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

    t0 = time.time()
    if args.cluster:
        run_cluster_job(
            input_dir=args.input,
            bucket=args.bucket,
            gcs_path=gcs_path,
            tasks_per_worker=args.tasks_per_worker,
            recursive=args.recursive,
        )
    else:
        run_local_job(args.input, args.bucket, args.gcs_path, args.nthreads)
    logger.info(f"Upload done. Took {time.time() - t0}s ")


if __name__ == "__main__":
    main()
