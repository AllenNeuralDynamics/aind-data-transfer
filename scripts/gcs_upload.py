import argparse
import itertools
import multiprocessing
import os
import time

from transfer.gcs import GCSUploader
from util.fileutils import collect_filepaths, make_cloud_paths


def _file_upload_worker(bucket_name, filename, gcs_path):
    uploader = GCSUploader(bucket_name)
    uploader.upload_file(filename, gcs_path)
    print(f"uploaded {filename}")
    return os.stat(filename).st_size


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
    parser.add_argument("--nthreads", type=int, default=1)
    parser.add_argument(
        "--service-account",
        type=str,
        help="Path to Google application credentials json key file",
    )
    args = parser.parse_args()
    return args


def main():
    args = parse_args()
    run_local_job(args.input, args.bucket, args.gcs_path, args.nthreads)


if __name__ == "__main__":
    main()
