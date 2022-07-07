""" Module to handle file transfers to AWS s3 storage.
"""

import argparse
import os
import logging
import pathlib
import time

from botocore.credentials import create_credential_resolver
from botocore.session import get_session
from s3transfer.constants import MB, GB
from s3transfer.crt import BotocoreCRTRequestSerializer, CRTTransferManager, create_s3_crt_client

LOG_FMT = '%(asctime)s %(message)s'
LOG_DATE_FMT = "%Y-%m-%d %H:%M"

logging.basicConfig(format=LOG_FMT, datefmt=LOG_DATE_FMT)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class S3Uploader:
    """
    Class to handle file and directory uploads to s3
    """

    def __init__(
            self,
            region='us-west-2',
            num_threads=1,
            target_throughput=5 * GB / 8,
            part_size=8 * MB,
            upload_timeout=None,
            use_ssl=True,
            verify=None
    ):
        session = get_session()
        resolver = create_credential_resolver(session, region_name=region)
        self.s3_crt_client = create_s3_crt_client(
            region,
            botocore_credential_provider=resolver,
            num_threads=num_threads,
            target_throughput=target_throughput,
            part_size=part_size,
            use_ssl=use_ssl,
            verify=verify
        )
        self.request_serializer = BotocoreCRTRequestSerializer(
            session
        )
        self.upload_timeout = upload_timeout

    def upload_file(self, filepath, s3_bucket, s3_key):
        """Upload a single file to s3.
        Args:
            filepath (str): absolute path to file to upload.
            s3_bucket (str): name of s3 bucket
            s3_key (str): location relative to bucket to store blob
        """
        bytes_uploaded = 0
        with CRTTransferManager(self.s3_crt_client, self.request_serializer) as manager:
            future = manager.upload(filepath, s3_bucket, s3_key)
            try:
                future.result(self.upload_timeout)
                logger.info(f"Uploaded file {filepath}")
                bytes_uploaded += os.stat(filepath).st_size
            except Exception as e:
                logger.error(f"Upload failed for {filepath} \n{e}")
        logger.info(f"{bytes_uploaded / (1024 ** 2)} MiB uploaded")

    def upload_folder(self, folder, s3_bucket, s3_folder):
        """Upload a directory to s3 recursively.
        Args:
            folder (str): absolute path of the folder to upload.
            s3_bucket (str): name of s3 bucket
            s3_folder (str): location relative to bucket to store objects
        """
        bytes_uploaded = 0
        with CRTTransferManager(self.s3_crt_client, self.request_serializer) as manager:
            filepaths = _collect_filepaths(folder)
            futures = []
            for fpath in filepaths:
                s3_path = os.path.join(s3_folder, os.path.relpath(fpath, folder))
                futures.append(manager.upload(fpath, s3_bucket, s3_path))
            n_files = len(filepaths)
            for i, (fpath, fut) in enumerate(zip(filepaths, futures)):
                try:
                    fut.result(self.upload_timeout)
                    logger.info(f"Uploaded file {i + 1}/{n_files} {fpath}")
                    bytes_uploaded += os.stat(fpath).st_size
                except Exception as e:
                    logger.error(f"Upload failed for {fpath} \n{e}")
        logger.info(f"{bytes_uploaded / (1024 ** 2)} MiB uploaded")

    def get_client(self):
        return self.s3_crt_client

    def set_client(self, client):
        self.s3_crt_client = client

    def get_timeout(self):
        return self.upload_timeout

    def set_timeout(self, timeout):
        self.upload_timeout = timeout


def _collect_filepaths(folder):
    filepaths = []
    for root, _, files in os.walk(folder):
        for f in files:
            filepaths.append(os.path.join(root, f))
    return filepaths


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input', type=str, help='file or folder to upload')
    parser.add_argument('-b', '--bucket', type=str, help='s3 bucket')
    parser.add_argument('--s3_path', type=str, default=None, help='s3 path relative to bucket, e.g., "folder/data.h5"')
    parser.add_argument('--n_threads', type=int, default=1, help='num threads to use for upload')
    parser.add_argument('--target_throughput', type=float, default=100 * GB / 8, help='target throughput (bytes)')
    parser.add_argument('--part_size', type=int, default=8 * MB, help='part size for s3 multipart uploads (bytes)')
    parser.add_argument('--timeout', type=float, default=None, help="per-file upload timeout (s). Default is None.")
    parser.add_argument('--log_level', type=int, default=logging.INFO, help='log level')
    parser.add_argument('--log_file', type=str, default='s3-upload.log', help='log filepath')

    args = parser.parse_args()

    logger.setLevel(args.log_level)
    fh = logging.FileHandler(args.log_file, mode='w')
    fh.setFormatter(logging.Formatter(LOG_FMT, datefmt=LOG_DATE_FMT))
    fh.setLevel(args.log_level)
    logger.addHandler(fh)

    s3_path = args.s3_path
    if s3_path is None:
        s3_path = pathlib.PurePath(args.input).name
        logger.info(f"Will upload to {args.bucket}/{s3_path}")
    # remove leading slash since it will result in the structure "bucket/'/'/s3_path" instead of "bucket/s3_path" 
    s3_path = s3_path.strip('/')

    uploader = S3Uploader(
        num_threads=args.n_threads,
        target_throughput=args.target_throughput,
        part_size=args.part_size,
        upload_timeout=args.timeout
    )

    t0 = time.time()
    if os.path.isdir(args.input):
        uploader.upload_folder(args.input, args.bucket, s3_path)
    elif os.path.isfile(args.input):
        uploader.upload_file(args.input, args.bucket, s3_path)
    else:
        raise ValueError(f"Invalid value for --input: {args.input} does not exist")
    logger.info(f'Upload done. Took {time.time() - t0}s ')


if __name__ == "__main__":
    main()
