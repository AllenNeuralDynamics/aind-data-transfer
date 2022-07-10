""" Module to handle file transfers to AWS s3 storage.
"""

import logging
import os
from pathlib import PurePath
from typing import Any, List

from awscrt.s3 import S3Client
from botocore.credentials import create_credential_resolver
from botocore.session import get_session
from s3transfer.constants import MB, GB
from s3transfer.crt import (
    BotocoreCRTRequestSerializer,
    CRTTransferManager,
    create_s3_crt_client,
    CRTTransferFuture,
)

LOG_FMT = "%(asctime)s %(message)s"
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
        region="us-west-2",
        num_threads=1,
        target_throughput=5 * GB / 8,
        part_size=8 * MB,
        upload_timeout=None,
        use_ssl=True,
        verify=None,
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
            verify=verify,
        )
        self.request_serializer = BotocoreCRTRequestSerializer(session)
        self.upload_timeout = upload_timeout

    def upload_file(self, filepath: str, s3_bucket: str, s3_key: str) -> None:
        """Upload a single file to s3.
        Args:
            filepath (str): absolute path to file to upload.
            s3_bucket (str): name of s3 bucket
            s3_key (str): location relative to bucket to store blob
        """
        with CRTTransferManager(
            self.s3_crt_client, self.request_serializer
        ) as manager:
            future = manager.upload(filepath, s3_bucket, s3_key)
            _await_file_upload_futures(
                [future], [filepath], self.upload_timeout
            )

    def upload_files(
        self,
        filepaths: Any[str],
        s3_bucket: str,
        s3_folder: str,
        root: str = None,
    ) -> None:
        """Upload a list of files to s3.
        Args:
            filepaths (list): absolute paths of files to upload.
            s3_bucket (str): name of s3 bucket
            s3_folder (str): location relative to bucket to store objects
            root (str): root directory shared by all files in filepaths. If None,
                        all files will be stored as a flat list under s3_folder.
                        Default is None.
        """
        s3_paths = _make_s3_paths(filepaths, s3_folder, root=root)
        with CRTTransferManager(
            self.s3_crt_client, self.request_serializer
        ) as manager:
            futures = []
            for fpath, s3_path in zip(filepaths, s3_paths):
                futures.append(manager.upload(fpath, s3_bucket, s3_path))
            _await_file_upload_futures(futures, filepaths, self.upload_timeout)

    def upload_folder(
        self,
        folder: str,
        s3_bucket: str,
        s3_folder: str,
        recursive: bool = True,
    ) -> None:
        """Upload a directory to s3.
        Args:
            folder (str): absolute path of the folder to upload.
            s3_bucket (str): name of s3 bucket
            s3_folder (str): location relative to bucket to store objects
            recursive (boolean): upload all sub-directories
        """
        filepaths = _collect_filepaths(folder, recursive)
        self.upload_files(filepaths, s3_bucket, s3_folder, root=folder)

    def get_client(self) -> S3Client:
        return self.s3_crt_client

    def set_client(self, client: S3Client) -> None:
        self.s3_crt_client = client

    def get_timeout(self) -> float:
        return self.upload_timeout

    def set_timeout(self, timeout: float) -> None:
        self.upload_timeout = timeout


def _make_s3_paths(
    filepaths: Any[str], s3_folder: str, root: str = None
) -> List[str]:
    s3_paths = []
    for fpath in filepaths:
        if root is None:
            s3_paths.append(os.path.join(s3_folder, PurePath(fpath).name))
        else:
            s3_paths.append(
                os.path.join(s3_folder, os.path.relpath(fpath, root))
            )
    return s3_paths


def _await_file_upload_futures(
    futures: Any[CRTTransferFuture], filepaths: Any[str], timeout: float
) -> None:
    n_files = len(filepaths)
    bytes_uploaded = 0
    for i, (fpath, fut) in enumerate(zip(filepaths, futures)):
        try:
            fut.result(timeout)
            logger.info(f"Uploaded file {i + 1}/{n_files} {fpath}")
            bytes_uploaded += os.stat(fpath).st_size
        except Exception as e:
            logger.error(f"Upload failed for {fpath} \n{e}")
    logger.info(f"{bytes_uploaded / (1024 ** 2)} MiB uploaded")


def _collect_filepaths(folder: str, recursive: bool = True) -> List[str]:
    if not recursive:
        return [
            os.path.join(folder, f)
            for f in os.listdir(folder)
            if os.path.isfile(os.path.join(folder, f))
        ]

    filepaths = []
    for root, _, files in os.walk(folder):
        for f in files:
            filepaths.append(os.path.join(root, f))
    return filepaths
