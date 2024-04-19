""" Module to handle file transfers to AWS s3 storage.
"""

import logging
from typing import List

from awscrt.s3 import S3Client
from botocore.session import get_session
from s3transfer.constants import GB, MB
from s3transfer.crt import (
    BotocoreCRTRequestSerializer,
    CRTTransferFuture,
    CRTTransferManager,
    create_s3_crt_client,
    BotocoreCRTCredentialsWrapper
)

from aind_data_transfer.util.file_utils import (
    collect_filepaths,
    make_cloud_paths,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def _get_crt_credentials_provider(session):
    botocore_credentials = session.get_credentials()
    wrapper = BotocoreCRTCredentialsWrapper(
        botocore_credentials
    )
    return wrapper.to_crt_credentials_provider()


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
        self.s3_crt_client = create_s3_crt_client(
            region,
            crt_credentials_provider=_get_crt_credentials_provider(session),
            num_threads=num_threads,
            target_throughput=target_throughput,
            part_size=part_size,
            use_ssl=use_ssl,
            verify=verify,
        )
        self.request_serializer = BotocoreCRTRequestSerializer(session)
        self.upload_timeout = upload_timeout

    def upload_file(
        self, filepath: str, s3_bucket: str, s3_key: str
    ) -> List[str]:
        """Upload a single file to s3.
        Args:
            filepath (str): absolute path to file to upload.
            s3_bucket (str): name of s3 bucket
            s3_key (str): location relative to bucket to store blob
        Returns:
            A list of filepaths for failed uploads
        """
        with CRTTransferManager(
            self.s3_crt_client, self.request_serializer
        ) as manager:
            future = manager.upload(filepath, s3_bucket, s3_key)
            return _await_file_upload_futures(
                [future], [filepath], self.upload_timeout
            )

    def upload_files(
        self,
        filepaths: List[str],
        s3_bucket: str,
        s3_folder: str,
        root: str = None,
    ) -> List[str]:
        """Upload a list of files to s3.
        Args:
            filepaths (list): absolute paths of files to upload.
            s3_bucket (str): name of s3 bucket
            s3_folder (str): location relative to bucket to store objects
            root (str): root directory shared by all files in filepaths.
                        If None, all files will be stored as a flat list
                        under s3_folder. Default is None.
        Returns:
            A list of filepaths for failed uploads
        """
        s3_paths = make_cloud_paths(filepaths, s3_folder, root=root)
        with CRTTransferManager(
            self.s3_crt_client, self.request_serializer
        ) as manager:
            futures = []
            for fpath, s3_path in zip(filepaths, s3_paths):
                futures.append(manager.upload(fpath, s3_bucket, s3_path))
            return _await_file_upload_futures(
                futures, filepaths, self.upload_timeout
            )

    def upload_folder(
        self,
        folder: str,
        s3_bucket: str,
        s3_folder: str,
        recursive: bool = True,
        exclude_dirs: list = None,
    ) -> List[str]:
        """Upload a directory to s3.
        Args:
            folder (str): absolute path of the folder to upload.
            s3_bucket (str): name of s3 bucket
            s3_folder (str): location relative to bucket to store objects
            recursive (boolean): upload all sub-directories
        Returns:
            A list of filepaths for failed uploads
        """
        return self.upload_files(
            list(collect_filepaths(folder, recursive, exclude_dirs=exclude_dirs)),
            s3_bucket,
            s3_folder,
            root=folder,
        )

    def get_client(self) -> S3Client:
        return self.s3_crt_client

    def set_client(self, client: S3Client) -> None:
        self.s3_crt_client = client

    def get_timeout(self) -> float:
        return self.upload_timeout

    def set_timeout(self, timeout: float) -> None:
        self.upload_timeout = timeout


def _await_file_upload_futures(
    futures: List[CRTTransferFuture], filepaths: List[str], timeout: float
) -> List[str]:
    n_files = len(filepaths)
    failed_uploads = []
    for i, (fpath, fut) in enumerate(zip(filepaths, futures)):
        try:
            fut.result(timeout)
            logger.debug(f"Uploaded file {i + 1}/{n_files} {fpath}")
        except Exception as e:
            logger.error(f"Upload failed for {fpath} \n{e}")
            failed_uploads.append(fpath)
    return failed_uploads
