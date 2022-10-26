import logging
from typing import List, Union

import google.auth
import google.auth.exceptions
from google.cloud import storage
from google.cloud.storage import Client
from aind_data_transfer.util.file_utils import make_cloud_paths, collect_filepaths

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def create_client() -> Client:
    try:
        logger.info("Retrieving GCS credentials")
        credentials, project = google.auth.default()
        logger.info("Creating client")
        client = storage.Client(
            credentials=credentials,
            project=project,
        )
    except Exception as e:
        logger.error(
            f"Error while authenticating Google client. Verify json file. \n{e}"
        )
        raise
    return client


class GCSUploader:
    def __init__(self, bucket_name: str):
        self.client = create_client()
        try:
            self.bucket = self.client.get_bucket(bucket_name)
        except google.cloud.exceptions.NotFound:
            logger.error(f"Bucket {bucket_name} not found")
            raise

    def upload_file(
        self,
        filepath: str,
        gcs_key: str,
        timeout: float = None,
        chunk_size: int = 64 * 1024 * 1024,
    ) -> bool:
        """Upload a single file to gcs.
        Args:
            filepath (str): absolute path to file to upload.
            gcs_bucket (str): name of gcs bucket
            gcs_key (str): location relative to bucket to store blob
            timeout (float): upload timeout
            chunk_size (int): upload file in chunks of this size
        Returns:
            A list of filepaths for failed uploads
        """
        blob = self.bucket.blob(gcs_key)
        blob.chunk_size = chunk_size
        try:
            blob.upload_from_filename(filepath, timeout=timeout)
            return True
        except Exception as e:
            logger.error(f"Error uploading file {filepath}\n{e}")
            return False

    def upload_files(
        self,
        filepaths: List[str],
        gcs_path: Union[str, List[str]],
        root: str = None,
        chunk_size: int = 64 * 1024 * 1024,
    ) -> List[str]:
        """Upload a list of files to gcs.
        Args:
            filepaths (list): absolute paths of files to upload.
            gcs_bucket (str): name of gcs bucket
            gcs_path (str or list): if a string, location relative to bucket to store objects.
                                    If a list, the cloud path for each file in filepaths.
            root (str): root directory shared by all files in filepaths.
                        If None, all files will be stored as a flat list
                        under gcs_folder. Default is None.
            chunk_size (int): upload each file in chunks of this size
        Returns:
            A list of filepaths for failed uploads
        """
        if isinstance(gcs_path, str):
            gcs_paths = make_cloud_paths(filepaths, gcs_path, root=root)
        else:
            try:
                _ = iter(gcs_path)
                gcs_paths = gcs_path
            except TypeError:
                logger.error(
                    f"Expected either a str or iterable, got {type(gcs_path)}"
                )
                raise
        failed_uploads = []
        for fpath, gpath in zip(filepaths, gcs_paths):
            if not self.upload_file(fpath, gpath, chunk_size=chunk_size):
                failed_uploads.append(fpath)
        return failed_uploads

    def upload_folder(
        self,
        folder: str,
        gcs_folder: str,
        recursive: bool = True,
        chunk_size: int = 64 * 1024 * 1024,
    ) -> List[str]:
        """Upload a directory to gcs.
        Args:
            folder (str): absolute path of the folder to upload.
            gcs_folder (str): location relative to bucket to store objects
            recursive (boolean): upload all sub-directories
            chunk_size (int): upload each file in chunks of this size
        Returns:
            A list of filepaths for failed uploads
        """
        return self.upload_files(
            collect_filepaths(folder, recursive),
            gcs_folder,
            root=folder,
            chunk_size=chunk_size,
        )
