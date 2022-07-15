import logging
from typing import List

import google.auth
import google.auth.exceptions
from google.cloud import storage
from google.cloud.storage import Client
from util.fileutils import make_cloud_paths, collect_filepaths

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
        self, filepath: str, gcs_key: str, timeout: float = None
    ) -> bool:
        """Upload a single file to gcs.
        Args:
            filepath (str): absolute path to file to upload.
            gcs_bucket (str): name of gcs bucket
            gcs_key (str): location relative to bucket to store blob
            timeout (float): upload timeout
        Returns:
            A list of filepaths for failed uploads
        """
        blob = self.bucket.blob(gcs_key)
        try:
            blob.upload_from_filename(filepath, timeout=timeout)
            return True
        except Exception as e:
            logger.error(f"Error uploading file {filepath}\n{e}")
            return False

    def upload_files(
        self,
        filepaths: List[str],
        gcs_folder: str,
        root: str = None,
    ) -> List[str]:
        """Upload a list of files to gcs.
        Args:
            filepaths (list): absolute paths of files to upload.
            gcs_bucket (str): name of gcs bucket
            gcs_folder (str): location relative to bucket to store objects
            root (str): root directory shared by all files in filepaths.
                        If None, all files will be stored as a flat list
                        under gcs_folder. Default is None.
        Returns:
            A list of filepaths for failed uploads
        """
        gcs_paths = make_cloud_paths(filepaths, gcs_folder, root=root)
        failed_uploads = []
        for fpath, gcs_path in zip(filepaths, gcs_paths):
            if not self.upload_file(fpath, gcs_path):
                failed_uploads.append(fpath)
        return failed_uploads

    def upload_folder(
        self,
        folder: str,
        gcs_folder: str,
        recursive: bool = True,
    ) -> List[str]:
        """Upload a directory to gcs.
        Args:
            folder (str): absolute path of the folder to upload.
            gcs_folder (str): location relative to bucket to store objects
            recursive (boolean): upload all sub-directories
        Returns:
            A list of filepaths for failed uploads
        """
        return self.upload_files(
            collect_filepaths(folder, recursive),
            gcs_folder,
            root=folder,
        )
