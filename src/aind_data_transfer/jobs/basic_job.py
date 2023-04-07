"""Module to define and potentially run a BasicJob."""

import glob
import json
import logging
import os
import shutil
import sys
import tempfile
from pathlib import Path

import boto3
from aind_codeocean_api.codeocean import CodeOceanClient

from aind_data_transfer.config_loader.base_config import BasicUploadJobConfigs
from aind_data_transfer.transformations.generic_compressors import (
    VideoCompressor,
    ZipCompressor,
)
from aind_data_transfer.transformations.metadata_creation import (
    ProceduresMetadata,
    RawDataDescriptionMetadata,
    SubjectMetadata,
)
from aind_data_transfer.util.s3_utils import upload_to_s3


class BasicJob:
    """Class that defines a basic upload job."""

    def __init__(self, job_configs: BasicUploadJobConfigs):
        """Init with job_configs"""
        self.configs = job_configs
        self._instance_logger = (
            logging.getLogger(__name__)
            .getChild(self.__class__.__name__)
            .getChild(str(id(self)))
        )
        self._instance_logger.setLevel(job_configs.log_level)

    def _check_if_s3_location_exists(self):
        """Check if the s3 bucket and prefix already exists. If so, raise an
        error."""
        s3_client = boto3.client("s3")
        try:
            results = s3_client.list_objects_v2(
                Bucket=self.configs.s3_bucket,
                Prefix=self.configs.s3_prefix,
                MaxKeys=1,
            )
        finally:
            s3_client.close()
        if results["KeyCount"] != 0:
            raise FileExistsError(
                f"S3 path s3://{self.configs.s3_bucket}/"
                f"{self.configs.s3_prefix} already exists. Please consult "
                f"your admin for help removing old folder if desired."
            )
        return None

    def _compress_raw_data(self, temp_dir: Path) -> None:
        """If compress data is set to False, the data will be uploaded to s3.
        Otherwise, it will be zipped, stored in temp_dir and uploaded later."""

        if self.configs.behavior_dir is not None:
            behavior_dir_excluded = self.configs.behavior_dir / "*"
        else:
            behavior_dir_excluded = None

        # If no compression is required, we'll upload the data directly instead
        # of copying it to a temp folder
        if not self.configs.compress_raw_data:
            # Upload non-behavior data to s3
            data_prefix = "/".join(
                [self.configs.s3_prefix, self.configs.experiment_type.value]
            )
            upload_to_s3(
                directory_to_upload=self.configs.data_source,
                s3_bucket=self.configs.s3_bucket,
                s3_prefix=data_prefix,
                dryrun=self.configs.dry_run,
                excluded=behavior_dir_excluded,
            )
        # Otherwise, we'll store the compressed folder in a temp directory
        # and upload it along with the other files
        else:
            self._instance_logger.info("Compressing raw data folder: ")
            zc = ZipCompressor(display_progress_bar=True)
            compressed_data_folder_name = (
                str(os.path.basename(self.configs.data_source)) + ".zip"
            )
            folder_path = (
                temp_dir
                / self.configs.experiment_type.value
                / compressed_data_folder_name
            )
            os.mkdir(folder_path.parent)
            skip_dirs = (
                None
                if self.configs.behavior_dir is None
                else [self.configs.behavior_dir]
            )
            zc.compress_dir(
                input_dir=self.configs.data_source,
                output_dir=folder_path,
                skip_dirs=skip_dirs,
            )
        return None

    def _compile_metadata(self, temp_dir: Path) -> None:
        """Compile metadata files. Keeps the data in the temp_dir before
        uploading to s3."""

        # Get metadata from service
        subject_metadata = SubjectMetadata.from_service(
            domain=self.configs.metadata_service_domain,
            subject_id=self.configs.subject_id,
        )
        subject_file_name = temp_dir / subject_metadata.output_filename
        subject_metadata.write_to_json(subject_file_name)
        procedures_metadata = ProceduresMetadata.from_service(
            domain=self.configs.metadata_service_domain,
            subject_id=self.configs.subject_id,
        )
        procedures_file_name = temp_dir / procedures_metadata.output_filename
        procedures_metadata.write_to_json(procedures_file_name)
        data_description_metadata = RawDataDescriptionMetadata.from_inputs(
            name=self.configs.s3_prefix, modality=[self.configs.modality]
        )
        data_description_file_name = (
            temp_dir / data_description_metadata.output_filename
        )
        data_description_metadata.write_to_json(data_description_file_name)

        # Check user defined metadata
        if self.configs.metadata_dir:
            # Check only json files in user defined metadata folder
            file_paths = glob.glob(str(self.configs.metadata_dir / "*.json"))
            for file_path in file_paths:
                new_path = temp_dir / Path(file_path).name
                # Only overwrite service defined metadata if metadata_dir_force
                # is set to true
                if (
                    not os.path.exists(new_path)
                    or self.configs.metadata_dir_force
                ):
                    shutil.copyfile(file_path, new_path)
        return None

    def _encrypt_behavior_dir(self, temp_dir: Path) -> None:
        """Encrypt the data in the behavior directory. Keeps the data in the
        temp_dir before uploading to s3."""
        if self.configs.behavior_dir:
            encryption_key = (
                self.configs.video_encryption_password.get_secret_value()
            )
            video_compressor = VideoCompressor(encryption_key=encryption_key)
            new_behavior_dir = temp_dir / "behavior"
            # Copy data to a temp directory
            for root, dirs, files in os.walk(self.configs.behavior_dir):
                for file in files:
                    raw_file_path = os.path.join(root, file)
                    new_file_path = os.path.join(new_behavior_dir, file)
                    os.makedirs(os.path.dirname(new_file_path), exist_ok=True)
                    shutil.copy(raw_file_path, new_file_path)
            video_compressor.compress_all_videos_in_dir(new_behavior_dir)
        return None

    def _upload_to_s3(self, temp_dir: Path) -> None:
        """Upload the data to s3."""
        upload_to_s3(
            directory_to_upload=temp_dir,
            s3_bucket=self.configs.s3_bucket,
            s3_prefix=self.configs.s3_prefix,
            dryrun=self.configs.dry_run,
        )
        return None

    def _trigger_codeocean_pipeline(self):
        """Trigger the codeocean pipeline."""
        # Legacy way we set up parameters...
        trigger_capsule_params = {
            "trigger_codeocean_job": {
                "job_type": self.configs.experiment_type.value,
                "capsule_id": self.configs.codeocean_trigger_capsule_id,
                "bucket": self.configs.s3_bucket,
                "prefix": self.configs.s3_prefix,
            }
        }
        co_client = CodeOceanClient(
            token=self.configs.codeocean_api_token.get_secret_value(),
            domain=self.configs.codeocean_domain,
        )
        if self.configs.dry_run:
            self._instance_logger.info(
                f"(dryrun) Would have ran capsule with params: "
                f"{trigger_capsule_params}"
            )
        else:
            response = co_client.run_capsule(
                capsule_id=self.configs.codeocean_trigger_capsule_id,
                data_assets=[],
                parameters=[json.dumps(trigger_capsule_params)],
            )
            self._instance_logger.info(
                f"Code Ocean Response: {response.json()}"
            )
        return None

    def run_job(self):
        """Runs the job. Creates a temp directory to compile the files before
        uploading."""
        self._check_if_s3_location_exists()
        with tempfile.TemporaryDirectory(
            dir=self.configs.temp_directory
        ) as td:
            self._compress_raw_data(temp_dir=Path(td))
            self._encrypt_behavior_dir(temp_dir=Path(td))
            self._compile_metadata(temp_dir=Path(td))
            self._upload_to_s3(temp_dir=Path(td))
            self._trigger_codeocean_pipeline()


if __name__ == "__main__":
    sys_args = sys.argv[1:]
    job_configs_from_main = BasicUploadJobConfigs.from_args(sys_args)
    job = BasicJob(job_configs=job_configs_from_main)
    job.run_job()
