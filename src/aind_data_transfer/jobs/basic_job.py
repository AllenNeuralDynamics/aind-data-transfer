"""Module to define and potentially run a BasicJob."""

import glob
import json
import logging
import os
import shutil
import sys
import tempfile
from datetime import datetime, timezone
from enum import Enum
from importlib import import_module
from pathlib import Path

import boto3
from aind_codeocean_api.codeocean import CodeOceanClient
from aind_data_schema.data_description import Modality

from aind_data_transfer import __version__
from aind_data_transfer.config_loader.base_config import BasicUploadJobConfigs
from aind_data_transfer.transformations.generic_compressors import (
    VideoCompressor,
    ZipCompressor,
)
from aind_data_transfer.transformations.metadata_creation import (
    ProceduresMetadata,
    ProcessingMetadata,
    RawDataDescriptionMetadata,
    SubjectMetadata,
)
from aind_data_transfer.util.s3_utils import upload_to_s3


# It might make more sense to move this class into different repo
class JobTypes(Enum):
    REGISTER_DATA = "register_data"
    RUN_GENERIC_PIPELINE = "run_generic_pipeline"
    TEST = "test"


class BasicJob:
    """Class that defines a basic upload job."""

    def __init__(self, job_configs: BasicUploadJobConfigs):
        """Init with job_configs"""
        self.job_configs = job_configs
        self._instance_logger = (
            logging.getLogger(__name__)
            .getChild(self.__class__.__name__)
            .getChild(str(id(self)))
        )
        self._instance_logger.setLevel(job_configs.log_level)

    def _test_upload(self, temp_dir: Path):
        """Run upload command on empty directory to see if user has permissions
        and aws cli installed.
        """
        upload_to_s3(
            directory_to_upload=temp_dir,
            s3_bucket=self.job_configs.s3_bucket,
            s3_prefix=self.job_configs.s3_prefix,
        )
        return None

    def _check_if_s3_location_exists(self):
        """Check if the s3 bucket and prefix already exists. If so, raise an
        error."""
        s3_client = boto3.client("s3")
        try:
            results = s3_client.list_objects_v2(
                Bucket=self.job_configs.s3_bucket,
                Prefix=self.job_configs.s3_prefix,
                MaxKeys=1,
            )
        finally:
            s3_client.close()
        if (
            results["KeyCount"] != 0
            and self.job_configs.force_cloud_sync is False
        ):
            raise FileExistsError(
                f"S3 path s3://{self.job_configs.s3_bucket}/"
                f"{self.job_configs.s3_prefix} already exists. Please consult "
                f"your admin for help removing old folder if desired."
            )
        elif (
            results["KeyCount"] != 0
            and self.job_configs.force_cloud_sync is True
        ):
            logging.warning(
                f"S3 path s3://{self.job_configs.s3_bucket}/"
                f"{self.job_configs.s3_prefix} already exists. Will force "
                f"syncing of local directory"
            )
        return None

    def _compress_raw_data(self, temp_dir: Path) -> None:
        """If compress data is set to False, the data will be uploaded to s3.
        Otherwise, it will be zipped, stored in temp_dir, and uploaded
        later."""

        if self.job_configs.behavior_dir is not None:
            behavior_dir_excluded = self.job_configs.behavior_dir / "*"
        else:
            behavior_dir_excluded = None

        for modality_config in self.job_configs.modalities:
            self._instance_logger.info(
                f"Starting to process {modality_config.source}"
            )
            if (
                not modality_config.compress_raw_data
                and not modality_config.skip_staging
            ):
                self._instance_logger.info(
                    f"Copying data to staging directory: {temp_dir}"
                )
                dst_dir = temp_dir / modality_config.default_output_folder_name
                shutil.copytree(
                    modality_config.source,
                    dst_dir,
                    ignore=behavior_dir_excluded,
                )
            elif (
                not modality_config.compress_raw_data
                and modality_config.skip_staging
            ):
                self._instance_logger.info(
                    f"Skipping staging and uploading {modality_config.source} "
                    f"directly to s3."
                )
                s3_prefix_modality = "/".join(
                    [
                        self.job_configs.s3_prefix,
                        modality_config.default_output_folder_name,
                    ]
                )
                upload_to_s3(
                    directory_to_upload=modality_config.source,
                    s3_bucket=self.job_configs.s3_bucket,
                    s3_prefix=s3_prefix_modality,
                    dryrun=self.job_configs.dry_run,
                    excluded=behavior_dir_excluded,
                )
            else:
                self._instance_logger.info(
                    f"Compressing data folder: {modality_config.source}"
                )
                if modality_config.modality == Modality.ECEPHYS:
                    # Would prefer to not have imports here, but we have
                    # conditional dependencies
                    EcephysCompressionParameters = getattr(
                        import_module(
                            "aind_data_transfer.transformations."
                            "ephys_compressors"
                        ),
                        "EcephysCompressionParameters",
                    )
                    EphysCompressors = getattr(
                        import_module(
                            "aind_data_transfer.transformations."
                            "ephys_compressors"
                        ),
                        "EphysCompressors",
                    )

                    compression_params = EcephysCompressionParameters(
                        source=modality_config.source,
                        extra_configs=modality_config.extra_configs,
                    )
                    compression_params._number_id = modality_config.number_id
                    ecephys_compress_job = EphysCompressors(
                        job_configs=compression_params,
                        behavior_dir=self.job_configs.behavior_dir,
                        log_level=self.job_configs.log_level,
                    )
                    ecephys_compress_job.compress_raw_data(temp_dir=temp_dir)
                else:
                    zc = ZipCompressor(display_progress_bar=True)
                    compressed_data_folder_name = (
                        str(os.path.basename(modality_config.source)) + ".zip"
                    )
                    folder_path = (
                        temp_dir
                        / modality_config.default_output_folder_name
                        / compressed_data_folder_name
                    )
                    os.mkdir(folder_path.parent)
                    skip_dirs = (
                        None
                        if self.job_configs.behavior_dir is None
                        else [self.job_configs.behavior_dir]
                    )
                    zc.compress_dir(
                        input_dir=modality_config.source,
                        output_dir=folder_path,
                        skip_dirs=skip_dirs,
                    )
        return None

    def _compile_metadata(
        self, temp_dir: Path, process_start_time: datetime
    ) -> None:
        """Compile metadata files. Keeps the data in the temp_dir before
        uploading to s3."""

        # Get metadata from service
        subject_metadata = SubjectMetadata.from_service(
            domain=self.job_configs.metadata_service_domain,
            subject_id=self.job_configs.subject_id,
        )
        subject_file_name = temp_dir / subject_metadata.output_filename
        subject_metadata.write_to_json(subject_file_name)
        procedures_metadata = ProceduresMetadata.from_service(
            domain=self.job_configs.metadata_service_domain,
            subject_id=self.job_configs.subject_id,
        )
        procedures_file_name = temp_dir / procedures_metadata.output_filename
        procedures_metadata.write_to_json(procedures_file_name)
        modalities = [m.modality for m in self.job_configs.modalities]
        data_description_metadata = RawDataDescriptionMetadata.from_inputs(
            name=self.job_configs.s3_prefix,
            modality=modalities,
        )
        data_description_file_name = (
            temp_dir / data_description_metadata.output_filename
        )
        data_description_metadata.write_to_json(data_description_file_name)

        processing_end_time = datetime.now(timezone.utc)
        processing_metadata = ProcessingMetadata.from_modalities_configs(
            modality_configs=self.job_configs.modalities,
            start_date_time=process_start_time,
            end_date_time=processing_end_time,
            output_location=(
                f"s3://{self.job_configs.s3_bucket}/"
                f"{self.job_configs.s3_prefix}"
            ),
            code_url=self.job_configs.aind_data_transfer_repo_location,
            notes=None,
        )
        processing_file_name = temp_dir / processing_metadata.output_filename
        processing_metadata.write_to_json(processing_file_name)

        # Check user defined metadata
        if self.job_configs.metadata_dir:
            # Check only json files in user defined metadata folder
            file_paths = glob.glob(
                str(self.job_configs.metadata_dir / "*.json")
            )
            for file_path in file_paths:
                new_path = temp_dir / Path(file_path).name
                # Only overwrite service defined metadata if metadata_dir_force
                # is set to true
                if (
                    not os.path.exists(new_path)
                    or self.job_configs.metadata_dir_force
                ):
                    shutil.copyfile(file_path, new_path)
        return None

    def _encrypt_behavior_dir(self, temp_dir: Path) -> None:
        """Encrypt the data in the behavior directory. Keeps the data in the
        temp_dir before uploading to s3."""
        if self.job_configs.behavior_dir:
            encryption_key = (
                self.job_configs.video_encryption_password.get_secret_value()
            )
            video_compressor = VideoCompressor(encryption_key=encryption_key)
            new_behavior_dir = temp_dir / "behavior"
            # Copy data to a temp directory
            for root, dirs, files in os.walk(self.job_configs.behavior_dir):
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
            s3_bucket=self.job_configs.s3_bucket,
            s3_prefix=self.job_configs.s3_prefix,
            dryrun=self.job_configs.dry_run,
        )
        return None

    def _trigger_codeocean_pipeline(self):
        """Trigger the codeocean pipeline."""
        if self.job_configs.codeocean_process_capsule_id is not None:
            job_type = JobTypes.RUN_GENERIC_PIPELINE.value
        else:
            # Handle legacy way we set up parameters...
            job_type = self.job_configs.platform.value.abbreviation
        trigger_capsule_params = {
            "trigger_codeocean_job": {
                "job_type": job_type,
                "modalities": (
                    [
                        m.modality.value.abbreviation
                        for m in self.job_configs.modalities
                    ]
                ),
                "capsule_id": self.job_configs.codeocean_trigger_capsule_id,
                "process_capsule_id": (
                    self.job_configs.codeocean_process_capsule_id
                ),
                "bucket": self.job_configs.s3_bucket,
                "prefix": self.job_configs.s3_prefix,
                "aind_data_transfer_version": __version__,
            }
        }
        co_client = CodeOceanClient(
            token=self.job_configs.codeocean_api_token.get_secret_value(),
            domain=self.job_configs.codeocean_domain,
        )
        if self.job_configs.dry_run:
            self._instance_logger.info(
                f"(dryrun) Would have ran capsule with params: "
                f"{trigger_capsule_params}"
            )
        else:
            response = co_client.run_capsule(
                capsule_id=self.job_configs.codeocean_trigger_capsule_id,
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
        process_start_time = datetime.now(timezone.utc)
        self._check_if_s3_location_exists()
        with tempfile.TemporaryDirectory(
            dir=self.job_configs.temp_directory
        ) as td:
            self._instance_logger.info("Checking write credentials...")
            self._test_upload(temp_dir=Path(td))
            self._instance_logger.info("Processing raw data...")
            self._compress_raw_data(temp_dir=Path(td))
            self._instance_logger.info("Checking for behavior directory...")
            self._encrypt_behavior_dir(temp_dir=Path(td))
            self._instance_logger.info("Compiling metadata...")
            self._compile_metadata(
                temp_dir=Path(td), process_start_time=process_start_time
            )
            self._instance_logger.info("Starting s3 upload...")
            self._upload_to_s3(temp_dir=Path(td))
            self._instance_logger.info("Initiating code ocean pipeline...")
            self._trigger_codeocean_pipeline()


if __name__ == "__main__":
    sys_args = sys.argv[1:]
    # First check if json args are set as an environment variable
    if os.getenv("UPLOAD_JOB_JSON_ARGS") is not None and len(sys_args) == 0:
        env_args = ["--json-args", os.getenv("UPLOAD_JOB_JSON_ARGS")]
        job_configs_from_main = BasicUploadJobConfigs.from_json_args(env_args)
    elif "--json-args" in sys_args:
        job_configs_from_main = BasicUploadJobConfigs.from_json_args(sys_args)
    else:
        job_configs_from_main = BasicUploadJobConfigs.from_args(sys_args)
    job = BasicJob(job_configs=job_configs_from_main)
    job.run_job()
