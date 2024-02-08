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
import inspect
from importlib import import_module
from pathlib import Path
from typing import Dict, Optional, get_args

import boto3
from aind_codeocean_api.codeocean import CodeOceanClient
from aind_codeocean_api.models.computations_requests import RunCapsuleRequest
from aind_data_schema.base import AindCoreModel
from aind_data_schema.core.data_description import DataDescription
from aind_data_schema.core.procedures import Procedures
from aind_data_schema.core.subject import Subject
from aind_data_schema.models.modalities import Modality
from aind_data_schema.core.metadata import Metadata, MetadataStatus

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
        self.metadata_record: Optional[Metadata] = None

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

    @staticmethod
    def __core_metadata_fields():
        all_model_fields = dict()
        for field_name in Metadata.model_fields:
            # The fields we're interested in are optional. We need to extract out the
            # class using the get_args method
            annotation_args = get_args(
                Metadata.model_fields[field_name].annotation)
            optional_classes = (
                None
                if not annotation_args
                else (
                    [
                        f
                        for f in
                        get_args(Metadata.model_fields[field_name].annotation)
                        if inspect.isclass(f) and issubclass(f, AindCoreModel)
                    ]
                )
            )
            if (
                    optional_classes
                    and inspect.isclass(optional_classes[0])
                    and issubclass(optional_classes[0], AindCoreModel)
            ):
                all_model_fields[field_name] = optional_classes[0]
        return all_model_fields

    @staticmethod
    def __download_json(file_location: Path) -> dict:
        with open(file_location, "r") as f:
            contents = json.load(f)
        return contents

    def _initialize_metadata_record(self, temp_dir: Path):
        """Perform some metadata collection and validation before more
        time-consuming compression and upload steps."""

        # Create a map: filename -> AindCoreModel,
        # e.g., "subject.json" -> Subject
        aind_core_models = self.__core_metadata_fields()
        core_filename_map: Dict[str, AindCoreModel] = {
            f.default_filename(): f for f in aind_core_models.values()
        }
        # Create a map: filename -> Metadata field name,
        # e.g., "subject.json" -> "subject"
        core_field_name_map = {
            v.default_filename(): k for k, v in aind_core_models.items()
        }
        # If the user defined a metadata directory, create a
        # map: filename -> Path, e.g., "subject.json" -> "dir/subject.json"
        metadata_in_folder_map: Dict[str, Path] = dict()
        if self.job_configs.metadata_dir:
            # Check only json files in user defined metadata folder
            file_paths = glob.glob(
                str(self.job_configs.metadata_dir / "*.json")
            )
            for file_path in file_paths:
                metadata_in_folder_map[Path(file_path).name] = Path(file_path)
        # Subject, Procedures, Data Description, and Processing are handled
        # as special cases
        subject_filename = Subject.default_filename()
        procedures_filename = Procedures.default_filename()
        data_description_filename = DataDescription.default_filename()
        # If subject not in user defined directory, query the service
        if metadata_in_folder_map.get(subject_filename) is not None:
            subject_metadata = self.__download_json(
                metadata_in_folder_map[subject_filename]
            )
            file_path = metadata_in_folder_map.get(subject_filename)
            new_path = temp_dir / Path(file_path).name
            shutil.copyfile(file_path, new_path)
            del metadata_in_folder_map[subject_filename]
        else:
            subject_metadata_0 = SubjectMetadata.from_service(
                domain=self.job_configs.metadata_service_domain,
                subject_id=self.job_configs.subject_id,
            )
            subject_metadata_0.write_to_json(temp_dir)
            subject_metadata = subject_metadata_0.model_obj
        del core_filename_map[subject_filename]
        # Basic check that subject_id matches
        if subject_metadata.get("subject_id") != self.job_configs.subject_id:
            raise Exception(
                f"Subject id {self.job_configs.subject_id} not found in"
                f" {subject_metadata}!"
            )

        # If procedures not in user defined directory, query the service
        if metadata_in_folder_map.get(procedures_filename) is not None:
            procedures_metadata = self.__download_json(
                metadata_in_folder_map[procedures_filename]
            )
            file_path = metadata_in_folder_map.get(procedures_filename)
            new_path = temp_dir / Path(file_path).name
            shutil.copyfile(file_path, new_path)
            del metadata_in_folder_map[procedures_filename]
        else:
            procedures_metadata_0 = ProceduresMetadata.from_service(
                domain=self.job_configs.metadata_service_domain,
                subject_id=self.job_configs.subject_id,
            )
            procedures_metadata_0.write_to_json(temp_dir)
            procedures_metadata = procedures_metadata_0.model_obj
        del core_filename_map[procedures_filename]

        # If data_description not in user defined directory, create one
        if metadata_in_folder_map.get(data_description_filename) is not None:
            data_description_metadata = self.__download_json(
                metadata_in_folder_map[data_description_filename]
            )
            file_path = metadata_in_folder_map.get(data_description_filename)
            new_path = temp_dir / Path(file_path).name
            shutil.copyfile(file_path, new_path)
            del metadata_in_folder_map[data_description_filename]
        else:
            modalities = [m.modality for m in self.job_configs.modalities]
            data_description_metadata_0 = (
                RawDataDescriptionMetadata.from_inputs(
                    name=self.job_configs.s3_prefix,
                    modality=modalities,
                )
            )
            data_description_metadata_0.write_to_json(temp_dir)
            data_description_metadata = data_description_metadata_0.model_obj
        del core_filename_map[data_description_filename]

        # Update metadata record object
        self.metadata_record = Metadata(
            name=self.job_configs.s3_prefix,
            location=self.job_configs.s3_bucket,
            subject=subject_metadata,
            procedures=procedures_metadata,
            data_description=data_description_metadata,
        )
        # For the remaining files in metadata dir, copy them over. We'll
        # copy al the files regardless of whether they were generated from
        # a core model. For the core models, we can attach the contents to
        # the metadata record
        for file_name, file_path in metadata_in_folder_map.items():
            new_path = temp_dir / Path(file_path).name
            shutil.copyfile(file_path, new_path)
            if core_field_name_map.get(file_name) is not None:
                model_contents = self.__download_json(file_path)
                field_name = core_field_name_map[file_name]
                setattr(self.metadata_record, field_name, model_contents)

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

    def _add_processing_to_metadata(
        self, temp_dir: Path, process_start_time: datetime
    ):
        """Append the processing metadata to the output"""
        processing_end_time = datetime.now(timezone.utc)
        processing_metadata = ProcessingMetadata.from_modalities_configs(
            modality_configs=self.job_configs.modalities,
            start_date_time=process_start_time,
            end_date_time=processing_end_time,
            output_location=(
                f"s3://{self.job_configs.s3_bucket}/"
                f"{self.job_configs.s3_prefix}"
            ),
            processor_full_name=self.job_configs.processor_name,
            code_url=self.job_configs.aind_data_transfer_repo_location,
        )
        processing_metadata.write_to_json(path=temp_dir)
        processing = processing_metadata.get_model()
        if (
            processing_metadata.validate_obj() is False
            and self.metadata_record.metadata_status != MetadataStatus.MISSING
        ):
            self.metadata_record.metadata_status = MetadataStatus.INVALID
        self.metadata_record.processing = processing
        self.metadata_record.write_standard_file(temp_dir)

    def _encrypt_behavior_dir(self, temp_dir: Path) -> None:
        """Encrypt the data in the behavior directory. Keeps the data in the
        temp_dir before uploading to s3. This feature will be deprecated."""
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
            job_type = self.job_configs.platform.abbreviation
        trigger_capsule_params = {
            "trigger_codeocean_job": {
                "job_type": job_type,
                "modalities": (
                    [
                        m.modality.abbreviation
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
            run_capsule_request = RunCapsuleRequest(
                capsule_id=self.job_configs.codeocean_trigger_capsule_id,
                parameters=[json.dumps(trigger_capsule_params)],
            )
            response = co_client.run_capsule(request=run_capsule_request)
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
            self._instance_logger.info("Compiling initial metadata...")
            self._initialize_metadata_record(temp_dir=Path(td))
            self._instance_logger.info("Processing raw data...")
            self._compress_raw_data(temp_dir=Path(td))
            self._instance_logger.info("Checking for behavior directory...")
            # TODO: remove the encryption step
            self._encrypt_behavior_dir(temp_dir=Path(td))
            self._instance_logger.info("Attaching processing metadata...")
            self._add_processing_to_metadata(
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
