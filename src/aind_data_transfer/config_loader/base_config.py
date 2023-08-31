"""This module adds classes to handle resolving common endpoints used in the
data transfer jobs."""

import argparse
import json
import os
import re
from datetime import date, datetime, time
from pathlib import Path
from typing import Any, Dict, List, Optional

from aind_data_access_api.secrets import get_parameter
from aind_data_schema.data_description import (
    ExperimentType,
    Modality,
    build_data_name,
)
from aind_data_schema.processing import ProcessName
from pydantic import (
    BaseSettings,
    DirectoryPath,
    Field,
    FilePath,
    PrivateAttr,
    SecretStr,
    validator,
)


class BasicJobEndpoints(BaseSettings):
    """Endpoints that define the services to read/write from"""

    aws_param_store_name: Optional[str] = Field(default=None, repr=False)

    codeocean_domain: str = Field(...)
    codeocean_trigger_capsule_id: str = Field(...)
    codeocean_trigger_capsule_version: Optional[str] = Field(None)
    metadata_service_domain: str = Field(...)
    aind_data_transfer_repo_location: str = Field(...)
    video_encryption_password: Optional[SecretStr] = Field(None)
    codeocean_api_token: Optional[SecretStr] = Field(None)
    codeocean_process_capsule_id: Optional[str] = Field(
        None,
        description=(
            "If defined, will run this Code Ocean Capsule after registering "
            "the data asset"
        ),
    )

    class Config:
        """This class will add custom sourcing from aws."""

        @staticmethod
        def settings_from_aws(param_name: Optional[str]):  # noqa: C901
            """
            Curried function that returns a function to retrieve creds from aws
            Parameters
            ----------
            param_name : Name of the credentials we wish to retrieve
            Returns
            -------
            A function that retrieves the credentials.
            """

            def set_settings(_: BaseSettings) -> Dict[str, Any]:
                """
                A simple settings source that loads from aws secrets manager
                """
                params_from_aws = json.loads(get_parameter(param_name))
                if params_from_aws.get("video_encryption_password_path"):
                    video_encrypt_pwd = json.loads(
                        get_parameter(
                            params_from_aws.get(
                                "video_encryption_password_path"
                            ),
                            with_decryption=True,
                        )
                    )
                    params_from_aws[
                        "video_encryption_password"
                    ] = video_encrypt_pwd.get("password")
                    if params_from_aws.get("video_encryption_password_path"):
                        del params_from_aws["video_encryption_password_path"]
                if params_from_aws.get("codeocean_api_token_path"):
                    co_api_token = json.loads(
                        get_parameter(
                            params_from_aws.get("codeocean_api_token_path"),
                            with_decryption=True,
                        )
                    )
                    params_from_aws["codeocean_api_token"] = co_api_token.get(
                        "CODEOCEAN_READWRITE_TOKEN"
                    )
                    if params_from_aws.get("codeocean_api_token_path"):
                        del params_from_aws["codeocean_api_token_path"]
                return params_from_aws

            return set_settings

        @classmethod
        def customise_sources(
            cls,
            init_settings,
            env_settings,
            file_secret_settings,
        ):
            """Class method to return custom sources."""
            aws_param_store_name = init_settings.init_kwargs.get(
                "aws_param_store_name"
            )
            if aws_param_store_name:
                return (
                    init_settings,
                    env_settings,
                    file_secret_settings,
                    cls.settings_from_aws(param_name=aws_param_store_name),
                )
            else:
                return (
                    init_settings,
                    env_settings,
                    file_secret_settings,
                )


class ModalityConfigs(BaseSettings):
    """Class to contain configs for each modality type"""

    # Optional number id to assign to modality config
    _number_id: Optional[int] = PrivateAttr(default=None)
    modality: Modality = Field(
        ..., description="Data collection modality", title="Modality"
    )
    source: DirectoryPath = Field(
        ...,
        description="Location of raw data to be uploaded",
        title="Data Source",
    )
    compress_raw_data: Optional[bool] = Field(
        default=None,
        description="Run compression on data",
        title="Compress Raw Data",
    )
    extra_configs: Optional[FilePath] = Field(
        default=None,
        description="Location of additional configuration file",
        title="Extra Configs",
    )
    skip_staging: bool = Field(
        default=False,
        description="Upload uncompressed directly without staging",
        title="Skip Staging",
    )

    @property
    def number_id(self):
        """Retrieve an optionally assigned numerical id"""
        return self._number_id

    @property
    def default_output_folder_name(self):
        """Construct the default folder name for the modality."""
        if self._number_id is None:
            return self.modality.name.lower()
        else:
            return self.modality.name.lower() + str(self._number_id)

    @validator("compress_raw_data", always=True)
    def get_compress_source_default(
        cls, compress_source: Optional[bool], values: Dict[str, Any]
    ) -> bool:
        """Set compress source default to True for ecephys data."""
        if (
            compress_source is None
            and "modality" in values
            and values["modality"] == Modality.ECEPHYS
        ):
            return True
        elif compress_source is not None:
            return compress_source
        else:
            return False


class BasicUploadJobConfigs(BasicJobEndpoints):
    """Configuration for the basic upload job"""

    _DATE_PATTERN1 = re.compile(r"^\d{4}-\d{2}-\d{2}$")
    _DATE_PATTERN2 = re.compile(r"^\d{1,2}/\d{1,2}/\d{4}$")
    _TIME_PATTERN1 = re.compile(r"^\d{1,2}-\d{1,2}-\d{1,2}$")
    _TIME_PATTERN2 = re.compile(r"^\d{1,2}:\d{1,2}:\d{1,2}$")

    s3_bucket: str = Field(
        ...,
        description="Bucket where data will be uploaded",
        title="S3 Bucket",
    )
    experiment_type: ExperimentType = Field(
        ..., description="Experiment type", title="Experiment Type"
    )
    modalities: List[ModalityConfigs] = Field(
        ...,
        description="Data collection modalities and their directory location",
        title="Modalities",
    )
    subject_id: str = Field(..., description="Subject ID", title="Subject ID")
    acq_date: date = Field(
        ..., description="Date data was acquired", title="Acquisition Date"
    )
    acq_time: time = Field(
        ...,
        description="Time of day data was acquired",
        title="Acquisition Time",
    )
    process_name: ProcessName = Field(
        default=ProcessName.OTHER,
        description="Type of processing performed on the raw data source.",
        title="Process Name",
    )
    temp_directory: Optional[DirectoryPath] = Field(
        default=None,
        description=(
            "As default, the file systems temporary directory will be used as "
            "an intermediate location to store the compressed data before "
            "being uploaded to s3"
        ),
        title="Temp directory",
    )
    behavior_dir: Optional[DirectoryPath] = Field(
        default=None,
        description="Directory of behavior data",
        title="Behavior Directory",
    )
    metadata_dir: Optional[DirectoryPath] = Field(
        default=None,
        description="Directory of metadata",
        title="Metadata Directory",
    )
    log_level: str = Field(
        default="WARNING",
        description="Logging level. Default is WARNING.",
        title="Log Level",
    )
    metadata_dir_force: bool = Field(
        default=False,
        description=(
            "Whether to override metadata from service with metadata in "
            "optional metadata directory"
        ),
        title="Metadata Directory Force",
    )
    dry_run: bool = Field(
        default=False,
        description="Perform a dry-run of data upload",
        title="Dry Run",
    )
    force_cloud_sync: bool = Field(
        default=False,
        description=(
            "Force syncing of data folder even if location exists in cloud"
        ),
        title="Force Cloud Sync",
    )

    @property
    def s3_prefix(self):
        """Construct s3_prefix from configs."""
        return build_data_name(
            label=f"{self.experiment_type.value}_{self.subject_id}",
            creation_date=self.acq_date,
            creation_time=self.acq_time,
        )

    @validator("acq_date", pre=True)
    def _parse_date(cls, date_str: str) -> date:
        """Parses date string to %YYYY-%MM-%DD format"""
        if re.match(BasicUploadJobConfigs._DATE_PATTERN1, date_str):
            return date.fromisoformat(date_str)
        elif re.match(BasicUploadJobConfigs._DATE_PATTERN2, date_str):
            return datetime.strptime(date_str, "%m/%d/%Y").date()
        else:
            raise ValueError(
                "Incorrect date format, should be YYYY-MM-DD or MM/DD/YYYY"
            )

    @validator("acq_time", pre=True)
    def _parse_time(cls, time_str: str) -> time:
        """Parses time string to "%HH-%MM-%SS format"""
        if re.match(BasicUploadJobConfigs._TIME_PATTERN1, time_str):
            return datetime.strptime(time_str, "%H-%M-%S").time()
        elif re.match(BasicUploadJobConfigs._TIME_PATTERN2, time_str):
            return time.fromisoformat(time_str)
        else:
            raise ValueError(
                "Incorrect time format, should be HH-MM-SS or HH:MM:SS"
            )

    @classmethod
    def from_args(cls, args: list):
        """Adds ability to construct settings from a list of arguments."""

        def _help_message(key: str) -> str:
            """Construct help message from field description"""
            return BasicUploadJobConfigs.schema()["properties"][key][
                "description"
            ]

        parser = argparse.ArgumentParser()
        # Required
        parser.add_argument(
            "-a",
            "--acq-date",
            required=True,
            type=str,
            help="Date data was acquired, yyyy-MM-dd or dd/MM/yyyy",
        )
        parser.add_argument(
            "-b",
            "--s3-bucket",
            required=True,
            type=str,
            help=_help_message("s3_bucket"),
        )
        parser.add_argument(
            "-e",
            "--experiment-type",
            required=True,
            type=str,
            help=_help_message("experiment_type"),
        )
        parser.add_argument(
            "-m",
            "--modalities",
            required=True,
            type=str,
            help=(
                f"String that can be parsed as json list where each entry "
                f"has fields: {ModalityConfigs.__fields__}"
            ),
        )
        parser.add_argument(
            "-p",
            "--endpoints-parameters",
            required=True,
            type=str,
            help=(
                "Either a string that can be parsed as a json object or a name"
                " that points to an aws parameter store location"
            ),
        )
        parser.add_argument(
            "-s",
            "--subject-id",
            required=True,
            type=str,
            help=_help_message("subject_id"),
        )
        parser.add_argument(
            "-t",
            "--acq-time",
            required=True,
            type=str,
            help="Time data was acquired, HH-mm-ss or HH:mm:ss",
        )
        # Optional
        parser.add_argument(
            "-l",
            "--log-level",
            required=False,
            type=str,
            help=_help_message("log_level"),
        )
        parser.add_argument(
            "-n",
            "--temp-directory",
            required=False,
            type=str,
            help=_help_message("temp_directory"),
        )
        parser.add_argument(
            "-v",
            "--behavior-dir",
            required=False,
            type=str,
            help=_help_message("behavior_dir"),
        )
        parser.add_argument(
            "-x",
            "--metadata-dir",
            required=False,
            type=str,
            help=_help_message("metadata_dir"),
        )
        parser.add_argument(
            "--dry-run", action="store_true", help=_help_message("dry_run")
        )
        parser.add_argument(
            "--metadata-dir-force",
            action="store_true",
            help=_help_message("metadata_dir_force"),
        )
        parser.add_argument(
            "--force-cloud-sync",
            action="store_true",
            help=_help_message("force_cloud_sync"),
        )
        parser.add_argument(
            "-i",
            "--codeocean-process-capsule-id",
            required=False,
            type=str,
            help=_help_message("codeocean_process_capsule_id"),
        )
        parser.set_defaults(dry_run=False)
        parser.set_defaults(metadata_dir_force=False)
        parser.set_defaults(force_cloud_sync=False)
        job_args = parser.parse_args(args)
        acq_date = job_args.acq_date
        acq_time = job_args.acq_time
        behavior_dir = (
            None
            if job_args.behavior_dir is None
            else Path(os.path.abspath(job_args.behavior_dir))
        )
        metadata_dir = (
            None
            if job_args.metadata_dir is None
            else Path(os.path.abspath(job_args.metadata_dir))
        )
        temp_directory = (
            None
            if job_args.temp_directory is None
            else Path(os.path.abspath(job_args.temp_directory))
        )
        log_level = (
            BasicUploadJobConfigs.__fields__["log_level"].default
            if job_args.log_level is None
            else job_args.log_level
        )
        # The user can define the endpoints explicitly as an object that can be
        # parsed with json.loads()
        try:
            params_from_json = BasicJobEndpoints.parse_obj(
                json.loads(job_args.endpoints_parameters)
            )
            endpoints_param_dict = params_from_json.dict()
        # If the endpoints are not defined explicitly, then we can check if
        # the input defines an aws parameter store name
        except json.decoder.JSONDecodeError:
            endpoints_param_dict = {
                "aws_param_store_name": job_args.endpoints_parameters
            }
        if job_args.codeocean_process_capsule_id is not None:
            endpoints_param_dict[
                "codeocean_process_capsule_id"
            ] = job_args.codeocean_process_capsule_id
        modalities_json = json.loads(job_args.modalities)
        modalities = [ModalityConfigs.parse_obj(m) for m in modalities_json]
        return cls(
            s3_bucket=job_args.s3_bucket,
            subject_id=job_args.subject_id,
            experiment_type=ExperimentType(job_args.experiment_type),
            modalities=modalities,
            acq_date=acq_date,
            acq_time=acq_time,
            behavior_dir=behavior_dir,
            temp_directory=temp_directory,
            metadata_dir=metadata_dir,
            dry_run=job_args.dry_run,
            metadata_dir_force=job_args.metadata_dir_force,
            force_cloud_sync=job_args.force_cloud_sync,
            log_level=log_level,
            **endpoints_param_dict,
        )

    @classmethod
    def from_json_args(cls, args: list):
        """Adds ability to construct settings from a single json string."""

        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--json-args",
            required=True,
            type=str,
            help="Configs passed as a single json string",
        )
        return cls(**json.loads(parser.parse_args(args).json_args))
