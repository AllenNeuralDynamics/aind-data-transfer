"""This module adds classes to handle resolving common endpoints used in the
data transfer jobs."""

import argparse
import json
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, ClassVar, Union, Type, Tuple

from aind_data_schema.core.data_description import build_data_name
from aind_data_schema.core.processing import ProcessName
from aind_data_schema.models.modalities import Modality
from aind_data_schema.models.platforms import Platform
from pydantic import (
    DirectoryPath,
    Field,
    FilePath,
    PrivateAttr,
    SecretStr,
    field_validator,
)
from pydantic_core.core_schema import ValidationInfo
from pydantic_settings import (
    BaseSettings,
    EnvSettingsSource,
    InitSettingsSource,
    PydanticBaseSettingsSource,
)
from aind_codeocean_api.credentials import JsonConfigSettingsSource
from aind_data_access_api.secrets import get_parameter


class AWSConfigSettingsParamSource(JsonConfigSettingsSource):
    """Class that parses from aws secrets manager."""

    @staticmethod
    def _get_param(param_name: str) -> Dict[str, Any]:
        """
        Retrieves a secret from AWS Secrets Manager.

        Parameters
        ----------
        param_name : str
          Parameter name as stored in Parameter Store

        Returns
        -------
        Dict[str, Any]
          Contents of the parameter

        """

        params_from_aws = json.loads(get_parameter(param_name))
        if params_from_aws.get("video_encryption_password_path"):
            video_encrypt_pwd = json.loads(
                get_parameter(
                    params_from_aws.get("video_encryption_password_path"),
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

    def _retrieve_contents(self) -> Dict[str, Any]:
        """Retrieve contents from config_file_location"""
        credentials_from_aws = self._get_param(self.config_file_location)
        return credentials_from_aws


class BasicJobEndpoints(BaseSettings):
    """Endpoints that define the services to read/write from"""

    aws_param_store_name: Optional[str] = Field(default=None, repr=False)

    codeocean_domain: str = Field(...)
    codeocean_trigger_capsule_id: Optional[str] = Field(None)
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

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: Type[BaseSettings],
        init_settings: InitSettingsSource,
        env_settings: EnvSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> Tuple[PydanticBaseSettingsSource, ...]:
        """
        Method to pull configs from a variety sources, such as a file or aws.
        Arguments are required and set by pydantic.
        Parameters
        ----------
        settings_cls : Type[BaseSettings]
          Top level class. Model fields can be pulled from this.
        init_settings : InitSettingsSource
          The settings in the init arguments.
        env_settings : EnvSettingsSource
          The settings pulled from environment variables.
        dotenv_settings : PydanticBaseSettingsSource
          Settings from .env files. Currently, not supported.
        file_secret_settings : PydanticBaseSettingsSource
          Settings from secret files such as used in Docker. Currently, not
          supported.

        Returns
        -------
        Tuple[PydanticBaseSettingsSource, ...]

        """

        aws_param_store_path = init_settings.init_kwargs.get(
            "aws_param_store_name"
        )

        # If user defines aws secrets, create creds from there
        if aws_param_store_path is not None:
            return (
                init_settings,
                AWSConfigSettingsParamSource(
                    settings_cls, aws_param_store_path
                ),
            )
        # Otherwise, create creds from init and env
        else:
            return (
                init_settings,
                env_settings,
            )


class ModalityConfigs(BaseSettings):
    """Class to contain configs for each modality type"""

    # Need some way to extract abbreviations. Maybe a public method can be
    # added to the Modality class
    _MODALITY_MAP: ClassVar = {
        m().abbreviation.upper().replace("-", "_"): m().abbreviation
        for m in Modality._ALL
    }

    # Optional number id to assign to modality config
    _number_id: Optional[int] = PrivateAttr(default=None)
    modality: Modality.ONE_OF = Field(
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
        validate_default=True,
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
            return self.modality.abbreviation
        else:
            return self.modality.abbreviation + str(self._number_id)

    @field_validator("modality", mode="before")
    def parse_modality_string(
        cls, input_modality: Union[str, dict, Modality]
    ) -> Union[dict, Modality]:
        """Attempts to convert strings to a Modality model. Raises an error
        if unable to do so."""
        if isinstance(input_modality, str):
            modality_abbreviation = cls._MODALITY_MAP.get(
                input_modality.upper()
            )
            if modality_abbreviation is None:
                raise AttributeError(f"Unknown Modality: {input_modality}")
            return Modality.from_abbreviation(modality_abbreviation)
        else:
            return input_modality

    @field_validator("compress_raw_data", mode="after")
    def get_compress_source_default(
        cls, compress_source: Optional[bool], info: ValidationInfo
    ) -> bool:
        """Set compress source default to True for ecephys data."""
        if (
            compress_source is None
            and info.data.get("modality") == Modality.ECEPHYS
        ):
            return True
        elif compress_source is not None:
            return compress_source
        else:
            return False


class BasicUploadJobConfigs(BasicJobEndpoints):
    """Configuration for the basic upload job"""

    _DATETIME_PATTERN1: ClassVar = re.compile(
        r"^\d{4}-\d{2}-\d{2}[ |T]\d{2}:\d{2}:\d{2}$"
    )
    _DATETIME_PATTERN2: ClassVar = re.compile(
        r"^\d{1,2}/\d{1,2}/\d{4} \d{1,2}:\d{2}:\d{2} [APap][Mm]$"
    )

    s3_bucket: str = Field(
        ...,
        description="Bucket where data will be uploaded",
        title="S3 Bucket",
    )
    platform: Platform.ONE_OF = Field(..., description="Platform", title="Platform")
    modalities: List[ModalityConfigs] = Field(
        ...,
        description="Data collection modalities and their directory location",
        title="Modalities",
    )
    subject_id: str = Field(..., description="Subject ID", title="Subject ID")
    acq_datetime: datetime = Field(
        ...,
        description="Datetime data was acquired",
        title="Acquisition Datetime",
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
    processor_name: str = Field(
        default="service",
        description="Name of entity processing the data",
        title="Processor Name",
    )
    process_name: ProcessName = Field(
        default=ProcessName.OTHER,
        description="Type of processing performed on the raw data source.",
        title="Process Name",
    )

    @property
    def s3_prefix(self):
        """Construct s3_prefix from configs."""
        return build_data_name(
            label=f"{self.platform.abbreviation}_{self.subject_id}",
            creation_datetime=self.acq_datetime,
        )

    @field_validator("acq_datetime", mode="before")
    def _parse_datetime(cls, datetime_str: str) -> datetime:
        """Parses datetime string to %YYYY-%MM-%DD HH:mm:ss"""
        # TODO: do this in data transfer service
        if re.match(BasicUploadJobConfigs._DATETIME_PATTERN1, datetime_str):
            return datetime.fromisoformat(datetime_str)
        elif re.match(BasicUploadJobConfigs._DATETIME_PATTERN2, datetime_str):
            return datetime.strptime(datetime_str, "%m/%d/%Y %I:%M:%S %p")
        else:
            raise ValueError(
                "Incorrect datetime format, should be YYYY-MM-DD HH:mm:ss "
                "or MM/DD/YYYY I:MM:SS P"
            )

    @field_validator("platform", mode="before")
    def _parse_platform(cls, v: Union[str, dict, Platform]) -> Platform:
        """Parses abbreviations to Platform model"""
        if isinstance(v, str):
            return Platform.from_abbreviation(v)
        else:
            return v

    @classmethod
    def from_args(cls, args: list):
        """Adds ability to construct settings from a list of arguments."""

        def _help_message(key: str) -> str:
            """Construct help message from field description"""
            return BasicUploadJobConfigs.model_json_schema()["properties"][key][
                "description"
            ]

        parser = argparse.ArgumentParser()
        # Required
        parser.add_argument(
            "-a",
            "--acq-datetime",
            required=True,
            type=str,
            help=(
                "Datetime data was acquired, YYYY-MM-DD HH:mm:ss "
                "or MM/DD/YYYY I:MM:SS P"
            ),
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
            "--platform",
            required=True,
            type=str,
            help=_help_message("platform"),
        )
        parser.add_argument(
            "-m",
            "--modalities",
            required=True,
            type=str,
            help=(
                f"String that can be parsed as json list where each entry "
                f"has fields: {ModalityConfigs.model_fields}"
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
        acq_datetime = job_args.acq_datetime
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
            BasicUploadJobConfigs.model_fields["log_level"].default
            if job_args.log_level is None
            else job_args.log_level
        )
        # The user can define the endpoints explicitly as an object that can be
        # parsed with json.loads()
        try:
            params_from_json = BasicJobEndpoints.model_validate(
                json.loads(job_args.endpoints_parameters)
            )
            endpoints_param_dict = params_from_json.model_dump()
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
        modalities = [ModalityConfigs.model_validate(m) for m in modalities_json]
        return cls(
            s3_bucket=job_args.s3_bucket,
            subject_id=job_args.subject_id,
            platform=Platform.from_abbreviation(job_args.platform),
            modalities=modalities,
            acq_datetime=acq_datetime,
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


class ConfigError(Exception):
    """Exception raised for errors in the config file."""

    def __init__(self, message: str):
        """Constructs the error message."""
        self.message = message

    def __str__(self):
        """Returns the error message."""
        return repr(self.message)
