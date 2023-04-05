"""This module adds classes to handle resolving common endpoints used in the
data transfer jobs."""

import json
import logging
from typing import Optional, Dict, Any

import boto3
from botocore.exceptions import ClientError

from pydantic import BaseSettings, Field, SecretStr, FilePath, DirectoryPath
from aind_data_schema.data_description import Modality, ExperimentType
from datetime import datetime
import argparse
from pathlib import Path
import re


class BasicJobEndpoints(BaseSettings):
    aws_param_name: Optional[str] = Field(default=None, repr=False)

    codeocean_domain: str = Field(...)
    codeocean_trigger_capsule_id: str = Field(...)
    codeocean_trigger_capsule_version: Optional[str] = Field(None)
    metadata_service_domain: str = Field(...)
    aind_data_transfer_repo_location: str = Field(...)
    video_encryption_password: Optional[SecretStr] = Field(None)
    codeocean_api_token: Optional[SecretStr] = Field(None)

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

            def get_param_from_aws(
                aws_param_name, aws_ssm_client, with_decryption=False
            ) -> Dict[str, Any]:
                try:
                    param_from_store = aws_ssm_client.get_parameter(
                        Name=aws_param_name, WithDecryption=with_decryption
                    )
                    param_string = param_from_store["Parameter"]["Value"]
                    params = json.loads(param_string)
                except ClientError as e:
                    logging.warning(
                        f"Unable to retrieve parameters from aws: {e.response}"
                    )
                    params = {}
                return params

            def set_settings(_: BaseSettings) -> Dict[str, Any]:
                """
                A simple settings source that loads from aws secrets manager
                """
                ssm_client = boto3.client("ssm")
                params_from_aws = get_param_from_aws(
                    aws_param_name=param_name, aws_ssm_client=ssm_client
                )
                if params_from_aws.get("video_encryption_password_path"):
                    video_encrypt_pwd = get_param_from_aws(
                        aws_ssm_client=ssm_client,
                        aws_param_name=params_from_aws.get(
                            "video_encryption_password_path"
                        ),
                        with_decryption=True,
                    )
                    params_from_aws[
                        "video_encryption_password"
                    ] = video_encrypt_pwd.get("password")
                    if params_from_aws.get("video_encryption_password_path"):
                        del params_from_aws["video_encryption_password_path"]
                if params_from_aws.get("codeocean_api_token_path"):
                    co_api_token = get_param_from_aws(
                        aws_ssm_client=ssm_client,
                        aws_param_name=params_from_aws.get(
                            "codeocean_api_token_path"
                        ),
                        with_decryption=True,
                    )
                    params_from_aws["codeocean_api_token"] = (
                        co_api_token.get("CODEOCEAN_READWRITE_TOKEN")
                    )
                    if params_from_aws.get("codeocean_api_token_path"):
                        del params_from_aws["codeocean_api_token_path"]

                ssm_client.close()
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
            aws_param_name = init_settings.init_kwargs.get("aws_param_name")
            if aws_param_name:
                return (
                    init_settings,
                    env_settings,
                    file_secret_settings,
                    cls.settings_from_aws(param_name=aws_param_name),
                )
            else:
                return (
                    init_settings,
                    env_settings,
                    file_secret_settings,
                )


class BasicUploadJobConfigs(BasicJobEndpoints):

    s3_bucket: str = Field(
        ...,
        description="Bucket where data will be uploaded",
        title="S3 Bucket",
    )
    experiment_type: ExperimentType = Field(
        ..., description="Experiment type", title="Experiment Type"
    )
    modality: Modality = Field(
        ..., description="Data collection modality", title="Modality"
    )
    subject_id: str = Field(..., description="Subject ID", title="Subject ID")
    acq_datetime: datetime = Field(
        ...,
        description="Date and time of data acquisition",
        title="Acquisition Datetime",
    )
    data_source: DirectoryPath = Field(
        ...,
        description="Location of raw data to be uploaded",
        title="Data Source",
    )
    behavior_dir: Optional[DirectoryPath] = Field(
        None,
        description="Directory of behavior data",
        title="Behavior Directory",
    )
    metadata_dir: Optional[DirectoryPath] = Field(
        None, description="Directory of metadata", title="Metadata Directory"
    )
    extra_configs: Optional[FilePath] = Field(
        None,
        description="Location of additional configuration file",
        title="Extra Configs",
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
    compress_raw_data: bool = Field(
        default=False,
        description="Run compression on data",
        title="Compress Raw Data",
    )

    @classmethod
    def from_args(cls, args: list):
        """Adds ability to construct settings from a list of arguments."""

        def _help_message(key: str) -> str:
            return BasicUploadJobConfigs.schema()["properties"][key]["description"]

        def _parse_date(date: str) -> str:
            """Parses date string to %YYYY-%MM-%DD format"""
            stripped_date = date.strip()
            pattern = r"^\d{4}-\d{2}-\d{2}$"
            pattern2 = r"^\d{1,2}/\d{1,2}/\d{4}$"
            if re.match(pattern, stripped_date):
                parsed_date = datetime.strptime(stripped_date, "%Y-%m-%d")
                return parsed_date.strftime("%Y-%m-%d")
            elif re.match(pattern2, stripped_date):
                parsed_date = datetime.strptime(stripped_date, "%m/%d/%Y")
                return parsed_date.strftime("%Y-%m-%d")
            else:
                raise ValueError(
                    "Incorrect date format, should be YYYY-MM-DD or MM/DD/YYYY"
                )

        def _parse_time(time: str) -> str:
            """Parses time string to "%HH-%MM-%SS format"""
            stripped_time = time.strip()
            pattern = r"^\d{1,2}-\d{1,2}-\d{1,2}$"
            pattern2 = r"^\d{1,2}:\d{1,2}:\d{1,2}$"
            if re.match(pattern, stripped_time):
                parsed_time = datetime.strptime(stripped_time, "%H-%M-%S")
                return parsed_time.strftime("%H:%M:%S")
            elif re.match(pattern2, stripped_time):
                parsed_time = datetime.strptime(stripped_time, "%H:%M:%S")
                return parsed_time.strftime("%H:%M:%S")
            else:
                raise ValueError(
                    "Incorrect time format, should be HH-MM-SS or HH:MM:SS"
                )

        parser = argparse.ArgumentParser()
        # Required
        parser.add_argument("-a", "--acq-date", required=True, type=str, help="Date data was acquired, yyyy-MM-dd or dd/MM/yyyy")
        parser.add_argument("-b", "--s3-bucket", required=True, type=str, help=_help_message("s3_bucket"))
        parser.add_argument("-d", "--data-source", required=True, type=str, help=_help_message("data_source"))
        parser.add_argument("-e", "--experiment-type", required=True, type=str, help=_help_message("experiment_type"))
        parser.add_argument("-m", "--modality", required=True, type=str, help=_help_message("modality"))
        parser.add_argument("-p", "--endpoints-parameters", required=True, type=str, help="Either a string that can be parsed as a json object or a name that points to an aws parameter store location")
        parser.add_argument("-s", "--subject-id", required=True, type=str, help=_help_message("subject_id"))
        parser.add_argument("-t", "--acq-time", required=True, type=str, help="Time data was acquired, HH-mm-ss or HH:mm:ss")
        # Optional
        parser.add_argument("-c", "--extra-configs", required=False, type=str, help=_help_message("extra_configs"))
        parser.add_argument("-v", "--behavior-dir", required=False, type=str, help=_help_message("behavior_dir"))
        parser.add_argument("-x", "--metadata-dir", required=False, type=str, help=_help_message("metadata_dir"))
        parser.add_argument("--dry-run", action="store_true", help=_help_message("dry_run"))
        parser.add_argument("--compress-raw-data", action="store_true", help=_help_message("compress_raw_data"))
        parser.add_argument("--metadata-dir-force", action="store_true", help=_help_message("metadata_dir_force"))
        parser.set_defaults(dry_run=False)
        parser.set_defaults(compress_raw_data=False)
        parser.set_defaults(metadata_dir_force=False)
        job_args = parser.parse_args(args)
        date_str = _parse_date(job_args.acq_date)
        time_str = _parse_time(job_args.acq_time)
        acq_datetime = datetime.fromisoformat(f"{date_str}T{time_str}")
        behavior_dir = None if job_args.behavior_dir is None else Path(job_args.behavior_dir)
        metadata_dir = None if job_args.metadata_dir is None else Path(job_args.metadata_dir)
        extra_configs = None if job_args.extra_configs is None else Path(job_args.extra_configs)
        # The user can define the endpoints explicitly as an object that can be
        # parsed with json.loads()
        try:
            params_from_json = BasicJobEndpoints.parse_obj(json.loads(job_args.endpoints_parameters))
            param_dict = params_from_json.dict()
        # If the endpoints are not defined explicitly, then we can check if
        # the input defines an aws parameter store name
        except json.decoder.JSONDecodeError:
            param_dict = {"aws_param_name": job_args.endpoints_parameters}
        return cls(s3_bucket=job_args.s3_bucket,
                   data_source=Path(job_args.data_source),
                   subject_id=job_args.subject_id,
                   modality=Modality(job_args.modality),
                   experiment_type=ExperimentType(job_args.experiment_type),
                   acq_datetime=acq_datetime,
                   behavior_dir=behavior_dir,
                   metadata_dir=metadata_dir,
                   extra_configs=extra_configs,
                   dry_run=job_args.dry_run,
                   compress_raw_data=job_args.compress_raw_data,
                   metadata_dir_force=job_args.metadata_dir_force,
                   **param_dict)
