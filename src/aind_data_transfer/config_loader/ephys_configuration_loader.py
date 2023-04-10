"""Loads Ephys job configurations"""
import argparse
import json
import logging
import os
import re
from pathlib import Path
from warnings import warn

import yaml
from botocore.exceptions import ClientError
from numcodecs import Blosc

from aind_data_transfer.readers.ephys_readers import EphysReaders
from aind_data_transfer.util.s3_utils import get_secret

warn(
    f"The module {__name__} is deprecated and will be removed in future "
    f"versions.",
    DeprecationWarning,
    stacklevel=2,
)


class EphysJobConfigurationLoader:
    """Class to handle loading ephys job configs"""

    DEFAULT_AWS_REGION = "us-west-2"
    # Not the actual passwords, just the key name to retrieve it from aws
    # secrets manager
    SERVICE_ENDPOINT_KEY = "service_endpoints"
    VIDEO_ENCRYPTION_KEY_NAME = "video_encryption_password"
    CODE_OCEAN_API_TOKEN_NAME = "codeocean-api-token"
    # TODO: Is there a better way to do this?
    CODE_REPO_LOCATION = (
        "https://github.com/AllenNeuralDynamics/aind-data-transfer"
    )

    @staticmethod
    def _get_endpoints(s3_region: str) -> dict:
        """
        If the service endpoints aren't set in the sys args, then this method
        will try to pull them from aws secrets manager. It's static since it's
        being called before the job is created.
        Parameters
        ----------
        s3_region : str

        Returns
        -------
        dict
          Will return an empty dictionary if the service endpoints are not set
          in sys args, or if they can't be pulled from aws secrets manager.

        """
        try:
            s3_secret_name = EphysJobConfigurationLoader.SERVICE_ENDPOINT_KEY
            get_secret(s3_secret_name, s3_region)
            endpoints = json.loads(get_secret(s3_secret_name, s3_region))
        except ClientError as e:
            logging.warning(
                f"Unable to retrieve aws secret: "
                f"{EphysJobConfigurationLoader.SERVICE_ENDPOINT_KEY}"
            )
            logging.debug(e.response)
            endpoints = {}
        return endpoints

    def __remove_none(self, data):
        """Remove keys whose value is None."""
        if isinstance(data, dict):
            return {
                k: self.__remove_none(v)
                for k, v in data.items()
                if v is not None
            }
        else:
            return data

    @staticmethod
    def __parse_compressor_configs(configs):
        """Util method to map a string to class attribute"""
        try:
            compressor_name = configs["compress_data_job"]["compressor"][
                "compressor_name"
            ]
        except KeyError:
            compressor_name = None
        try:
            compressor_kwargs = configs["compress_data_job"]["compressor"][
                "kwargs"
            ]
        except KeyError:
            compressor_kwargs = None
        if (
            compressor_name
            and compressor_name == Blosc.codec_id
            and compressor_kwargs
            and "shuffle" in compressor_kwargs
        ):
            shuffle_str = configs["compress_data_job"]["compressor"]["kwargs"][
                "shuffle"
            ]
            shuffle_val = getattr(Blosc, shuffle_str)
            configs["compress_data_job"]["compressor"]["kwargs"][
                "shuffle"
            ] = shuffle_val

    @staticmethod
    def __resolve_endpoints(configs):  # noqa: C901
        """
        Only the raw data source needs to be provided as long as the base dir
        name is formatted correctly. If the dest_data_dir and cloud endpoints
        are not set in the conf file, they will be created automatically based
        on the name of the raw_data_source.
        Args:
            configs (dic): Configurations

        Returns:
            None, modifies the base configs in place
        """
        raw_data_folder = Path(configs["endpoints"]["raw_data_dir"]).name

        dest_data_dir = configs["endpoints"]["dest_data_dir"]
        reg_match_subject_datetime = re.match(
            EphysReaders.SourceRegexPatterns.subject_datetime.value,
            raw_data_folder,
        )
        reg_match_ecephys_subject_datetime = re.match(
            EphysReaders.SourceRegexPatterns.ecephys_subject_datetime.value,
            raw_data_folder,
        )
        if dest_data_dir is None and reg_match_subject_datetime is not None:
            configs["endpoints"]["dest_data_dir"] = (
                "ecephys_" + raw_data_folder
            )
        if (
            configs["data"].get("subject_id") is None
            and reg_match_subject_datetime is not None
        ):
            configs["data"]["subject_id"] = reg_match_subject_datetime.group(1)
        if (
            dest_data_dir is None
            and reg_match_ecephys_subject_datetime is not None
        ):
            configs["endpoints"]["dest_data_dir"] = raw_data_folder
        if (
            configs["data"].get("subject_id") is None
            and reg_match_ecephys_subject_datetime is not None
        ):
            configs["data"][
                "subject_id"
            ] = reg_match_ecephys_subject_datetime.group(1)

        if configs["endpoints"]["s3_prefix"] is None:
            dest_data_folder = Path(configs["endpoints"]["dest_data_dir"]).name
            configs["endpoints"]["s3_prefix"] = dest_data_folder

        if configs["endpoints"]["gcp_prefix"] is None:
            dest_data_folder = Path(configs["endpoints"]["dest_data_dir"]).name
            configs["endpoints"]["gcp_prefix"] = dest_data_folder

        if configs["trigger_codeocean_job"]["job_type"] is None:
            configs["trigger_codeocean_job"]["job_type"] = configs["data"][
                "name"
            ]

        if configs["trigger_codeocean_job"]["bucket"] is None:
            configs["trigger_codeocean_job"]["bucket"] = configs["endpoints"][
                "s3_bucket"
            ]

        if configs["trigger_codeocean_job"]["prefix"] is None:
            configs["trigger_codeocean_job"]["prefix"] = configs["endpoints"][
                "s3_prefix"
            ]

        if configs["aws_secret_names"]["region"] is None:
            configs["aws_secret_names"][
                "region"
            ] = EphysJobConfigurationLoader.DEFAULT_AWS_REGION

        # Not the actual password, just the key used to retrieve it
        if configs["aws_secret_names"]["video_encryption_password"] is None:
            configs["aws_secret_names"][
                "video_encryption_password"
            ] = EphysJobConfigurationLoader.VIDEO_ENCRYPTION_KEY_NAME

        if configs["aws_secret_names"]["code_ocean_api_token_name"] is None:
            configs["aws_secret_names"][
                "code_ocean_api_token_name"
            ] = EphysJobConfigurationLoader.CODE_OCEAN_API_TOKEN_NAME

        if configs["endpoints"]["code_repo_location"] is None:
            configs["endpoints"][
                "code_repo_location"
            ] = EphysJobConfigurationLoader.CODE_REPO_LOCATION

        if (
            (configs["endpoints"]["codeocean_domain"] is None)
            or (configs["endpoints"]["metadata_service_url"] is None)
            or (configs["trigger_codeocean_job"]["capsule_id"] is None)
        ):
            service_endpoints = EphysJobConfigurationLoader._get_endpoints(
                configs["aws_secret_names"]["region"]
            )
            if configs["endpoints"]["codeocean_domain"] is None:
                configs["endpoints"]["codeocean_domain"] = service_endpoints[
                    "codeocean_domain"
                ]
            if configs["endpoints"]["metadata_service_url"] is None:
                configs["endpoints"][
                    "metadata_service_url"
                ] = service_endpoints["metadata_service_url"]

            if configs["trigger_codeocean_job"]["capsule_id"] is None:
                configs["trigger_codeocean_job"][
                    "capsule_id"
                ] = service_endpoints["codeocean_trigger_capsule"]

    @staticmethod
    def __resolve_logging(configs: dict) -> None:
        """
        Resolves logging config in place
        Parameters
        ----------
        configs : dict
          Configurations

        Returns
        -------
        None
        """

        if configs["logging"]["level"] is None:
            configs["logging"]["level"] = "INFO"
        if os.getenv("LOG_LEVEL"):
            configs["logging"]["level"] = os.getenv("LOG_LEVEL")

    def load_configs(self, sys_args):
        """Load yaml config at conf_src Path as python dict"""
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "-c", "--conf-file-location", required=True, type=str
        )
        parser.add_argument(
            "-r", "--raw-data-source", required=False, type=str
        )
        parser.add_argument(
            "-b", "--behavior-directory", required=False, type=str
        )
        args = parser.parse_args(sys_args)
        conf_src = args.conf_file_location
        with open(conf_src) as f:
            raw_config = yaml.load(f, Loader=yaml.SafeLoader)
        if args.raw_data_source is not None:
            raw_config["endpoints"]["raw_data_dir"] = args.raw_data_source
        if args.behavior_directory is not None:
            raw_config["endpoints"][
                "behavior_directory"
            ] = args.behavior_directory
        self.__resolve_endpoints(raw_config)
        self.__resolve_logging(raw_config)
        config_without_nones = self.__remove_none(raw_config)
        self.__parse_compressor_configs(config_without_nones)
        return config_without_nones
