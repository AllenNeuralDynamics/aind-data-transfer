"""This module adds classes to handle resolving common endpoints used in the
data transfer jobs."""

import json
from typing import Any, Dict, Optional, Tuple, Type

from aind_codeocean_api.credentials import JsonConfigSettingsSource
from aind_data_access_api.secrets import get_parameter
from pydantic import (
    Field,
    SecretStr,
)
from pydantic_settings import (
    BaseSettings,
    EnvSettingsSource,
    InitSettingsSource,
    PydanticBaseSettingsSource,
)


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


class ConfigError(Exception):
    """Exception raised for errors in the config file."""

    def __init__(self, message: str):
        """Constructs the error message."""
        self.message = message

    def __str__(self):
        """Returns the error message."""
        return repr(self.message)
