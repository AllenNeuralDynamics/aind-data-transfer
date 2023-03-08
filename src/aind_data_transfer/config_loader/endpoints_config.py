"""This module adds classes to handle resolving common endpoints used in the
data transfer jobs."""

import json
import logging
import os
from abc import ABC, abstractmethod
from typing import Optional

import boto3
from botocore.exceptions import ClientError


class JobEndpointsResolver(ABC):
    """Abstract class to contain methods that are used in the parameter store
    resolver to retrieve endpoints and the secrets store manager."""

    @abstractmethod
    def _download_params_from_aws(self):
        """Child classes will need to define a way to retrieve data from aws"""
        pass

    def _endpoint_config_names(self):
        """Get a list of the class attributes that need to be resolved"""
        return [
            class_attr
            for class_attr in dir(self)
            if (
                (not class_attr.startswith("_"))
                and (not callable(getattr(self, class_attr)))
            )
        ]

    def _resolve_from_dict(self, param_dict: Optional[dict]) -> bool:
        """
        Loops through the endpoints. If it isn't set yet, such as during the
        class constructor, then this method will check if it can be set by
        a dictionary. Returns a bool to let the user know whether all the
        parameters are set.

        Parameters
        ----------
        param_dict : Optional[dict]
          The input dictionary where the parameter might be contained

        Returns
        -------
        bool
          True if all the parameters. False if some parameters are not set.

        """
        endpoint_names = self._endpoint_config_names()
        all_params_set = True
        for class_attr in endpoint_names:
            # If not explicitly set, check env vars
            if getattr(self, class_attr) is None and param_dict is None:
                all_params_set = False
            elif (
                getattr(self, class_attr) is None
                and class_attr not in param_dict.keys()
            ):
                all_params_set = False
            elif getattr(self, class_attr) is None:
                self.__setattr__(class_attr, param_dict.get(class_attr))
        return all_params_set

    def _resolve_endpoint_configs(self, env_var_name: str) -> None:
        """
        If the endpoints are not defined in the class constructor, then this
        method will try to resolve the endpoints by checking if they can be
        pulled from an environment variable json string. It will then attempt
        to download a json string from aws to parse.
        Parameters
        ----------
        env_var_name : str
          Name of the environment variable to check

        Returns
        -------
        None
          The attributes are modified in place
        """
        # Try to load from env vars if not set
        env_vars_string = os.getenv(env_var_name)
        env_vars = (
            None if env_vars_string is None else json.loads(env_vars_string)
        )
        # Try to resolve from env var
        all_params_set = self._resolve_from_dict(env_vars)
        # Try to resolve from aws
        if all_params_set is False:
            param_from_aws = self._download_params_from_aws()
            all_params_set = self._resolve_from_dict(param_from_aws)

        if all_params_set is False:
            logging.warning("Not all endpoints are configured.")

        return None


class JobEndpoints(JobEndpointsResolver):
    """This class handles configuring common service endpoints in the jobs."""

    _DEFAULT_PARAMETER_STORE_KEY_NAME = "/aind/data/transfer/endpoints"
    _ENV_VAR_NAME = "AIND_DATA_TRANSFER_ENDPOINTS"

    def __init__(
        self,
        param_store: str = _DEFAULT_PARAMETER_STORE_KEY_NAME,
        codeocean_domain: str = None,
        codeocean_trigger_capsule_id: str = None,
        codeocean_trigger_capsule_version: str = None,
        metadata_service_domain: str = None,
        aind_data_transfer_repo_location: str = None,
    ):
        self.__param_store = param_store
        self.codeocean_domain = codeocean_domain
        self.codeocean_trigger_capsule_id = codeocean_trigger_capsule_id
        self.codeocean_trigger_capsule_version = (
            codeocean_trigger_capsule_version
        )
        self.metadata_service_domain = metadata_service_domain
        self.aind_data_transfer_repo_location = (
            aind_data_transfer_repo_location
        )
        self._resolve_endpoint_configs(self._ENV_VAR_NAME)

    def _download_params_from_aws(self):
        """Attempt to download the endpoints from an aws parameter store"""
        ssm_client = boto3.client("ssm")
        try:
            param_from_store = ssm_client.get_parameter(
                Name=self.__param_store
            )
            param_string = param_from_store["Parameter"]["Value"]
            params = json.loads(param_string)
        except ClientError as e:
            logging.warning(
                f"Unable to retrieve parameters from aws: {e.response}"
            )
            params = None
        finally:
            ssm_client.close()
        return params


class JobSecrets(JobEndpointsResolver):
    """This class handles configuring common secrets used in the jobs."""

    _DEFAULT_SECRETS_NAME = "/aind/data/transfer/secrets"
    _ENV_VAR_NAME = "AIND_DATA_TRANSFER_SECRETS"

    def __init__(
        self,
        secrets_name: str = _DEFAULT_SECRETS_NAME,
        video_encryption_password: str = None,
        codeocean_api_token: str = None,
    ):
        self.__secrets_name = secrets_name
        self.video_encryption_password = video_encryption_password
        self.codeocean_api_token = codeocean_api_token
        self._resolve_endpoint_configs(self._ENV_VAR_NAME)

    def _download_params_from_aws(self):
        """Attempt to download the endpoints from an aws secrets manager"""
        sm_client = boto3.client("secretsmanager")
        try:
            secret_from_aws = sm_client.get_secret_value(
                SecretId=self.__secrets_name
            )
            secret_as_string = secret_from_aws["SecretString"]
            secrets = json.loads(secret_as_string)
        except ClientError as e:
            logging.warning(
                f"Unable to retrieve parameters from aws: {e.response}"
            )
            secrets = None
        finally:
            sm_client.close()
        return secrets
