import json
import logging
import os
from abc import ABC, abstractmethod

import boto3
from botocore.exceptions import ClientError


class JobEndpointsResolver(ABC):
    @abstractmethod
    def _download_params_from_aws(self):
        pass

    def _endpoint_config_names(self):
        return [
            class_attr
            for class_attr in dir(self)
            if (
                (not class_attr.startswith("_"))
                and (not callable(getattr(self, class_attr)))
            )
        ]

    def _resolve_from_dict(self, param_dict):
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

    def _resolve_endpoint_configs(self, env_var_name):
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

    _DEFAULT_SECRETS_NAME = "/aind/data/transfer/secrets"
    _ENV_VAR_NAME = "AIND_DATA_TRANSFER_SECRETS"

    def __init__(
        self,
        secrets_name: str = _DEFAULT_SECRETS_NAME,
        video_encryption_password: str = None,
        codeocean_api_token: str = None,
    ):
        self.__secrets_name = secrets_name
        self.__video_encryption_password = video_encryption_password
        self.__codeocean_api_token = codeocean_api_token
        self._resolve_endpoint_configs(self._ENV_VAR_NAME)

    @property
    def video_encryption_password(self):
        return self.__video_encryption_password

    @property
    def codeocean_api_token(self):
        return self.__codeocean_api_token

    def _endpoint_config_names(self):
        return [
            "_JobSecrets__video_encryption_password",
            "_JobSecrets__codeocean_api_token",
        ]

    def _download_params_from_aws(self):
        sm_client = boto3.client("secretsmanager")
        try:
            secret_from_aws = sm_client.get_secret_value(
                SecretId=self.__secrets_name
            )
            secret_as_string = secret_from_aws["SecretString"]
            secret_store = json.loads(secret_as_string)
            secrets_raw = json.loads(secret_store.get(self.__secrets_name))
            secrets = {f"_JobSecrets__{k}": v for k, v in secrets_raw.items()}
        except ClientError as e:
            logging.warning(
                f"Unable to retrieve parameters from aws: {e.response}"
            )
            secrets = None
        finally:
            sm_client.close()
        return secrets
