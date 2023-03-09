"""Tests methods in base_config module"""
import json
import os
import unittest
from unittest import mock
from unittest.mock import MagicMock

from aind_data_transfer.config_loader.base_config import (
    JobEndpoints,
    JobSecrets,
)


class TestJobEndpointsConfigs(unittest.TestCase):
    """Tests methods in JobEndpointsResolver class."""

    EXAMPLE_ENV_VAR1 = json.dumps(
        {
            "codeocean_domain": "some_domain",
            "codeocean_trigger_capsule_id": "some_capsule_id",
            "codeocean_trigger_capsule_version": None,
            "metadata_service_domain": "some_ms_domain",
            "aind_data_transfer_repo_location": "some_location",
        }
    )

    @mock.patch.dict(
        os.environ, {"AIND_DATA_TRANSFER_ENDPOINTS": EXAMPLE_ENV_VAR1}
    )
    @mock.patch("boto3.client")
    def test_resolved_from_env_var(self, mock_boto_client: MagicMock):
        """
        Tests that the parameters can be defined using an env var.
        Parameters
        ----------
        mock_boto_client : MagicMock
          A boto3 client shouldn't be created. We can also test that this isn't
          called.

        Returns
        -------
        None

        """
        job_endpoints = JobEndpoints(
            codeocean_domain="some_domain1", param_store="some_param_store"
        )
        self.assertEqual(
            getattr(job_endpoints, "_JobEndpoints__param_store"),
            "some_param_store",
        )
        self.assertEqual(job_endpoints.codeocean_domain, "some_domain1")
        self.assertEqual(
            job_endpoints.codeocean_trigger_capsule_id, "some_capsule_id"
        )
        self.assertIsNone(job_endpoints.codeocean_trigger_capsule_version)
        self.assertEqual(
            job_endpoints.metadata_service_domain, "some_ms_domain"
        )
        self.assertEqual(
            job_endpoints.aind_data_transfer_repo_location, "some_location"
        )
        self.assertFalse(mock_boto_client.called)

    @mock.patch("boto3.client")
    def test_resolved_from_aws(self, mock_client: MagicMock):
        """
        Tests that the parameters can be pulled from aws
        Parameters
        ----------
        mock_client : MagicMock
          We can mock the api call to retrieve a secret.

        Returns
        -------
        None

        """
        mock_client.return_value.get_parameter.return_value = {
            "Parameter": {"Value": self.EXAMPLE_ENV_VAR1}
        }

        job_endpoints = JobEndpoints()
        self.assertEqual(
            getattr(job_endpoints, "_JobEndpoints__param_store"),
            "/aind/data/transfer/endpoints",
        )
        self.assertEqual(job_endpoints.codeocean_domain, "some_domain")
        self.assertEqual(
            job_endpoints.codeocean_trigger_capsule_id, "some_capsule_id"
        )
        self.assertIsNone(job_endpoints.codeocean_trigger_capsule_version)
        self.assertEqual(
            job_endpoints.metadata_service_domain, "some_ms_domain"
        )
        self.assertEqual(
            job_endpoints.aind_data_transfer_repo_location, "some_location"
        )

    @mock.patch("boto3.client")
    @mock.patch("logging.Logger.warning")
    def test_missing_parameters(
        self, mock_warning: MagicMock, mock_client: MagicMock
    ):
        """
        Tests that a warning is logged if not all the parameters are set.
        Parameters
        ----------
        mock_warning : MagicMock
          Mock the warning call
        mock_client : MagicMock
          We can mock the api call to retrieve a secret.

        Returns
        -------
        None

        """

        mock_response = json.dumps({"codeocean_domain": "some_domain"})
        mock_client.return_value.get_parameter.return_value = {
            "Parameter": {"Value": mock_response}
        }
        job_endpoints = JobEndpoints()
        self.assertEqual("some_domain", job_endpoints.codeocean_domain)
        self.assertIsNone(job_endpoints.codeocean_trigger_capsule_id)
        self.assertIsNone(job_endpoints.codeocean_trigger_capsule_version)
        self.assertIsNone(job_endpoints.aind_data_transfer_repo_location)
        self.assertIsNone(job_endpoints.metadata_service_domain)
        mock_warning.assert_called_once_with(
            "Not all endpoints are configured."
        )


class TestJobSecretsConfigs(unittest.TestCase):
    """Tests methods in JobSecrets class"""

    EXAMPLE_ENV_VAR1 = json.dumps(
        {
            "video_encryption_password": "some_password",
            "codeocean_api_token": "some_token",
        }
    )

    @mock.patch.dict(
        os.environ, {"AIND_DATA_TRANSFER_SECRETS": EXAMPLE_ENV_VAR1}
    )
    @mock.patch("boto3.client")
    def test_resolved_from_env_var(self, mock_boto_client: MagicMock) -> None:
        """
        Tests that the secrets can be defined using an env var.
        Parameters
        ----------
        mock_boto_client : MagicMock
          A boto3 client shouldn't be created. We can also test that this isn't
          called.

        Returns
        -------
        None

        """
        job_secrets = JobSecrets(
            codeocean_api_token="some_token1", secrets_name="some_secret_store"
        )
        self.assertEqual(
            getattr(job_secrets, "_JobSecrets__secrets_name"),
            "some_secret_store",
        )
        self.assertEqual(job_secrets.codeocean_api_token, "some_token1")
        self.assertEqual(
            job_secrets.video_encryption_password, "some_password"
        )

        self.assertFalse(mock_boto_client.called)

    @mock.patch("boto3.client")
    def test_resolved_from_aws(self, mock_client: MagicMock) -> None:
        """
        Tests that the secrets can be pulled from aws
        Parameters
        ----------
        mock_client : MagicMock
          We can mock the api call to retrieve a secret.

        Returns
        -------
        None

        """
        mock_client.return_value.get_secret_value.return_value = {
            "SecretString": self.EXAMPLE_ENV_VAR1
        }

        job_secrets = JobSecrets()
        self.assertEqual(
            getattr(job_secrets, "_JobSecrets__secrets_name"),
            "/aind/data/transfer/secrets",
        )
        self.assertEqual(job_secrets.codeocean_api_token, "some_token")
        self.assertEqual(
            job_secrets.video_encryption_password, "some_password"
        )

    @mock.patch("boto3.client")
    @mock.patch("logging.Logger.warning")
    def test_missing_secrets(
        self, mock_warning: MagicMock, mock_client: MagicMock
    ):
        """
        Tests that a warning is logged if not all the parameters are set.
        Parameters
        ----------
        mock_warning : MagicMock
          Mock the warning call
        mock_client : MagicMock
          We can mock the api call to retrieve a secret.

        Returns
        -------
        None

        """

        mock_response = json.dumps({"codeocean_api_token": "some_token"})
        mock_client.return_value.get_secret_value.return_value = {
            "SecretString": mock_response
        }
        job_secrets = JobSecrets()
        self.assertEqual("some_token", job_secrets.codeocean_api_token)
        self.assertIsNone(job_secrets.video_encryption_password)
        mock_warning.assert_called_once_with(
            "Not all endpoints are configured."
        )


if __name__ == "__main__":
    unittest.main()
