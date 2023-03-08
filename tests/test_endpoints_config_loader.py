"""Tests endpoints are configured properly"""
import json
import os
import unittest
from unittest import mock
from unittest.mock import MagicMock

from aind_data_transfer.config_loader.endpoints_config import (
    JobEndpoints,
    JobSecrets,
)


class TestJobEndpointsConfigs(unittest.TestCase):

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


class TestJobSecretsConfigs(unittest.TestCase):
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
    def test_resolved_from_env_var(self, mock_boto_client: MagicMock):
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
    def test_resolved_from_aws(self, mock_client: MagicMock):
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


if __name__ == "__main__":
    unittest.main()
