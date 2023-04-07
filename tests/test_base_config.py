"""Tests methods in base_config module"""
import json
import os
import unittest
from datetime import date, time
from pathlib import Path
from unittest import mock
from unittest.mock import MagicMock

from aind_data_schema.data_description import ExperimentType, Modality

from aind_data_transfer.config_loader.base_config import (
    BasicJobEndpoints,
    BasicUploadJobConfigs,
)

TEST_DIR = Path(os.path.dirname(os.path.realpath(__file__))) / "resources"
DATA_DIR = TEST_DIR / "v0.6.x_neuropixels_multiexp_multistream"
CONFIG_FILE = TEST_DIR / "test_configs" / "ephys_upload_job_test_configs.yml"
BEHAVIOR_DIR = TEST_DIR / "v0.6.x_neuropixels_multiexp_multistream" / "Videos"
METADATA_DIR = TEST_DIR / "test_metadata"


class TestJobEndpointsConfigs(unittest.TestCase):
    """Tests methods in BasicJobEndpoints class."""

    EXAMPLE_ENV_VAR1 = {
        "CODEOCEAN_DOMAIN": "some_domain",
        "CODEOCEAN_TRIGGER_CAPSULE_ID": "some_capsule_id",
        "METADATA_SERVICE_DOMAIN": "some_ms_domain",
        "AIND_DATA_TRANSFER_REPO_LOCATION": "some_dtr_location",
        "VIDEO_ENCRYPTION_PASSWORD": "some_password",
    }

    EXAMPLE_PARAM_STORE_RESPONSE = json.dumps(
        {
            "codeocean_domain": "some_domain",
            "codeocean_trigger_capsule_id": "some_capsule_id",
            "metadata_service_domain": "some_ms_domain",
            "aind_data_transfer_repo_location": "some_dtr_location",
            "video_encryption_password_path": "/path/to/password",
            "codeocean_api_token_path": "/path/to/token",
        }
    )

    @mock.patch.dict(os.environ, EXAMPLE_ENV_VAR1, clear=True)
    @mock.patch("boto3.client")
    def test_resolved_from_env_var(self, mock_boto_client: MagicMock):
        """
        Tests that endpoints can be defined using env vars.
        """
        job_endpoints = BasicJobEndpoints()
        self.assertEqual("some_domain", job_endpoints.codeocean_domain)
        self.assertEqual(
            "some_capsule_id", job_endpoints.codeocean_trigger_capsule_id
        )
        self.assertIsNone(job_endpoints.codeocean_trigger_capsule_version)
        self.assertEqual(
            "some_ms_domain", job_endpoints.metadata_service_domain
        )
        self.assertEqual(
            "some_dtr_location", job_endpoints.aind_data_transfer_repo_location
        )
        self.assertEqual(
            "some_password",
            job_endpoints.video_encryption_password.get_secret_value(),
        )
        self.assertIsNone(job_endpoints.codeocean_api_token)
        self.assertFalse(mock_boto_client.called)

    @mock.patch("boto3.client")
    def test_pull_from_aws(self, mock_client: MagicMock):
        """Tests that endpoints are set correctly from aws param store"""

        mock_client.return_value.get_parameter.side_effect = [
            {"Parameter": {"Value": self.EXAMPLE_PARAM_STORE_RESPONSE}},
            {
                "Parameter": {
                    "Value": json.dumps({"password": "some_password"})
                }
            },
            {
                "Parameter": {
                    "Value": json.dumps(
                        {"CODEOCEAN_READWRITE_TOKEN": "some_token"}
                    )
                }
            },
        ]
        job_endpoints = BasicJobEndpoints(
            aws_param_store_name="some_param_store_name"
        )
        self.assertEqual("some_domain", job_endpoints.codeocean_domain)
        self.assertEqual(
            "some_capsule_id", job_endpoints.codeocean_trigger_capsule_id
        )
        self.assertIsNone(job_endpoints.codeocean_trigger_capsule_version)
        self.assertEqual(
            "some_ms_domain", job_endpoints.metadata_service_domain
        )
        self.assertEqual(
            "some_dtr_location", job_endpoints.aind_data_transfer_repo_location
        )
        self.assertEqual(
            "some_password",
            job_endpoints.video_encryption_password.get_secret_value(),
        )
        self.assertEqual(
            "some_token", job_endpoints.codeocean_api_token.get_secret_value()
        )


class TestBasicUploadJobConfigs(unittest.TestCase):
    """Tests that the configs for the basic upload job are set correctly."""

    EXAMPLE_ENV_VAR1 = {
        "CODEOCEAN_DOMAIN": "some_domain",
        "CODEOCEAN_TRIGGER_CAPSULE_ID": "some_capsule_id",
        "METADATA_SERVICE_DOMAIN": "some_ms_domain",
        "AIND_DATA_TRANSFER_REPO_LOCATION": "some_dtr_location",
        "VIDEO_ENCRYPTION_PASSWORD": "some_password",
        "S3_BUCKET": "some_bucket",
        "EXPERIMENT_TYPE": "confocal",
        "MODALITY": "ECEPHYS",
        "SUBJECT_ID": "12345",
        "ACQ_DATE": "2020-10-10",
        "ACQ_TIME": "10:10:10",
        "DATA_SOURCE": str(DATA_DIR),
        "DRY_RUN": "true",
    }

    EXAMPLE_PARAM_STORE_RESPONSE = json.dumps(
        {
            "codeocean_domain": "some_domain",
            "codeocean_trigger_capsule_id": "some_capsule_id",
            "metadata_service_domain": "some_ms_domain",
            "aind_data_transfer_repo_location": "some_dtr_location",
            "video_encryption_password_path": "/path/to/password",
            "codeocean_api_token_path": "/path/to/token",
        }
    )

    @mock.patch.dict(os.environ, EXAMPLE_ENV_VAR1, clear=True)
    @mock.patch("boto3.client")
    def test_resolved_from_env_vars(self, mock_boto_client: MagicMock):
        """Tests that the configs can be set from env vars"""

        basic_job_configs = BasicUploadJobConfigs()
        self.assertEqual("some_domain", basic_job_configs.codeocean_domain)
        self.assertEqual(
            "some_capsule_id", basic_job_configs.codeocean_trigger_capsule_id
        )
        self.assertEqual(
            "some_ms_domain", basic_job_configs.metadata_service_domain
        )
        self.assertEqual(
            "some_dtr_location",
            basic_job_configs.aind_data_transfer_repo_location,
        )
        self.assertEqual(
            "some_password",
            basic_job_configs.video_encryption_password.get_secret_value(),
        )
        self.assertEqual("some_bucket", basic_job_configs.s3_bucket)
        self.assertEqual(
            ExperimentType.CONFOCAL, basic_job_configs.experiment_type
        )
        self.assertEqual(Modality.ECEPHYS, basic_job_configs.modality)
        self.assertEqual("12345", basic_job_configs.subject_id)
        self.assertEqual(
            date.fromisoformat("2020-10-10"),
            basic_job_configs.acq_date,
        )
        self.assertEqual(
            time.fromisoformat("10:10:10"),
            basic_job_configs.acq_time,
        )
        self.assertEqual(DATA_DIR, basic_job_configs.data_source)
        self.assertTrue(basic_job_configs.dry_run)
        self.assertIsNone(basic_job_configs.behavior_dir)
        self.assertIsNone(basic_job_configs.extra_configs)
        self.assertIsNone(basic_job_configs.metadata_dir)
        self.assertFalse(basic_job_configs.metadata_dir_force)
        self.assertEqual(
            "confocal_12345_2020-10-10_10-10-10", basic_job_configs.s3_prefix
        )
        self.assertFalse(mock_boto_client.called)

    @mock.patch("boto3.client")
    def test_from_req_args(self, mock_client: MagicMock):
        """Tests that the required configs can be set from aws param store"""
        test_req_args = [
            "-d",
            str(DATA_DIR),
            "-b",
            "some_bucket",
            "-s",
            "12345",
            "-e",
            "SmartSPIM",
            "-m",
            "OPHYS",
            "-a",
            "2022-10-10",
            "-t",
            "13-24-01",
            "-p",
            "/aws/param/store",
        ]

        mock_client.return_value.get_parameter.side_effect = [
            {"Parameter": {"Value": self.EXAMPLE_PARAM_STORE_RESPONSE}},
            {
                "Parameter": {
                    "Value": json.dumps({"password": "some_password"})
                }
            },
            {
                "Parameter": {
                    "Value": json.dumps(
                        {"CODEOCEAN_READWRITE_TOKEN": "some_token"}
                    )
                }
            },
        ]

        basic_job_configs = BasicUploadJobConfigs.from_args(test_req_args)
        self.assertEqual("some_domain", basic_job_configs.codeocean_domain)
        self.assertEqual(
            "some_capsule_id", basic_job_configs.codeocean_trigger_capsule_id
        )
        self.assertEqual(
            "some_ms_domain", basic_job_configs.metadata_service_domain
        )
        self.assertEqual(
            "some_dtr_location",
            basic_job_configs.aind_data_transfer_repo_location,
        )
        self.assertEqual(
            "some_password",
            basic_job_configs.video_encryption_password.get_secret_value(),
        )
        self.assertEqual("some_bucket", basic_job_configs.s3_bucket)
        self.assertEqual(
            ExperimentType.SMARTSPIM, basic_job_configs.experiment_type
        )
        self.assertEqual(Modality.OPHYS, basic_job_configs.modality)
        self.assertEqual("12345", basic_job_configs.subject_id)
        self.assertEqual(
            date.fromisoformat("2022-10-10"),
            basic_job_configs.acq_date,
        )
        self.assertEqual(
            time.fromisoformat("13:24:01"),
            basic_job_configs.acq_time,
        )
        self.assertEqual(
            "SmartSPIM_12345_2022-10-10_13-24-01", basic_job_configs.s3_prefix
        )
        self.assertEqual("WARNING", basic_job_configs.log_level)
        self.assertEqual(DATA_DIR, basic_job_configs.data_source)
        self.assertFalse(basic_job_configs.dry_run)
        self.assertIsNone(basic_job_configs.behavior_dir)
        self.assertIsNone(basic_job_configs.extra_configs)
        self.assertIsNone(basic_job_configs.metadata_dir)

    @mock.patch("boto3.client")
    def test_from_opt_args(self, mock_client: MagicMock):
        """Tests that the optional configs can also be set."""

        test_opt_args = [
            "-d",
            str(DATA_DIR),
            "-b",
            "some_bucket",
            "-s",
            "12345",
            "-e",
            "SmartSPIM",
            "-m",
            "OPHYS",
            "-l",
            "INFO",
            "-a",
            "2022-10-10",
            "-t",
            "13-24-01",
            "-p",
            "/aws/param/store",
            "-c",
            str(CONFIG_FILE),
            "-n",
            str(TEST_DIR),
            "-v",
            str(BEHAVIOR_DIR),
            "-x",
            str(METADATA_DIR),
            "--dry-run",
            "--compress-raw-data",
            "--metadata-dir-force",
        ]

        mock_client.return_value.get_parameter.side_effect = [
            {"Parameter": {"Value": self.EXAMPLE_PARAM_STORE_RESPONSE}},
            {
                "Parameter": {
                    "Value": json.dumps({"password": "some_password"})
                }
            },
            {
                "Parameter": {
                    "Value": json.dumps(
                        {"CODEOCEAN_READWRITE_TOKEN": "some_token"}
                    )
                }
            },
        ]

        basic_job_configs = BasicUploadJobConfigs.from_args(test_opt_args)
        self.assertEqual("some_domain", basic_job_configs.codeocean_domain)
        self.assertEqual(
            "some_capsule_id", basic_job_configs.codeocean_trigger_capsule_id
        )
        self.assertEqual(
            "some_ms_domain", basic_job_configs.metadata_service_domain
        )
        self.assertEqual(
            "some_dtr_location",
            basic_job_configs.aind_data_transfer_repo_location,
        )
        self.assertEqual(
            "some_password",
            basic_job_configs.video_encryption_password.get_secret_value(),
        )
        self.assertEqual("some_bucket", basic_job_configs.s3_bucket)
        self.assertEqual(
            ExperimentType.SMARTSPIM, basic_job_configs.experiment_type
        )
        self.assertEqual(Modality.OPHYS, basic_job_configs.modality)
        self.assertEqual("12345", basic_job_configs.subject_id)
        self.assertEqual(
            date.fromisoformat("2022-10-10"),
            basic_job_configs.acq_date,
        )
        self.assertEqual(
            time.fromisoformat("13:24:01"),
            basic_job_configs.acq_time,
        )
        self.assertEqual(
            "SmartSPIM_12345_2022-10-10_13-24-01", basic_job_configs.s3_prefix
        )
        self.assertEqual(DATA_DIR, basic_job_configs.data_source)
        self.assertEqual("INFO", basic_job_configs.log_level)
        self.assertTrue(basic_job_configs.dry_run)
        self.assertTrue(basic_job_configs.compress_raw_data)
        self.assertTrue(basic_job_configs.metadata_dir_force)
        self.assertEqual(BEHAVIOR_DIR, basic_job_configs.behavior_dir)
        self.assertEqual(CONFIG_FILE, basic_job_configs.extra_configs)
        self.assertEqual(METADATA_DIR, basic_job_configs.metadata_dir)

    @mock.patch("boto3.client")
    def test_from_custom_endpoints(self, mock_client: MagicMock):
        """Tests that the endpoints can be defined using a json encoded str"""

        custom_endpoints = {
            "codeocean_domain": "some_domain",
            "codeocean_trigger_capsule_id": "some_capsule_id",
            "metadata_service_domain": "some_ms_domain",
            "aind_data_transfer_repo_location": "some_dtr_location",
            "video_encryption_password": "some_password",
            "codeocean_api_token": "some_token",
        }

        test_req_args = [
            "-d",
            str(DATA_DIR),
            "-b",
            "some_bucket",
            "-s",
            "12345",
            "-e",
            "SmartSPIM",
            "-m",
            "OPHYS",
            "-a",
            "10/10/2022",
            "-t",
            "13:24:01",
            "-p",
            json.dumps(custom_endpoints),
        ]

        test_malformed_date_args = [
            "-d",
            str(DATA_DIR),
            "-b",
            "some_bucket",
            "-s",
            "12345",
            "-e",
            "SmartSPIM",
            "-m",
            "OPHYS",
            "-a",
            "12/12/20225",
            "-t",
            "13:24:01",
            "-p",
            json.dumps(custom_endpoints),
        ]

        test_malformed_time_args = [
            "-d",
            str(DATA_DIR),
            "-b",
            "some_bucket",
            "-s",
            "12345",
            "-e",
            "SmartSPIM",
            "-m",
            "OPHYS",
            "-a",
            "10/10/2022",
            "-t",
            "121:20:20",
            "-p",
            json.dumps(custom_endpoints),
        ]

        basic_job_configs = BasicUploadJobConfigs.from_args(test_req_args)
        self.assertEqual("some_domain", basic_job_configs.codeocean_domain)
        self.assertEqual(
            "some_capsule_id", basic_job_configs.codeocean_trigger_capsule_id
        )
        self.assertEqual(
            "some_ms_domain", basic_job_configs.metadata_service_domain
        )
        self.assertEqual(
            "some_dtr_location",
            basic_job_configs.aind_data_transfer_repo_location,
        )
        self.assertEqual(
            "some_password",
            basic_job_configs.video_encryption_password.get_secret_value(),
        )
        self.assertEqual("some_bucket", basic_job_configs.s3_bucket)
        self.assertEqual(
            ExperimentType.SMARTSPIM, basic_job_configs.experiment_type
        )
        self.assertEqual(Modality.OPHYS, basic_job_configs.modality)
        self.assertEqual("12345", basic_job_configs.subject_id)
        self.assertEqual(
            date.fromisoformat("2022-10-10"),
            basic_job_configs.acq_date,
        )
        self.assertEqual(
            time.fromisoformat("13:24:01"),
            basic_job_configs.acq_time,
        )
        self.assertEqual(
            "SmartSPIM_12345_2022-10-10_13-24-01", basic_job_configs.s3_prefix
        )
        self.assertEqual(DATA_DIR, basic_job_configs.data_source)
        self.assertFalse(basic_job_configs.dry_run)
        self.assertIsNone(basic_job_configs.behavior_dir)
        self.assertIsNone(basic_job_configs.extra_configs)
        self.assertIsNone(basic_job_configs.metadata_dir)
        self.assertEqual("WARNING", basic_job_configs.log_level)
        self.assertFalse(mock_client.called)

        with self.assertRaises(ValueError):
            BasicUploadJobConfigs.from_args(test_malformed_date_args)

        with self.assertRaises(ValueError):
            BasicUploadJobConfigs.from_args(test_malformed_time_args)


if __name__ == "__main__":
    unittest.main()
