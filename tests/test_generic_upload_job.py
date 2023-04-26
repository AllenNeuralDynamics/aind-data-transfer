"""Test module for generic upload job."""

import json
import os
import unittest
from pathlib import Path
from unittest.mock import MagicMock, call, patch

from aind_data_transfer.jobs.s3_upload_job import GenericS3UploadJobList

TEST_DIR = Path(os.path.dirname(os.path.realpath(__file__)))
CONFIGS_DIR = TEST_DIR / "resources" / "test_configs"
METADATA_DIR = TEST_DIR / "resources" / "test_metadata"


class TestGenericS3UploadJobList(unittest.TestCase):
    """Unit tests for methods in GenericS3UploadJobs class."""

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

    PATH_TO_EXAMPLE_CSV_FILE = (
        Path(os.path.dirname(os.path.realpath(__file__)))
        / "resources"
        / "test_configs"
        / "jobs_list.csv"
    )

    PATH_TO_EXAMPLE_CSV_FILE2 = (
        Path(os.path.dirname(os.path.realpath(__file__)))
        / "resources"
        / "test_configs"
        / "jobs_list2.csv"
    )

    @patch("boto3.client")
    @patch("aind_data_transfer.jobs.basic_job.BasicJob.run_job")
    def test_load_configs(
        self, mock_run_job: MagicMock, mock_client: MagicMock
    ) -> None:
        """Tests configs are loaded correctly."""
        mock_resp1 = {
            "Parameter": {"Value": self.EXAMPLE_PARAM_STORE_RESPONSE}
        }
        mock_resp2 = {
            "Parameter": {"Value": json.dumps({"password": "some_password"})}
        }
        mock_resp3 = {
            "Parameter": {
                "Value": json.dumps(
                    {"CODEOCEAN_READWRITE_TOKEN": "some_token"}
                )
            }
        }

        mock_client.return_value.get_parameter.side_effect = [
            mock_resp1,
            mock_resp2,
            mock_resp3,
            mock_resp1,
            mock_resp2,
            mock_resp3,
        ]

        args = ["-j", str(self.PATH_TO_EXAMPLE_CSV_FILE)]
        dry_run_args = [
            "-j",
            str(self.PATH_TO_EXAMPLE_CSV_FILE),
            "--dry-run",
            "--compress-raw-data",
        ]
        jobs = GenericS3UploadJobList(args=args)
        dry_run_jobs = GenericS3UploadJobList(args=dry_run_args)

        self.assertFalse(jobs.job_list[0].job_configs.dry_run)
        self.assertTrue(dry_run_jobs.job_list[0].job_configs.dry_run)
        self.assertTrue(jobs.job_list[0].job_configs.compress_raw_data)
        self.assertTrue(dry_run_jobs.job_list[0].job_configs.compress_raw_data)
        jobs.run_job()
        dry_run_jobs.run_job()

        mock_client.assert_has_calls(
            [
                call("ssm"),
                call().get_parameter(
                    Name="/some/aws/param/store/name", WithDecryption=False
                ),
                call().close(),
                call("ssm"),
                call().get_parameter(
                    Name="/path/to/password", WithDecryption=True
                ),
                call().close(),
                call("ssm"),
                call().get_parameter(
                    Name="/path/to/token", WithDecryption=True
                ),
                call().close(),
                call("ssm"),
                call().get_parameter(
                    Name="/some/aws/param/store/name", WithDecryption=False
                ),
                call().close(),
                call("ssm"),
                call().get_parameter(
                    Name="/path/to/password", WithDecryption=True
                ),
                call().close(),
                call("ssm"),
                call().get_parameter(
                    Name="/path/to/token", WithDecryption=True
                ),
                call().close(),
            ]
        )
        mock_run_job.assert_has_calls([call(), call(), call(), call()])
        self.assertEqual(2, len(jobs.job_list))
        self.assertEqual(2, len(dry_run_jobs.job_list))

    @patch("boto3.client")
    @patch("aind_data_transfer.jobs.basic_job.BasicJob.run_job")
    def test_load_configs2(
        self, mock_run_job: MagicMock, mock_client: MagicMock
    ) -> None:
        """Tests configs are loaded correctly."""
        mock_resp1 = {
            "Parameter": {"Value": self.EXAMPLE_PARAM_STORE_RESPONSE}
        }
        mock_resp2 = {
            "Parameter": {"Value": json.dumps({"password": "some_password"})}
        }
        mock_resp3 = {
            "Parameter": {
                "Value": json.dumps(
                    {"CODEOCEAN_READWRITE_TOKEN": "some_token"}
                )
            }
        }

        mock_client.return_value.get_parameter.side_effect = [
            mock_resp1,
            mock_resp2,
            mock_resp3,
        ]

        args = ["-j", str(self.PATH_TO_EXAMPLE_CSV_FILE2)]
        jobs = GenericS3UploadJobList(args=args)

        # Default compress_raw_data should be false for non-ephys jobs
        self.assertFalse(jobs.job_list[0].job_configs.dry_run)
        self.assertFalse(jobs.job_list[0].job_configs.compress_raw_data)
        # Default compress_raw_data should be true for ephys data
        self.assertFalse(jobs.job_list[1].job_configs.dry_run)
        self.assertTrue(jobs.job_list[1].job_configs.compress_raw_data)
        self.assertTrue(jobs.job_list[2].job_configs.dry_run)
        self.assertTrue(jobs.job_list[2].job_configs.compress_raw_data)
        self.assertFalse(jobs.job_list[3].job_configs.dry_run)
        self.assertTrue(jobs.job_list[3].job_configs.compress_raw_data)
        jobs.run_job()
        # There are three jobs defined in the csv file
        mock_run_job.assert_has_calls([call(), call(), call(), call()])


if __name__ == "__main__":
    unittest.main()
