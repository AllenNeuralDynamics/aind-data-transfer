"""Tests ephys configurations are set properly"""
import os
import unittest
from pathlib import Path
from unittest import mock

from numcodecs import Blosc

from aind_data_transfer.config_loader.ephys_configuration_loader import (
    EphysJobConfigurationLoader,
)

TEST_DIR = Path(os.path.dirname(os.path.realpath(__file__)))
CONFIGS_DIR = TEST_DIR / "resources" / "test_configs"


class TestEphysJobConfigs(unittest.TestCase):
    """Tests ephys job pipeline methods"""

    @mock.patch(
        "aind_data_transfer.config_loader.ephys_configuration_loader."
        "EphysJobConfigurationLoader._get_endpoints"
    )
    def test_conf_loads(self, mocked_get_endpoints):
        """Basic config loads test"""

        mocked_get_endpoints.return_value = {"codeocean_trigger_capsule": None}

        raw_data_dir = (
            "tests/resources/v0.6.x_neuropixels_multiexp_multistream"
        )
        dest_data_dir = (
            "tests/resources/new/v0.6.x_neuropixels_multiexp_multistream"
        )

        expected_configs = {
            "jobs": {
                "clip": True,
                "compress": True,
                "attach_metadata": False,
                "upload_to_s3": True,
                "upload_to_gcp": False,
                "trigger_codeocean_job": False,
            },
            "endpoints": {
                "raw_data_dir": raw_data_dir,
                "dest_data_dir": dest_data_dir,
                "s3_bucket": "some-s3-bucket",
                "s3_prefix": "v0.6.x_neuropixels_multiexp_multistream",
                "gcp_bucket": "some-gcp-bucket",
                "gcp_prefix": "test_20221001",
                "codeocean_domain": "https://acmecorp.codeocean.com",
                "code_repo_location": "https://location_of_code_repo",
                "metadata_service_url": "http://some-url",
            },
            "aws_secret_names": {
                "code_ocean_api_token_name": "secret_name_for_api_token",
                "region": "us-west-2",
                "video_encryption_password": "video_encryption_password",
            },
            "data": {"name": "openephys"},
            "clip_data_job": {
                "clip_kwargs": {},
            },
            "compress_data_job": {
                "write_kwargs": {
                    "n_jobs": -1,
                    "chunk_duration": "1s",
                    "progress_bar": True,
                },
                "format_kwargs": {},
                "compressor": {
                    "compressor_name": "blosc",
                    "kwargs": {"shuffle": Blosc.BITSHUFFLE},
                },
                "scale_params": {"chunk_size": 20},
            },
            "upload_data_job": {"dryrun": True},
            "trigger_codeocean_job": {
                "job_type": "openephys",
                "bucket": "some-s3-bucket",
                "prefix": "v0.6.x_neuropixels_multiexp_multistream",
            },
            "logging": {"level": "INFO"},
        }
        conf_file_path = CONFIGS_DIR / "ephys_upload_job_test_configs.yml"

        args = ["-c", str(conf_file_path)]

        loaded_configs = EphysJobConfigurationLoader().load_configs(args)
        self.assertEqual(loaded_configs, expected_configs)

    @mock.patch(
        "aind_data_transfer.config_loader.ephys_configuration_loader."
        "EphysJobConfigurationLoader._get_endpoints"
    )
    def test_endpoints_are_resolved(self, mocked_get_endpoints):
        """Tests default endpoints are resolved correctly"""

        mocked_get_endpoints.return_value = {"codeocean_trigger_capsule": None}

        raw_data_dir = "/some/random/folder/625463_2022-10-06_10-14-25"
        expected_configs = {
            "jobs": {
                "clip": True,
                "compress": True,
                "attach_metadata": False,
                "upload_to_s3": True,
                "upload_to_gcp": True,
                "trigger_codeocean_job": False,
            },
            "endpoints": {
                "raw_data_dir": raw_data_dir,
                "dest_data_dir": "ecephys_625463_2022-10-06_10-14-25",
                "s3_bucket": "some-s3-bucket",
                "s3_prefix": "ecephys_625463_2022-10-06_10-14-25",
                "gcp_bucket": "some-gcp-bucket",
                "gcp_prefix": "ecephys_625463_2022-10-06_10-14-25",
                "codeocean_domain": "https://acmecorp.codeocean.com",
                "code_repo_location": "https://location_of_code_repo",
                "metadata_service_url": "http://some-url",
            },
            "aws_secret_names": {
                "code_ocean_api_token_name": "secret_name_for_api_token",
                "region": "us-west-2",
                "video_encryption_password": "secret_name_for_vid_password",
            },
            "data": {"name": "openephys", "subject_id": "625463"},
            "clip_data_job": {"clip_kwargs": {}},
            "compress_data_job": {
                "write_kwargs": {
                    "n_jobs": -1,
                    "chunk_duration": "1s",
                    "progress_bar": True,
                },
                "format_kwargs": {},
                "compressor": {
                    "compressor_name": "wavpack",
                    "kwargs": {"level": 3},
                },
                "scale_params": {},
            },
            "upload_data_job": {"dryrun": True},
            "trigger_codeocean_job": {
                "job_type": "openephys",
                "bucket": "some-s3-bucket",
                "prefix": "ecephys_625463_2022-10-06_10-14-25",
            },
            "logging": {"level": "INFO"},
        }

        conf_file_path1 = CONFIGS_DIR / "example_configs_src_pattern1.yml"
        conf_file_path2 = CONFIGS_DIR / "example_configs_src_pattern2.yml"
        args1 = ["-c", str(conf_file_path1)]
        args2 = ["-c", str(conf_file_path2)]
        loaded_configs1 = EphysJobConfigurationLoader().load_configs(args1)
        loaded_configs2 = EphysJobConfigurationLoader().load_configs(args2)
        self.assertEqual(loaded_configs1, expected_configs)
        self.assertEqual(loaded_configs2, expected_configs)


if __name__ == "__main__":
    unittest.main()
