"""Tests methods in ecephys_config module"""
import os
import unittest
from datetime import date, time
from pathlib import Path
from unittest.mock import MagicMock, patch

from aind_data_schema.data_description import ExperimentType, Modality
from aind_data_schema.processing import ProcessName

from aind_data_transfer.config_loader.ecephys_config import (
    EcephysUploadJobConfigs,
)
from aind_data_transfer.readers.ephys_readers import DataReader
from aind_data_transfer.transformations.ephys_compressors import CompressorName

TEST_DIR = Path(os.path.dirname(os.path.realpath(__file__))) / "resources"
DATA_DIR = TEST_DIR / "v0.6.x_neuropixels_multiexp_multistream"


class TestEcephysUploadJobConfigs(unittest.TestCase):
    """Tests methods in EcephysUploadJobEndpoints class."""

    EXAMPLE_ENV_VAR1 = {
        "S3_BUCKET": "some_bucket",
        "MODALITY": "ecephys",
        "SUBJECT_ID": "12345",
        "ACQ_DATE": "2020-10-10",
        "ACQ_TIME": "05:10:10",
        "DATA_SOURCE": str(DATA_DIR),
        "CODEOCEAN_DOMAIN": "some_domain",
        "CODEOCEAN_API_TOKEN": "some_token",
        "CODEOCEAN_TRIGGER_CAPSULE_ID": "some_capsule_id",
        "METADATA_SERVICE_DOMAIN": "some_ms_domain",
        "AIND_DATA_TRANSFER_REPO_LOCATION": "some_dtr_location",
        "VIDEO_ENCRYPTION_PASSWORD": "some_password",
    }

    @patch.dict(os.environ, EXAMPLE_ENV_VAR1, clear=True)
    @patch("boto3.client")
    def test_defaults(self, mock_client: MagicMock):
        """Tests defaults are set correctly."""
        ecephys_configs = EcephysUploadJobConfigs()
        self.assertEqual(
            date.fromisoformat("2020-10-10"), ecephys_configs.acq_date
        )
        self.assertEqual(
            time.fromisoformat("05:10:10"), ecephys_configs.acq_time
        )
        self.assertEqual(
            "some_dtr_location",
            ecephys_configs.aind_data_transfer_repo_location,
        )
        self.assertEqual(None, ecephys_configs.aws_param_store_name)
        self.assertEqual(None, ecephys_configs.behavior_dir)
        self.assertEqual(100, ecephys_configs.clip_n_frames)
        self.assertEqual(
            "some_token",
            ecephys_configs.codeocean_api_token.get_secret_value(),
        )
        self.assertEqual("some_domain", ecephys_configs.codeocean_domain)
        self.assertEqual(
            "some_capsule_id", ecephys_configs.codeocean_trigger_capsule_id
        )
        self.assertEqual(
            None, ecephys_configs.codeocean_trigger_capsule_version
        )
        self.assertEqual("1s", ecephys_configs.compress_chunk_duration)
        self.assertEqual(
            {"n_jobs": -1}, ecephys_configs.compress_job_save_kwargs
        )
        self.assertEqual(
            150, ecephys_configs.compress_max_windows_filename_len
        )
        self.assertTrue(ecephys_configs.compress_raw_data)
        self.assertEqual("zarr", ecephys_configs.compress_write_output_format)
        self.assertEqual({"level": 3}, ecephys_configs.compressor_kwargs)
        self.assertEqual(
            CompressorName.WAVPACK, ecephys_configs.compressor_name
        )
        self.assertEqual(DataReader.OPENEPHYS, ecephys_configs.data_reader)
        self.assertEqual(DATA_DIR, ecephys_configs.data_source)
        self.assertEqual(False, ecephys_configs.dry_run)
        self.assertEqual(
            ExperimentType.ECEPHYS, ecephys_configs.experiment_type
        )
        self.assertEqual(None, ecephys_configs.extra_configs)
        self.assertEqual("WARNING", ecephys_configs.log_level)
        self.assertEqual(None, ecephys_configs.metadata_dir)
        self.assertEqual(False, ecephys_configs.metadata_dir_force)
        self.assertEqual(
            "some_ms_domain", ecephys_configs.metadata_service_domain
        )
        self.assertEqual(Modality.ECEPHYS, ecephys_configs.modality)
        self.assertEqual(
            ProcessName.EPHYS_PREPROCESSING, ecephys_configs.process_name
        )
        self.assertEqual("some_bucket", ecephys_configs.s3_bucket)
        self.assertEqual(10000, ecephys_configs.scale_chunk_size)
        self.assertEqual(100, ecephys_configs.scale_num_chunks_per_segment)
        self.assertEqual("12345", ecephys_configs.subject_id)
        self.assertEqual(None, ecephys_configs.temp_directory)
        self.assertEqual(
            "some_password",
            ecephys_configs.video_encryption_password.get_secret_value(),
        )

        mock_client.assert_not_called()


if __name__ == "__main__":
    unittest.main()
