"""Tests for ecephys_job module."""

import os
import unittest
from pathlib import Path
from unittest.mock import MagicMock, call, patch

from aind_data_transfer.config_loader.ecephys_config import (
    EcephysUploadJobConfigs,
)
from aind_data_transfer.jobs.ecephys_job import EcephysJob

TEST_DIR = Path(os.path.dirname(os.path.realpath(__file__))) / "resources"
DATA_DIR = TEST_DIR / "v0.6.x_neuropixels_multiexp_multistream"
BEHAVIOR_DIR = TEST_DIR / "v0.6.x_neuropixels_multiexp_multistream" / "Videos"


class TestEcephysJob(unittest.TestCase):
    """Tests for EcephysJob class"""

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
    @patch("shutil.copytree")
    @patch("shutil.ignore_patterns")
    @patch("numpy.memmap")
    @patch("aind_data_transfer.jobs.ecephys_job.upload_to_s3")
    @patch(
        "aind_data_transfer.readers.ephys_readers.EphysReaders."
        "get_streams_to_clip"
    )
    @patch(
        "aind_data_transfer.readers.ephys_readers.EphysReaders."
        "get_read_blocks"
    )
    @patch(
        "aind_data_transfer.transformations.ephys_compressors."
        "EphysCompressors.get_compressor"
    )
    @patch(
        "aind_data_transfer.transformations.ephys_compressors."
        "EphysCompressors.scale_read_blocks"
    )
    @patch(
        "aind_data_transfer.writers.ephys_writers.EphysWriters."
        "compress_and_write_block"
    )
    @patch(
        "aind_data_transfer.jobs.ecephys_job."
        "correct_np_opto_electrode_locations"
    )
    def test_ecephys_job_no_compression(
        self,
        mock_correct_np_opto: MagicMock,
        mock_write_block: MagicMock,
        mock_scale_read_blocks: MagicMock,
        mock_get_compressor: MagicMock,
        mock_get_read_blocks: MagicMock,
        mock_get_streams_to_clip: MagicMock,
        mock_upload_to_s3: MagicMock,
        mock_memmap: MagicMock,
        mock_ignore_patterns: MagicMock,
        mock_copytree: MagicMock,
    ):
        """Tests ecephys job runs correctly with no compression"""
        ecephys_configs = EcephysUploadJobConfigs()
        ecephys_configs.compress_raw_data = False
        ecephys_job = EcephysJob(job_configs=ecephys_configs)
        ecephys_job._compress_raw_data(temp_dir=Path("some_path"))
        # Upload raw data with behavior directory defined
        ecephys_configs.behavior_dir = BEHAVIOR_DIR
        ecephys_job._compress_raw_data(temp_dir=Path("some_path"))

        mock_correct_np_opto.assert_has_calls([call(DATA_DIR), call(DATA_DIR)])
        mock_upload_to_s3.assert_has_calls(
            [
                call(
                    directory_to_upload=DATA_DIR,
                    s3_bucket="some_bucket",
                    s3_prefix="ecephys_12345_2020-10-10_05-10-10/ecephys",
                    dryrun=False,
                    excluded=None,
                ),
                call(
                    directory_to_upload=DATA_DIR,
                    s3_bucket="some_bucket",
                    s3_prefix="ecephys_12345_2020-10-10_05-10-10/ecephys",
                    dryrun=False,
                    excluded=(BEHAVIOR_DIR / "*"),
                ),
            ]
        )
        self.assertFalse(mock_write_block.called)
        self.assertFalse(mock_scale_read_blocks.called)
        self.assertFalse(mock_get_compressor.called)
        self.assertFalse(mock_get_read_blocks.called)
        self.assertFalse(mock_get_streams_to_clip.called)
        self.assertFalse(mock_memmap.called)
        self.assertFalse(mock_ignore_patterns.called)
        self.assertFalse(mock_copytree.called)

    @patch.dict(os.environ, EXAMPLE_ENV_VAR1, clear=True)
    @patch("shutil.copytree")
    @patch("shutil.ignore_patterns")
    @patch("aind_data_transfer.jobs.ecephys_job.memmap")
    @patch("aind_data_transfer.jobs.ecephys_job.upload_to_s3")
    @patch(
        "aind_data_transfer.readers.ephys_readers.EphysReaders."
        "get_streams_to_clip"
    )
    @patch(
        "aind_data_transfer.readers.ephys_readers.EphysReaders."
        "get_read_blocks"
    )
    @patch(
        "aind_data_transfer.transformations.ephys_compressors."
        "EphysCompressors.get_compressor"
    )
    @patch(
        "aind_data_transfer.transformations.ephys_compressors."
        "EphysCompressors.scale_read_blocks"
    )
    @patch(
        "aind_data_transfer.writers.ephys_writers.EphysWriters."
        "compress_and_write_block"
    )
    @patch(
        "aind_data_transfer.jobs.ecephys_job."
        "correct_np_opto_electrode_locations"
    )
    def test_ecephys_job_with_compression(
        self,
        mock_correct_np_opto: MagicMock,
        mock_write_block: MagicMock,
        mock_scale_read_blocks: MagicMock,
        mock_get_compressor: MagicMock,
        mock_get_read_blocks: MagicMock,
        mock_get_streams_to_clip: MagicMock,
        mock_upload_to_s3: MagicMock,
        mock_memmap: MagicMock,
        mock_ignore_patterns: MagicMock,
        mock_copytree: MagicMock,
    ):
        """Tests ecephys job runs correctly with compression"""
        mock_get_streams_to_clip.return_value.__iter__.return_value = [
            {"data": [], "relative_path_name": "some_rel_path", "n_chan": 1}
        ]
        mock_ignore_patterns.side_effect = [
            ["*.dat"],
            ["*.dat", str(BEHAVIOR_DIR / "*")],
        ]
        mock_get_compressor.return_value = "Mocked Compressor"
        mock_get_read_blocks.return_value = {
            "recording": "mocked recording",
            "experiment_name": "mocked exp name",
            "stream_name": "mocked stream name",
        }
        mock_scale_read_blocks.return_value = {
            "scaled_recording": "mocked scale rec",
            "experiment_name": "mocked exp name",
            "stream_name": "mocked stream name",
        }

        ecephys_configs = EcephysUploadJobConfigs()
        ecephys_job = EcephysJob(job_configs=ecephys_configs)
        ecephys_job._compress_raw_data(temp_dir=Path("some_path"))
        # Test when a behavior directory is defined
        ecephys_configs.behavior_dir = BEHAVIOR_DIR
        ecephys_job._compress_raw_data(temp_dir=Path("some_path"))

        self.assertFalse(mock_upload_to_s3.called)
        mock_correct_np_opto.assert_has_calls([call(DATA_DIR), call(DATA_DIR)])
        mock_copytree.assert_has_calls(
            [
                call(
                    DATA_DIR,
                    Path("some_path/ecephys_clipped"),
                    ignore=["*.dat"],
                ),
                call(
                    DATA_DIR,
                    Path("some_path/ecephys_clipped"),
                    ignore=["*.dat", str(BEHAVIOR_DIR / "*")],
                ),
            ]
        )
        mock_get_streams_to_clip.assert_has_calls(
            [
                call("openephys", DATA_DIR),
                call().__iter__(),
                call("openephys", DATA_DIR),
                call().__iter__(),
            ]
        )
        mock_memmap.assert_has_calls(
            [
                call(
                    Path("some_path/ecephys_clipped/some_rel_path"),
                    dtype="int16",
                    shape=(100, 1),
                    order="C",
                    mode="w+",
                ),
                call().__setitem__(slice(None, None, None), []),
                call(
                    Path("some_path/ecephys_clipped/some_rel_path"),
                    dtype="int16",
                    shape=(100, 1),
                    order="C",
                    mode="w+",
                ),
                call().__setitem__(slice(None, None, None), []),
            ]
        )
        mock_get_read_blocks.assert_has_calls(
            [call("openephys", DATA_DIR), call("openephys", DATA_DIR)]
        )

        mock_get_compressor.assert_has_calls(
            [call("wavpack", level=3), call("wavpack", level=3)]
        )
        mock_scale_read_blocks.assert_has_calls(
            [
                call(
                    read_blocks={
                        "recording": "mocked recording",
                        "experiment_name": "mocked exp name",
                        "stream_name": "mocked stream name",
                    },
                    num_chunks_per_segment=100,
                    chunk_size=10000,
                ),
                call(
                    read_blocks={
                        "recording": "mocked recording",
                        "experiment_name": "mocked exp name",
                        "stream_name": "mocked stream name",
                    },
                    num_chunks_per_segment=100,
                    chunk_size=10000,
                ),
            ]
        )
        mock_write_block.assert_has_calls(
            [
                call(
                    read_blocks={
                        "scaled_recording": "mocked scale rec",
                        "experiment_name": "mocked exp name",
                        "stream_name": "mocked stream name",
                    },
                    compressor="Mocked Compressor",
                    output_dir=Path("some_path/ecephys_compressed"),
                    max_windows_filename_len=150,
                    output_format="zarr",
                    job_kwargs={"n_jobs": -1},
                ),
                call(
                    read_blocks={
                        "scaled_recording": "mocked scale rec",
                        "experiment_name": "mocked exp name",
                        "stream_name": "mocked stream name",
                    },
                    compressor="Mocked Compressor",
                    output_dir=Path("some_path/ecephys_compressed"),
                    max_windows_filename_len=150,
                    output_format="zarr",
                    job_kwargs={"n_jobs": -1},
                ),
            ]
        )


if __name__ == "__main__":
    unittest.main()
