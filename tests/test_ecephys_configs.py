"""Tests methods in ecephys_config module"""
import os
import unittest
from pathlib import Path

from aind_data_schema.models.modalities import Modality
from aind_data_schema.models.process_names import ProcessName

from aind_data_transfer.readers.ephys_readers import DataReader
from aind_data_transfer.transformations.ephys_compressors import (
    CompressorName,
    EcephysCompressionParameters,
)

TEST_DIR = Path(os.path.dirname(os.path.realpath(__file__))) / "resources"
DATA_DIR = TEST_DIR / "v0.6.x_neuropixels_multiexp_multistream"


class TestEcephysUploadJobConfigs(unittest.TestCase):
    """Tests methods in EcephysUploadJobEndpoints class."""

    def test_defaults(self):
        """Tests defaults are set correctly."""
        ecephys_configs = EcephysCompressionParameters(source=DATA_DIR)

        self.assertEqual(100, ecephys_configs.clip_n_frames)
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
        self.assertEqual(DATA_DIR, ecephys_configs.source)
        self.assertEqual(None, ecephys_configs.extra_configs)
        self.assertEqual(Modality.ECEPHYS, ecephys_configs.modality)
        self.assertEqual(
            ProcessName.EPHYS_PREPROCESSING, ecephys_configs.process_name
        )
        self.assertEqual(10000, ecephys_configs.scale_chunk_size)
        self.assertEqual(100, ecephys_configs.scale_num_chunks_per_segment)


if __name__ == "__main__":
    unittest.main()
