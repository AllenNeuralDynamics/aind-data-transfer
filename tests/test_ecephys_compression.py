"""Tests for ecephys_job module."""

import os
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
import spikeinterface.extractors as se

from aind_data_transfer.transformations.ephys_compressors import (
    EcephysCompressionParameters,
    EphysCompressors,
)

TEST_DIR = Path(os.path.dirname(os.path.realpath(__file__))) / "resources"
DATA_DIR = TEST_DIR / "v0.6.x_neuropixels_multiexp_multistream"
BEHAVIOR_DIR = TEST_DIR / "v0.6.x_neuropixels_multiexp_multistream" / "Videos"


class TestEcephysCompression(unittest.TestCase):
    """Tests for EcephysJob class"""

    def test_ecephys_job_with_compression(
        self,
    ):
        """Tests ecephys job runs correctly with compression"""

        with TemporaryDirectory() as tmp_dir:
            tmp_dir = Path(tmp_dir)
            ecephys_configs = EcephysCompressionParameters(source=DATA_DIR)
            ecephys_compressor = EphysCompressors(
                job_configs=ecephys_configs, behavior_dir=BEHAVIOR_DIR
            )
            ecephys_compressor.compress_raw_data(
                temp_dir=tmp_dir / "some_path"
            )

            # Check if the compressed data is written
            assert (tmp_dir / "some_path" / "ecephys_clipped").is_dir()
            assert (tmp_dir / "some_path" / "ecephys_compressed").is_dir()
            # Check that all streams and blocks are compressed
            num_blocks = se.get_neo_num_blocks("openephys", DATA_DIR)
            stream_names, _ = se.get_neo_streams("openephys", DATA_DIR)
            for stream_name in stream_names:
                written_zarr_folders_for_stream = [
                    p
                    for p in (
                        tmp_dir / "some_path" / "ecephys_compressed"
                    ).iterdir()
                    if stream_name in p.name
                ]
                assert len(written_zarr_folders_for_stream) == num_blocks


if __name__ == "__main__":
    unittest.main()
