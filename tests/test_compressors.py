import os
import unittest
from pathlib import Path

import numpy as np
from numcodecs import Blosc
from wavpack_numcodecs import WavPack

from aind_data_transfer.readers import EphysReaders
from aind_data_transfer.transformations.compressors import (
    EphysCompressors,
    ImagingCompressors,
    VideoCompressor,
)

TEST_DIR = Path(os.path.dirname(os.path.realpath(__file__)))
RESOURCES_DIR = TEST_DIR / "resources"


class TestEphysCompressors(unittest.TestCase):
    def test_get_compressor(self):
        blosc_configs = {
            "cname": "zstd",
            "clevel": 9,
            "shuffle": Blosc.BITSHUFFLE,
        }
        wavpack_configs = {"level": 3}
        blosc = EphysCompressors.get_compressor(
            EphysCompressors.Compressors.blosc.name, **blosc_configs
        )
        wavpack = EphysCompressors.get_compressor(
            EphysCompressors.Compressors.wavpack.name, **wavpack_configs
        )
        expected_blosc = Blosc(
            cname="zstd", clevel=9, shuffle=Blosc.BITSHUFFLE
        )
        expected_wavpack = WavPack(level=3)
        self.assertEqual(blosc, expected_blosc)
        self.assertEqual(wavpack, expected_wavpack)

    def test_get_compressor_fails(self):
        with self.assertRaises(Exception):
            EphysCompressors.get_compressor("Made up name")

    def test_scale_recording(self):
        open_ephys_dir = (
            RESOURCES_DIR / "v0.6.x_neuropixels_multiexp_multistream"
        )
        openephys_reader = EphysReaders.Readers.openephys.name
        read_blocks = EphysReaders.get_read_blocks(
            openephys_reader, open_ephys_dir
        )
        read_block = next(read_blocks)
        chunk_size = min(read_block["recording"].get_num_frames(0) - 1, 10000)
        # The first read block should be a NI-DAQ recording.
        self.assertTrue(
            EphysReaders.RecordingBlockPrefixes.nidaq.value
            in read_block["stream_name"],
            "The first block isn't NI-DAQ. If the test data has "
            "been updated, this test needs to be updated too.",
        )
        # We expect the NI-DAQ scaled recordings to be the same as the original
        scaled_read_blocks = EphysCompressors.scale_read_blocks(
            [read_block], disable_tqdm=True, chunk_size=chunk_size
        )
        scaled_read_block = next(scaled_read_blocks)
        self.assertEqual(
            scaled_read_block["scaled_recording"], read_block["recording"]
        )

        read_block = next(read_blocks)
        # The second read block should be a Neuropixel recording.
        self.assertTrue(
            EphysReaders.RecordingBlockPrefixes.neuropix.value
            in read_block["stream_name"],
            "The second block isn't Neuropixel. If the test data "
            "has been updated, this test needs to be updated too.",
        )
        chunk_size = min(read_block["recording"].get_num_frames(0) - 1, 10000)
        scaled_read_blocks = EphysCompressors.scale_read_blocks(
            [read_block], disable_tqdm=True, chunk_size=chunk_size
        )
        scaled_read_block = next(scaled_read_blocks)

        # The lsb value should be 1 and the median values should be 0 in the
        # scaled neuropixel recordings.
        lsb_value, median_values = EphysCompressors._get_median_and_lsb(
            scaled_read_block["scaled_recording"],
            disable_tqdm=True,
            num_chunks_per_segment=10,
            chunk_size=chunk_size,
        )

        expected_median_values = np.zeros(
            shape=median_values.shape, dtype=median_values.dtype
        )

        self.assertEqual(lsb_value, 1)
        self.assertTrue(np.array_equal(expected_median_values, median_values))


class TestImagingCompressors(unittest.TestCase):
    def test_get_compressor(self):
        blosc_configs = {
            "cname": "zstd",
            "clevel": 1,
            "shuffle": Blosc.SHUFFLE,
        }
        blosc = ImagingCompressors.get_compressor(
            ImagingCompressors.Compressors.blosc.name, **blosc_configs
        )
        expected_blosc = Blosc(cname="zstd", clevel=1, shuffle=Blosc.SHUFFLE)
        self.assertEqual(blosc, expected_blosc)

    def test_get_compressor_fails(self):
        with self.assertRaises(Exception):
            ImagingCompressors.get_compressor("Made up name")


class TestVideoCompressor(unittest.TestCase):
    """Tests VideoCompressor class methods"""

    def test_video_compressor_creation(self):
        """Tests VideoCompressor default is set correctly"""
        video_compressor = VideoCompressor()
        self.assertEqual(video_compressor.compression_level, 5)


if __name__ == "__main__":
    unittest.main()
