import os
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from numcodecs import Blosc
from wavpack_numcodecs import WavPack

from aind_data_transfer.readers.ephys_readers import EphysReaders, DataReader
from aind_data_transfer.transformations.ephys_compressors import (
    EphysCompressors,
    CompressorName,
)
from aind_data_transfer.transformations.generic_compressors import (
    VideoCompressor,
    ZipCompressor,
)
from aind_data_transfer.transformations.imaging_compressors import (
    ImagingCompressors,
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
            CompressorName.BLOSC.value, **blosc_configs
        )
        wavpack = EphysCompressors.get_compressor(
            CompressorName.WAVPACK.value, **wavpack_configs
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
        openephys_reader = DataReader.OPENEPHYS.value
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
            [read_block], chunk_size=chunk_size
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
            [read_block], chunk_size=chunk_size
        )
        scaled_read_block = next(scaled_read_blocks)


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


class TestGenericCompressor(unittest.TestCase):
    """Tests VideoCompressor class methods"""

    def test_video_compressor_creation(self):
        """Tests VideoCompressor default is set correctly"""
        video_compressor = VideoCompressor()
        self.assertEqual(video_compressor.compression_level, 5)

    @patch("pyminizip.compress_multiple")
    def test_zip_compressor(self, mock_zip: MagicMock):
        """Tests zip compression. No subdirectories or files skipped."""
        zc = ZipCompressor()
        input_dir = (
            RESOURCES_DIR
            / "v0.6.x_neuropixels_multiexp_multistream"
            / "Videos"
        )
        output_dir = RESOURCES_DIR
        file_names = [
            str(input_dir / "a_video_file.avi"),
            str(input_dir / "another_video_file.avi"),
            str(input_dir / "video_ref.csv"),
        ]
        prefix_names = [str("Videos"), str("Videos"), str("Videos")]
        zc.compress_dir(input_dir=input_dir, output_dir=output_dir)
        file_names_call = mock_zip.mock_calls[0].args[0]
        prefix_names_call = mock_zip.mock_calls[0].args[1]
        zipped_actual_calls = zip(file_names_call, prefix_names_call)
        zipped_expected_calls = zip(file_names, prefix_names)
        output_dir_call = mock_zip.mock_calls[0].args[2]
        self.assertEqual(output_dir_call, str(RESOURCES_DIR))
        self.assertEqual(set(zipped_expected_calls), set(zipped_actual_calls))

    @patch("pyminizip.compress_multiple")
    def test_zip_compressor_skip_dir(self, mock_zip: MagicMock):
        """Tests zip compression. One file skipped."""
        zc = ZipCompressor()
        input_dir = (
            RESOURCES_DIR
            / "v0.6.x_neuropixels_multiexp_multistream"
            / "Videos"
        )
        skip_file = (
            RESOURCES_DIR
            / "v0.6.x_neuropixels_multiexp_multistream"
            / "Videos"
            / "a_video_file.avi"
        )
        output_dir = RESOURCES_DIR
        file_names = [
            str(input_dir / "another_video_file.avi"),
            str(input_dir / "video_ref.csv"),
        ]
        prefix_names = [str("Videos"), str("Videos")]
        zc.compress_dir(
            input_dir=input_dir, output_dir=output_dir, skip_dirs=[skip_file]
        )
        file_names_call = mock_zip.mock_calls[0].args[0]
        prefix_names_call = mock_zip.mock_calls[0].args[1]
        zipped_actual_calls = zip(file_names_call, prefix_names_call)
        zipped_expected_calls = zip(file_names, prefix_names)
        output_dir_call = mock_zip.mock_calls[0].args[2]
        self.assertEqual(output_dir_call, str(RESOURCES_DIR))
        self.assertEqual(set(zipped_expected_calls), set(zipped_actual_calls))


if __name__ == "__main__":
    unittest.main()
