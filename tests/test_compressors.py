import unittest
from transfer.compressors import EphysCompressors
from transfer.readers import EphysReaders
from numcodecs import Blosc
from wavpack_numcodecs import WavPack
from pathlib import Path
import os

TEST_DIR = Path(os.path.dirname(os.path.realpath(__file__)))
RESOURCES_DIR = TEST_DIR / "resources"


class TestEphysCompressors(unittest.TestCase):

    def test_get_compressor(self):
        blosc_configs = {'cname': 'zstd',
                         'clevel': 9,
                         'shuffle': Blosc.BITSHUFFLE}
        wavpack_configs = {'level': 3}
        blosc = EphysCompressors.get_compressor(
            EphysCompressors.Compressors.blosc.name, **blosc_configs)
        wavpack = EphysCompressors.get_compressor(
            EphysCompressors.Compressors.wavpack.name, **wavpack_configs)
        expected_blosc = Blosc(cname='zstd',
                               clevel=9,
                               shuffle=Blosc.BITSHUFFLE)
        expected_wavpack = WavPack(level=3)
        self.assertEqual(blosc, expected_blosc)
        self.assertEqual(wavpack, expected_wavpack)

    def test_get_compressor_fails(self):
        with self.assertRaises(Exception):
            EphysCompressors.get_compressor(
                "Made up name")

    def test_scale_recording(self):
        open_ephys_dir = (
                RESOURCES_DIR / "v0.6.x_neuropixels_multiexp_multistream"
        )
        openephys_reader = EphysReaders.Readers.openephys.name
        read_blocks = EphysReaders.get_read_blocks(openephys_reader,
                                                   open_ephys_dir)

        read_block = next(read_blocks)
        expected_lsb_value = 7
        expected_median_values_shape = (384,)
        expected_median_values_first = 194
        expected_median_values_last = 338
        lsb_value, median_values = (
            EphysCompressors._get_median_and_lsb(read_block['recording'],
                                                 disable_tqdm=True,
                                                 num_random_chunks=2,
                                                 num_chunks_per_segment=1,
                                                 chunk_size=20)
        )
        scaled_read_blocks = (
            EphysCompressors.scale_read_blocks([read_block], disable_tqdm=True)
        )
        scaled_read_block_str = str(next(scaled_read_blocks))
        # Maybe there's a better way to test rather than comparing strings?
        expected_scaled_read_block_str = (
            "{'scaled_recording': ScaleRecording: 384 channels - 1 segments - "
            "30.0kHz - 0.003s, 'block_index': 0, 'stream_name': 'Record Node "
            "101#Neuropix-PXI-100.ProbeB'}"
        )
        self.assertEqual(lsb_value, expected_lsb_value)
        self.assertEqual(expected_median_values_shape, median_values.shape)
        self.assertEqual(expected_median_values_first, median_values[0])
        self.assertEqual(expected_median_values_last, median_values[-1])
        self.assertEqual(expected_scaled_read_block_str,
                         str(scaled_read_block_str))


if __name__ == "__main__":
    unittest.main()
