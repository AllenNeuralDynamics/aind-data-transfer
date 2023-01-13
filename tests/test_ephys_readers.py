"""Unit tests for ephys readers module."""
import os
import unittest
from pathlib import Path

from aind_data_transfer.readers.ephys_readers import EphysReaders

TEST_DIR = Path(os.path.dirname(os.path.realpath(__file__)))
RESOURCES_DIR = TEST_DIR / "resources"


class TestEphysReaders(unittest.TestCase):

    open_ephys_dir = RESOURCES_DIR / "v0.6.x_neuropixels_multiexp_multistream"

    nidaq_rec_prefix = (
        "'recording': OpenEphysBinaryRecordingExtractor: 8 channels - 1 "
        "segments - 30.0kHz - 0.003s, 'experiment_name': 'experiment"
    )
    nidaq_rec_suffix = (
        "'stream_name': 'Record Node 101#NI-DAQmx-103.PXIe-6341'"
    )

    neuropix_rec_prefix = (
        "'recording': OpenEphysBinaryRecordingExtractor: 384 channels - 1 "
        "segments - 30.0kHz - 0.003s, 'experiment_name': 'experiment"
    )
    neuropix_rec_suffix = (
        "'stream_name': 'Record Node 101#Neuropix-PXI-100.Probe"
    )

    expected_read_blocks_info = [
        f"{{{nidaq_rec_prefix}1', {nidaq_rec_suffix}}}",
        f"{{{neuropix_rec_prefix}1', {neuropix_rec_suffix}B'}}",
        f"{{{neuropix_rec_prefix}1', {neuropix_rec_suffix}C'}}",
        f"{{{nidaq_rec_prefix}3', {nidaq_rec_suffix}}}",
        f"{{{neuropix_rec_prefix}3', {neuropix_rec_suffix}B'}}",
        f"{{{neuropix_rec_prefix}3', {neuropix_rec_suffix}C'}}",
        f"{{{nidaq_rec_prefix}6', {nidaq_rec_suffix}}}",
        f"{{{neuropix_rec_prefix}6', {neuropix_rec_suffix}B'}}",
        f"{{{neuropix_rec_prefix}6', {neuropix_rec_suffix}C'}}",
    ]

    def test_read(self):
        openephys_reader = EphysReaders.Readers.openephys.name
        read_blocks = EphysReaders.get_read_blocks(
            openephys_reader, self.open_ephys_dir
        )
        read_blocks_info = [str(r) for r in read_blocks]
        self.assertEqual(self.expected_read_blocks_info, read_blocks_info)

    def test_read_fails(self):
        with self.assertRaises(Exception):
            list(
                EphysReaders.get_read_blocks(
                    "Made up readers", self.open_ephys_dir
                )
            )


if __name__ == "__main__":
    unittest.main()
