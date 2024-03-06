"""Unit tests for ephys readers module."""

import os
import unittest
from pathlib import Path
import numpy as np

from aind_data_transfer.readers.ephys_readers import EphysReaders, DataReader

TEST_DIR = Path(os.path.dirname(os.path.realpath(__file__)))
RESOURCES_DIR = TEST_DIR / "resources"


class TestEphysReaders(unittest.TestCase):
    open_ephys_dir = RESOURCES_DIR / "v0.6.x_neuropixels_multiexp_multistream"

    nidaq_rec_prefix = (
        "'recording': OpenEphysBinaryRecordingExtractor: "
        "8 channels - 30.0kHz - 1 segments - 100 samples \n"
        "                                   0.00s (3.33 ms) - int16 dtype - 1.56 KiB, "
        "'experiment_name': 'experiment"
    )
    nidaq_rec_suffix = (
        "'stream_name': 'Record Node 101#NI-DAQmx-103.PXIe-6341'"
    )

    neuropix_rec_prefix = (
        "'recording': OpenEphysBinaryRecordingExtractor: "
        "384 channels - 30.0kHz - 1 segments - 100 samples \n"
        "                                   0.00s (3.33 ms) - int16 dtype - 75.00 KiB, "
        "'experiment_name': 'experiment"
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
        openephys_reader = DataReader.OPENEPHYS.value
        read_blocks = EphysReaders.get_read_blocks(
            openephys_reader, self.open_ephys_dir
        )
        read_blocks_info = [str(r) for r in read_blocks]
        self.assertEqual(self.expected_read_blocks_info, read_blocks_info)

    def test_sync_timestamps(self):
        openephys_reader = DataReader.OPENEPHYS.value
        read_blocks = EphysReaders.get_read_blocks(
            openephys_reader, self.open_ephys_dir
        )
        for read_block in read_blocks:
            record_node, oe_stream = read_block["stream_name"].split("#")
            experiment = read_block["experiment_name"]
            sync_timestamps_file = (
                self.open_ephys_dir
                / record_node
                / experiment
                / "recording1"
                / "continuous"
                / oe_stream
                / "timestamps.npy"
            )
            sync_timestamps = np.load(sync_timestamps_file)
            np.testing.assert_array_equal(
                read_block["recording"].get_times(), sync_timestamps
            )

    def test_read_fails(self):
        with self.assertRaises(Exception):
            list(
                EphysReaders.get_read_blocks(
                    "Made up readers", self.open_ephys_dir
                )
            )


if __name__ == "__main__":
    unittest.main()
