import os
import unittest
from pathlib import Path
from transfer.readers import EphysReaders

TEST_DIR = Path(os.path.dirname(os.path.realpath(__file__)))
RESOURCES_DIR = TEST_DIR / "resources"


class TestEphysReaders(unittest.TestCase):

    open_ephys_dir = RESOURCES_DIR / "v0.6.x_neuropixels_multiexp_multistream"
    rec_string = ("'recording': OpenEphysBinaryRecordingExtractor: "
                  "384 channels - 1 segments - 30.0kHz - 0.003s")
    stream_prefix = "'stream_name': 'Record Node 101#Neuropix-PXI-100.Probe"
    expected_read_blocks_info = (
        [f"{{{rec_string}, 'block_index': 0, {stream_prefix}B'}}",
         f"{{{rec_string}, 'block_index': 0, {stream_prefix}C'}}",
         f"{{{rec_string}, 'block_index': 1, {stream_prefix}B'}}",
         f"{{{rec_string}, 'block_index': 1, {stream_prefix}C'}}",
         f"{{{rec_string}, 'block_index': 2, {stream_prefix}B'}}",
         f"{{{rec_string}, 'block_index': 2, {stream_prefix}C'}}"
         ])

    def test_read(self):
        openephys_reader = EphysReaders.Readers.openephys.name
        read_blocks = EphysReaders.get_read_blocks(openephys_reader,
                                                   self.open_ephys_dir)
        read_blocks_info = [str(r) for r in read_blocks]
        self.assertEqual(self.expected_read_blocks_info, read_blocks_info)

    def test_read_fails(self):
        with self.assertRaises(Exception):
            list(EphysReaders.get_read_blocks("Made up reader",
                                              self.open_ephys_dir))


if __name__ == "__main__":
    unittest.main()
