import os
import unittest
from pathlib import Path
from transfer.readers import OpenEphysReader
import spikeinterface.extractors as se

TEST_DIR = Path(os.path.dirname(os.path.realpath(__file__)))
RESOURCES_DIR = TEST_DIR / "resources"


class TestOpenEphysReaders(unittest.TestCase):

    open_ephys_dir = RESOURCES_DIR / "v0.6.x_neuropixels_multiexp_multistream"
    # configs

    def test_read(self):
        configs = {'stream_id': '0', 'block_index': 0}
        print(str(self.open_ephys_dir))
        rec_oe = se.read_openephys(folder_path=self.open_ephys_dir,
                                stream_id='0',
                                block_index=0)
        rec_dtype = rec_oe.get_dtype()

        lsb_value, median_values = get_median_and_lsb(rec_oe,
                                                      num_random_chunks=2)

        # output = self.reader.read(str(self.open_ephys_dir), {'stream_id': '0', 'block_index': 0})

        print(rec_dtype)

        self.assertIsNotNone(rec_oe)


if __name__ == "__main__":
    unittest.main()
