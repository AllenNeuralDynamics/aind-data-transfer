import os
import unittest
from pathlib import Path

from parameterized import parameterized

from aind_data_transfer.readers.imaging_readers import ImagingReaders

TEST_DIR = Path(os.path.dirname(os.path.realpath(__file__)))
RESOURCES_DIR = TEST_DIR / "resources"


class TestImagingReaders(unittest.TestCase):
    imaging_dir = RESOURCES_DIR / "imaging"
    exaspim_dir = imaging_dir / "exaSPIM_125L_2022-08-05_17-25-36"
    mesospim_dir = imaging_dir / "mesoSPIM_125L_2022-08-18_17-05-00"
    dispim_dir = imaging_dir / "smartSPIM_12345_2023-03-16_00-00-00"
    generic_dir = imaging_dir / "12345-random-sample"

    @parameterized.expand(
        [
            (ImagingReaders.Readers.exaspim.value, exaspim_dir),
            (ImagingReaders.Readers.mesospim.value, mesospim_dir),
            (ImagingReaders.Readers.dispim.value, dispim_dir),
            (ImagingReaders.Readers.generic.value, generic_dir),
        ]
    )
    def test_get_raw_data_dir(self, reader, input_dir):
        raw_data_dir = ImagingReaders.get_raw_data_dir(reader, input_dir)
        expected_raw_data_dir = input_dir / reader
        self.assertEqual(expected_raw_data_dir, raw_data_dir)

    @parameterized.expand(
        [
            ImagingReaders.Readers.exaspim.value,
            ImagingReaders.Readers.mesospim.value,
            ImagingReaders.Readers.dispim.value,
            ImagingReaders.Readers.generic.value,
        ]
    )
    def test_get_raw_data_dir_fails(self, reader):
        with self.assertRaises(FileNotFoundError):
            ImagingReaders.get_raw_data_dir(reader, "Made up directory")


if __name__ == "__main__":
    unittest.main()
