import unittest
from aind_data_transfer.transformations import tiff_converter
from pathlib import Path
import h5py as h5


class TestTiffConverter(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.input_dir = Path("//allen/aind/scratch/BCI_43_032423")
        cls.output_dir = Path("//allen/aind/scratch/BCI_43_032423")
        cls.unique_id = "BCI_43_032423"
        cls.filename = "BCI_43_032423.h5"
        cls.filepath = cls.output_dir / cls.filename
    
    @classmethod
    def tearDownClass(cls) -> None:
        (cls.output_dir / cls.filename).unlink()
    
    def test_run_converter(self):
        btc = tiff_converter.BergamoTiffConverter(self.input_dir, self.output_dir, self.unique_id)
        keys = ['data', 'epoch_filenames',  'epoch_slice_location','lookup_table']
        filepath = btc.run_converter()
        print(filepath)
        print(self.filepath)
        self.assertEqual(filepath,self.filepath)
        with h5.File(filepath, 'r') as f:
            for key in keys:
                self.assertIn(key, f.keys())
        

if __name__ == '__main__':
    unittest.main()