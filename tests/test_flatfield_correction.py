import unittest
import os

import numpy as np
import dask.array as da

from aind_data_transfer.transformations.flatfield_correction import (
    BkgSubtraction,
)


class TestBkgSubtraction(unittest.TestCase):
    def test_subtract(self):
        im = da.from_array(np.full((64, 64, 64), fill_value=100, dtype=np.uint16), chunks=(16, 16, 16))
        bkg_im = da.from_array(np.full((32, 32), fill_value=50, dtype=np.uint16), chunks=(16, 16))

        result = BkgSubtraction.subtract(im, bkg_im)
        self.assertIsInstance(result, da.Array)
        self.assertEqual(result.shape, im.shape)
        self.assertEqual(result.dtype, np.uint16)
        self.assertEqual(result.sum().compute(), np.product(im.shape) * 50)

        # test when bkg_im is larger than im
        bkg_im = da.from_array(np.full((128, 128), fill_value=50, dtype=np.uint16), chunks=(16, 16))
        result = BkgSubtraction.subtract(im, bkg_im)
        self.assertIsInstance(result, da.Array)
        self.assertEqual(result.shape, im.shape)
        self.assertEqual(result.dtype, np.uint16)
        self.assertEqual(result.sum().compute(), np.product(im.shape) * 50)

        # test clipping behavior
        im = da.from_array(np.full((64, 64, 64), fill_value=100, dtype=np.uint16), chunks=(16, 16, 16))
        bkg_im = da.from_array(np.full((64, 64), fill_value=200, dtype=np.uint16), chunks=(16, 16))
        result = BkgSubtraction.subtract(im, bkg_im)
        self.assertIsInstance(result, da.Array)
        self.assertEqual(result.shape, im.shape)
        self.assertEqual(result.dtype, np.uint16)
        self.assertEqual(result.sum().compute(), 0)

    def test_get_bkg_path(self):
        bkg_im_dir = "path/to/background_images/"

        with self.assertRaises(ValueError):
            BkgSubtraction.get_bkg_path("invalid_tile_path", bkg_im_dir)

        # Test with a valid tile path
        raw_tile_path = "/path/to/tiles/tile_X_0000_Y_0000_Z_0000_ch_488.ims"
        expected_bkg_path = os.path.join(
            bkg_im_dir, "bkg_tile_X_0000_Y_0000_Z_0000.tiff"
        )
        result = BkgSubtraction.get_bkg_path(raw_tile_path, bkg_im_dir)
        self.assertEqual(result, expected_bkg_path)


if __name__ == "__main__":
    unittest.main()
