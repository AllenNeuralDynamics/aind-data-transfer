import os
import shutil
import tempfile
import unittest
from pathlib import Path

import dask.array as da
import zarr
import h5py
import numpy as np
from tifffile import tifffile

from aind_data_transfer.util.io_utils import (
    DataReaderFactory,
    ImarisReader,
    TiffReader,
    BlockedArrayWriter,
)

# TODO: make test fixtures instead of constants?
IM_SHAPE = (64, 128, 128)
IM_DTYPE = np.dtype("uint16")


def _write_test_tiffs(folder, n=1):
    paths = []
    for i in range(n):
        a = np.ones(IM_SHAPE, dtype=IM_DTYPE)
        path = os.path.join(folder, f"data_{i}.tif")
        paths.append(path)
        tifffile.imwrite(path, a, imagej=True)
    return paths


def _write_test_h5(folder, n=1):
    paths = []
    for i in range(n):
        a = np.ones(IM_SHAPE, dtype=IM_DTYPE)
        path = os.path.join(folder, f"data_{i}.h5")
        paths.append(path)
        with h5py.File(path, "w") as f:
            f.create_dataset(
                ImarisReader.DEFAULT_DATA_PATH, data=a, chunks=True
            )
            # Write origin metadata
            dataset_info = f.create_group("DataSetInfo/Image")
            dataset_info.attrs["X"] = np.array(
                list(str(IM_SHAPE[2])), dtype="S"
            )
            dataset_info.attrs["Y"] = np.array(
                list(str(IM_SHAPE[1])), dtype="S"
            )
            dataset_info.attrs["Z"] = np.array(
                list(str(IM_SHAPE[0])), dtype="S"
            )
    return paths


class TestTiffReader(unittest.TestCase):
    def setUp(self):
        self._image_dir = Path(__file__).parent / "resources/imaging/data/tiff"
        self._image_dir.mkdir(parents=True, exist_ok=True)
        self._image_path = _write_test_tiffs(self._image_dir, n=1)[0]
        self._reader = TiffReader(self._image_path)

    def tearDown(self) -> None:
        self._reader.close()
        shutil.rmtree(self._image_dir)

    def test_get_filepath(self):
        self.assertEqual(self._image_path, self._reader.get_filepath())

    def test_as_array(self):
        a = self._reader.as_array()
        self.assertIsInstance(a, np.ndarray)
        self.assertEqual(IM_SHAPE, a.shape)
        self.assertTrue(np.all(a == 1))
        self.assertEqual(IM_DTYPE, a.dtype)

    def test_as_dask_array(self):
        d = self._reader.as_dask_array()
        self.assertIsInstance(d, da.Array)
        self.assertEqual(IM_SHAPE, d.shape)
        self.assertTrue(np.all(d == 1))
        self.assertEqual(IM_DTYPE, d.dtype)

    def test_get_shape(self):
        self.assertEqual(IM_SHAPE, self._reader.get_shape())

    def test_get_chunks(self):
        chunks = self._reader.get_chunks()
        self.assertEqual(3, len(chunks))
        self.assertTrue(all(isinstance(c, int) for c in chunks))
        self.assertTrue(all(c >= 1 for c in chunks))

    def test_get_dtype(self):
        self.assertEqual(IM_DTYPE, self._reader.get_dtype())

    def test_get_itemsize(self):
        self.assertEqual(IM_DTYPE.itemsize, self._reader.get_itemsize())


class TestImarisReader(unittest.TestCase):
    def setUp(self):
        self._temp_dir = tempfile.TemporaryDirectory()
        self._image_dir = Path(self._temp_dir.name) / "images"
        os.makedirs(self._image_dir, exist_ok=True)
        self._image_path = _write_test_h5(self._image_dir, n=1)[0]
        self._reader = ImarisReader(self._image_path)

    def tearDown(self) -> None:
        self._reader.close()
        self._temp_dir.cleanup()

    def test_constructor(self):
        self.assertEqual(self._reader.filepath, self._image_path)
        self.assertIsNotNone(self._reader.get_handle())

    def test_get_filepath(self):
        self.assertEqual(self._image_path, self._reader.get_filepath())

    def test_as_array(self):
        a = self._reader.as_array()
        self.assertIsInstance(a, np.ndarray)
        self.assertEqual(IM_SHAPE, a.shape)
        self.assertTrue(np.all(a == 1))
        self.assertEqual(IM_DTYPE, a.dtype)

    def test_as_dask_array(self):
        d = self._reader.as_dask_array(chunks=(32, 32, 32))
        self.assertIsInstance(d, da.Array)
        self.assertEqual(IM_SHAPE, d.shape)
        self.assertTrue(np.all(d == 1))
        self.assertEqual(IM_DTYPE, d.dtype)

    def test_get_shape(self):
        self.assertEqual(IM_SHAPE, self._reader.get_shape())

    def test_get_chunks(self):
        chunks = self._reader.get_chunks()
        self.assertEqual(3, len(chunks))
        self.assertTrue(all(isinstance(c, int) for c in chunks))
        self.assertTrue(all(c >= 1 for c in chunks))

    def test_get_itemsize(self):
        self.assertEqual(IM_DTYPE.itemsize, self._reader.get_itemsize())

    def test_get_handle(self):
        self.assertIsNotNone(self._reader.get_handle())

    def test_close(self):
        self._reader.close()
        self.assertIsNone(self._reader.handle)
        self.assertRaises(TypeError, self._reader.as_array)

    def test_n_levels(self):
        self.assertEqual(1, self._reader.n_levels)


class TestDataReaderFactor(unittest.TestCase):
    def setUp(self):
        self._temp_dir = tempfile.TemporaryDirectory()
        self._image_dir = Path(self._temp_dir.name) / "images"
        os.makedirs(self._image_dir, exist_ok=True)
        self._h5_path = _write_test_h5(self._image_dir, n=1)[0]
        self._tiff_path = _write_test_tiffs(self._image_dir, n=1)[0]

    def tearDown(self) -> None:
        self._temp_dir.cleanup()

    def test_create_hdf5reader(self):
        reader = DataReaderFactory().create(self._h5_path)
        self.assertIsInstance(reader, ImarisReader)
        reader.close()

    def test_create_tiffreader(self):
        reader = DataReaderFactory().create(self._tiff_path)
        self.assertIsInstance(reader, TiffReader)
        reader.close()


class TestBlockedArrayWriter(unittest.TestCase):
    def test_gen_slices(self):
        """Test the gen_slices method."""
        arr_shape = (16, 16, 16)
        block_shape = (4, 5, 8)

        # Generate the slices
        slices = list(BlockedArrayWriter.gen_slices(arr_shape, block_shape))

        # Verify the number of slices
        num_blocks = np.prod(
            [np.ceil(a / b) for a, b in zip(arr_shape, block_shape)]
        )
        self.assertEqual(len(slices), num_blocks)

        # Verify the shape of each slice
        for slice_ in slices:
            slice_shape = tuple(s.stop - s.start for s in slice_)
            # For each dimension, the slice size should be equal to the block size or the remainder of the array size
            for ss, bs, as_ in zip(slice_shape, block_shape, arr_shape):
                self.assertTrue(ss == bs or ss == as_ % bs)

    def test_gen_slices_different_lengths(self):
        """Test the gen_slices method with arguments of different lengths."""
        arr_shape = (10, 10, 10)
        block_shape = (5, 5)

        # Test that gen_slices raises an Exception when given inputs of different lengths
        with self.assertRaises(Exception):
            list(BlockedArrayWriter.gen_slices(arr_shape, block_shape))

    def test_store(self):
        """Test the store method."""
        arr_shape = (10, 10, 10)
        block_shape = (5, 5, 5)

        # Create an input Dask array
        in_array = da.from_array(
            np.random.rand(*arr_shape), chunks=block_shape
        )

        # Create an output Zarr array
        out_array = zarr.zeros(arr_shape)

        # Call the store method
        BlockedArrayWriter.store(in_array, out_array, block_shape)

        # Check if the output array matches the input array
        np.testing.assert_array_equal(out_array, in_array)

    def test_get_block_shape(self):
        d = da.zeros(
            shape=(20000, 20000, 20000),
            chunks=(128, 256, 256),
            dtype=np.uint16,
        )
        target_size_mb = 102400
        expected_block_shape = (4096, 4096, 4096)
        self.assertEqual(
            expected_block_shape,
            BlockedArrayWriter.get_block_shape(d, target_size_mb),
        )


if __name__ == "__main__":
    unittest.main()
