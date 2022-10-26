import os
import tempfile
import unittest
from pathlib import Path

import dask.array
import h5py
import numpy as np
import zarr.core
from distributed import Client
from tifffile import tifffile

from aind_data_transfer.util.io_utils import TiffReader, ImarisReader, DataReaderFactory

# TODO: make test fixtures instead of constants?
IM_SHAPE = (64, 128, 128)
IM_DTYPE = np.uint16


def _write_test_tiffs(folder, n=1):
    paths = []
    for i in range(n):
        a = np.ones(IM_SHAPE, dtype=IM_DTYPE)
        path = os.path.join(folder, f"data_{i}.tif")
        paths.append(path)
        tifffile.imwrite(path, a)
    return paths


def _write_test_h5(folder, n=1):
    DEFAULT_DATA_PATH = "/DataSet/ResolutionLevel 0/TimePoint 0/Channel 0/Data"
    paths = []
    for i in range(n):
        a = np.ones(IM_SHAPE, dtype=IM_DTYPE)
        path = os.path.join(folder, f"data_{i}.h5")
        paths.append(path)
        with h5py.File(path, 'w') as f:
            f.create_dataset(DEFAULT_DATA_PATH, data=a, chunks=True)
    return paths


class TestTiffReader(unittest.TestCase):

    def setUp(self):
        self._temp_dir = tempfile.TemporaryDirectory()
        self._image_dir = Path(self._temp_dir.name) / "images"
        os.makedirs(self._image_dir, exist_ok=True)
        self._image_path = _write_test_tiffs(self._image_dir, n=1)[0]
        self._reader = TiffReader(self._image_path)

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
        d = self._reader.as_dask_array()
        self.assertIsInstance(d, dask.array.Array)
        self.assertEqual(IM_SHAPE, d.shape)
        self.assertTrue(np.all(d == 1))
        self.assertEqual(IM_DTYPE, d.dtype)

    def test_as_zarr(self):
        z = self._reader.as_zarr()
        self.assertIsInstance(z, zarr.core.Array)
        self.assertEqual(IM_SHAPE, z.shape)
        self.assertTrue(np.all(z[:] == 1))
        self.assertEqual(IM_DTYPE, z.dtype)

    def test_get_shape(self):
        self.assertEqual(IM_SHAPE, self._reader.get_shape())

    def test_get_chunks(self):
        chunks = self._reader.get_chunks()
        self.assertEqual(3, len(chunks))
        self.assertTrue(all(isinstance(c, int) for c in chunks))
        self.assertTrue(all(c >= 1 for c in chunks))

    def test_get_itemsize(self):
        self.assertEqual(2, self._reader.get_itemsize())

    def test_get_handle(self):
        self.assertIsNotNone(self._reader.get_handle())

    def test_close(self):
        self._reader.close()
        self.assertIsNone(self._reader.handle)
        self.assertRaises(AttributeError, self._reader.as_array)


class TestHDF5Reader(unittest.TestCase):

    def setUp(self):
        self._temp_dir = tempfile.TemporaryDirectory()
        self._image_dir = Path(self._temp_dir.name) / "images"
        os.makedirs(self._image_dir, exist_ok=True)
        self._image_path = _write_test_h5(self._image_dir, n=1)[0]
        self._reader = ImarisReader(self._image_path)
        self.client = Client()

    def tearDown(self) -> None:
        self._reader.close()
        self._temp_dir.cleanup()
        self.client.close()

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

    # FIXME: this hangs??
    # def test_as_dask_array(self):
    #     d = self._reader.as_dask_array()
    #     self.assertIsInstance(d, dask.array.Array)
    #     self.assertEqual(SHAPE, d.shape)
    #     self.assertTrue(np.all(d == 1))
    #     self.assertEqual(DTYPE, d.dtype)

    def test_get_shape(self):
        self.assertEqual(IM_SHAPE, self._reader.get_shape())

    def test_get_chunks(self):
        chunks = self._reader.get_chunks()
        self.assertEqual(3, len(chunks))
        self.assertTrue(all(isinstance(c, int) for c in chunks))
        self.assertTrue(all(c >= 1 for c in chunks))

    def test_get_itemsize(self):
        self.assertEqual(2, self._reader.get_itemsize())

    def test_get_handle(self):
        self.assertIsNotNone(self._reader.get_handle())

    def test_close(self):
        self._reader.close()
        self.assertIsNone(self._reader.handle)
        self.assertRaises(TypeError, self._reader.as_array)


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

    def test_create_tiffreader(self):
        reader = DataReaderFactory().create(self._tiff_path)
        self.assertIsInstance(reader, TiffReader)


if __name__ == "__main__":
    unittest.main()
