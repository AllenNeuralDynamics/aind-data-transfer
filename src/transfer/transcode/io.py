import os
import zarr
from abc import ABC, abstractmethod

import dask.array as da
import dask_image.imread
import h5py
# Importing this alone doesn't work on HPC
# Must manually override HDF5_PLUGIN_PATH environment variable
# in each Dask worker
import hdf5plugin
import numpy as np
import tifffile


class DataReader(ABC):
    def __init__(self, filepath):
        self.filepath = filepath
        super().__init__()

    @abstractmethod
    def as_dask_array(self, chunks=True) -> da.Array:
        pass

    @abstractmethod
    def as_array(self) -> np.ndarray:
        pass

    @abstractmethod
    def close(self) -> None:
        pass

    @abstractmethod
    def get_shape(self) -> tuple:
        pass

    @abstractmethod
    def get_chunks(self) -> tuple:
        pass

    @abstractmethod
    def get_itemsize(self) -> int:
        pass


class TiffReader(DataReader):
    def as_dask_array(self, chunks=True):
        return dask_image.imread.imread(self.filepath)

    def as_array(self):
        return tifffile.imread(self.filepath)

    def as_zarr(self):
        with tifffile.TiffFile(self.filepath) as tif:
            return zarr.open(tif.aszarr(), 'r')

    def get_shape(self):
        # Open as Zarr store in case we're dealing with
        # ImageJ hyperstacks
        return self.as_zarr().shape

    def get_chunks(self):
        return self.as_zarr().chunks

    def get_itemsize(self):
        return self.as_zarr().itemsize

    def close(self):
        pass


class HDF5Reader(DataReader):
    IMAGE_DATA_PATH = "/DataSet/ResolutionLevel 0/TimePoint 0/Channel 0/Data"

    def __init__(self, filepath):
        super().__init__(filepath)
        self.handle = h5py.File(self.filepath, mode='r')

    def as_dask_array(self, chunks=True):
        return da.from_array(self.get_dataset(), chunks=chunks)

    def as_array(self):
        return self.get_dataset()[:]

    def get_dataset(self) -> h5py.Dataset:
        return self.handle[self.IMAGE_DATA_PATH]

    def get_shape(self):
        return self.get_dataset().shape

    def get_chunks(self):
        return self.get_dataset().chunks

    def get_itemsize(self):
        return self.get_dataset().dtype.itemsize

    def close(self):
        if self.handle is not None:
            self.handle.close()
            self.handle = None


class DataReaderFactory:
    VALID_EXTENSIONS = [".tiff", ".tif", ".h5", ".ims"]

    factory = {}

    def __init__(self):
        self.factory[".tif"] = TiffReader
        self.factory[".tiff"] = TiffReader
        self.factory[".h5"] = HDF5Reader
        self.factory[".ims"] = HDF5Reader

    def create(self, filepath) -> DataReader:
        _, ext = os.path.splitext(filepath)
        if ext not in self.VALID_EXTENSIONS:
            raise NotImplementedError(f"File type {ext} not supported")
        return self.factory[ext](filepath)
