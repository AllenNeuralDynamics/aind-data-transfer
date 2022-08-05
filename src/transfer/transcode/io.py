import os
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
    def as_dask_array(self) -> da.Array:
        pass

    @abstractmethod
    def as_array(self) -> np.ndarray:
        pass

    @abstractmethod
    def close(self) -> None:
        pass


class TiffReader(DataReader):
    def as_dask_array(self):
        return dask_image.imread.imread(self.filepath)

    def as_array(self):
        return tifffile.imread(self.filepath)

    def close(self):
        pass


class HDF5Reader(DataReader):
    IMAGE_DATA_PATH = "/DataSet/ResolutionLevel 0/TimePoint 0/Channel 0/Data"

    def __init__(self, filepath):
        super().__init__(filepath)
        self.handle = h5py.File(self.filepath, mode='r')

    def as_dask_array(self):
        return da.from_array(self.get_dataset())

    def as_array(self):
        return self.get_dataset()[:]

    def get_dataset(self):
        return self.handle[self.IMAGE_DATA_PATH]

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
