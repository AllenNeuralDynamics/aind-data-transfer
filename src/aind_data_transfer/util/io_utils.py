from typing import Any

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
    def as_dask_array(self, chunks: Any = True) -> da.Array:
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

    def get_filepath(self) -> str:
        return self.filepath


class TiffReader(DataReader):
    def __init__(self, filepath):
        super().__init__(filepath)
        self.handle = tifffile.TiffFile(filepath)

    def as_dask_array(self, chunks=True):
        # Note, this will only read the first plane of
        # ImageJ-formatted Tiffs that are larger than 4GB
        return dask_image.imread.imread(self.filepath)

    def as_array(self):
        return self.as_zarr()[:]

    def as_zarr(self):
        return zarr.open(self.handle.aszarr(), "r")

    def get_shape(self):
        # Open as Zarr store in case we're dealing with
        # ImageJ hyperstacks
        return self.as_zarr().shape

    def get_chunks(self):
        return self.as_zarr().chunks

    def get_itemsize(self):
        with tifffile.TiffFile(self.filepath) as tif:
            return tif.series[0].dtype.itemsize

    def get_handle(self):
        return self.handle

    def close(self):
        if self.handle is not None:
            self.handle.close()
            self.handle = None


class MissingDatasetError(Exception):
    pass


class ImarisReader(DataReader):
    DEFAULT_DATA_PATH = "/DataSet/ResolutionLevel 0/TimePoint 0/Channel 0/Data"

    def __init__(self, filepath):
        super().__init__(filepath)
        self.handle = h5py.File(self.filepath, mode="r")

    def as_dask_array(self, data_path=DEFAULT_DATA_PATH, chunks=True):
        return da.from_array(self.get_dataset(data_path), chunks=chunks)

    def as_array(self, data_path=DEFAULT_DATA_PATH):
        return self.get_dataset(data_path)[:]

    def get_dataset(self, data_path=DEFAULT_DATA_PATH) -> h5py.Dataset:
        return self.handle[data_path]

    def get_shape(self, data_path=DEFAULT_DATA_PATH):
        return self.get_dataset(data_path).shape

    def get_chunks(self, data_path=DEFAULT_DATA_PATH):
        return self.get_dataset(data_path).chunks

    def get_itemsize(self, data_path=DEFAULT_DATA_PATH):
        return self.get_dataset(data_path).dtype.itemsize

    def get_handle(self):
        return self.handle

    def get_dask_pyramid(
        self, num_levels, timepoint=0, channel=0, chunks: Any = True
    ):
        darrays = []
        for lvl in range(0, num_levels):
            ds_path = f"/DataSet/ResolutionLevel {lvl}/TimePoint {timepoint}/Channel {channel}/Data"
            if ds_path not in self.get_handle():
                raise MissingDatasetError(f"{ds_path} does not exist")

            if isinstance(chunks, bool) or chunks is None:
                lvl_chunks = chunks
            else:
                lvl_shape = self.get_shape(ds_path)
                assert len(chunks) == len(lvl_shape)
                lvl_chunks = list(chunks)
                for i in range(len(chunks)):
                    lvl_chunks[i] = min(chunks[i], lvl_shape[i])

            darrays.append(self.as_dask_array(ds_path, chunks=lvl_chunks))

        return darrays

    def get_dataset_info(self):
        return self.get_handle()[f"DataSetInfo/Image"]

    def get_origin(self):
        info = self.get_dataset_info()
        x_min = float(info.attrs["ExtMin0"].tostring())
        y_min = float(info.attrs["ExtMin1"].tostring())
        z_min = float(info.attrs["ExtMin2"].tostring())
        return [z_min, y_min, x_min]

    def get_extent(self):
        info = self.get_dataset_info()
        x_max = float(info.attrs["ExtMax0"].tostring())
        y_max = float(info.attrs["ExtMax1"].tostring())
        z_max = float(info.attrs["ExtMax2"].tostring())
        return [z_max, y_max, x_max]

    def get_voxel_size(self):
        info = self.get_dataset_info()
        extmin = self.get_origin()
        extmax = self.get_extent()
        x = int(info.attrs["X"].tostring())
        y = int(info.attrs["Y"].tostring())
        z = int(info.attrs["Z"].tostring())
        unit = info.attrs["Unit"].tostring()
        voxsize = [
            (extmax[0] - extmin[0]) / z,
            (extmax[1] - extmin[1]) / y,
            (extmax[2] - extmin[2]) / x,
        ]
        return voxsize, unit

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
        self.factory[".h5"] = ImarisReader
        self.factory[".ims"] = ImarisReader

    def get_valid_extensions(self):
        return self.VALID_EXTENSIONS

    def create(self, filepath) -> DataReader:
        _, ext = os.path.splitext(filepath)
        if ext not in self.VALID_EXTENSIONS:
            raise NotImplementedError(f"File type {ext} not supported")
        return self.factory[ext](filepath)
