import logging
import math
import os
import re
import tempfile
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, List, Tuple, Generator

import dask.array as da
import fsspec
import h5py

# Importing this alone doesn't work on HPC
# Must manually override HDF5_PLUGIN_PATH environment variable
# in each Dask worker
import hdf5plugin
import numpy as np
import ujson
import zarr
from aind_data_transfer.util.chunk_utils import expand_chunks
from numpy.typing import ArrayLike
from kerchunk.tiff import tiff_to_zarr
from numpy import dtype


_LOGGER = logging.getLogger(__name__)


class DataReader(ABC):
    def __init__(self, filepath):
        self.filepath = filepath
        super().__init__()

    @abstractmethod
    def as_dask_array(self, chunks: Any = None) -> da.Array:
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

    @abstractmethod
    def __enter__(self):
        pass

    @abstractmethod
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def get_filepath(self) -> str:
        return self.filepath


class TiffReader(DataReader):
    def __init__(self, filepath: str):
        """
        Class constructor

        Args:
            filepath: the path to the Tiff
            refs_dir: the directory to write the temporary references file used by kerchunk
        """
        filepath = str(filepath)
        super().__init__(filepath)
        refs = tiff_to_zarr(filepath)
        # This file needs to be readable by all nodes
        # in the cluster. I'm writing to the current
        # working directory since the code will likely
        # be run from a location accessible by all nodes.
        self._temp_dir = tempfile.TemporaryDirectory(dir=os.getcwd())
        self._refs_file = os.path.join(
            self._temp_dir.name, f"references-{Path(filepath).name}.json"
        )
        if os.path.isfile(self._refs_file):
            raise Exception(
                f"References file already exists: {self._refs_file}"
            )
        Path(self._refs_file).parent.mkdir(parents=True, exist_ok=True)
        with open(self._refs_file, "wb") as f:
            f.write(ujson.dumps(refs).encode())
        fs = fsspec.filesystem(
            "reference", fo=self._refs_file, remote_protocol="file"
        )
        self._arr = zarr.open(fs.get_mapper(""), "r")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def as_dask_array(self, chunks=None):
        """Return the image stack as a Dask Array

        Args:
            chunks: the chunk shape.

        Returns:
            dask.array.Array
        """
        # For performance it is *critical* the initial dask array construction
        # uses single plane chunks, and to then rechunk with the desired chunks
        a = da.from_array(
            self._arr, chunks=(1, self._arr.shape[-2], self._arr.shape[-1])
        )
        if chunks is not None:
            a = a.rechunk(chunks)
        return a

    def as_array(self) -> np.ndarray:
        """
        Get the image stack as a ndarray
        """
        return self._arr[:]

    def as_zarr(self):
        """
        Get the image stack as a zarr Array
        """
        return self._arr

    def get_shape(self) -> tuple:
        """
        Get the shape of the image stack
        """
        return self._arr.shape

    def get_chunks(self) -> tuple:
        """
        Get the chunk shape of the image stack
        """
        return self._arr.chunks

    def get_dtype(self) -> dtype:
        """
        Get the data type of the image stack
        """
        return self._arr.dtype

    def get_itemsize(self) -> int:
        """
        Get the number of bytes per element
        """
        return self.get_dtype().itemsize

    def close(self) -> None:
        """Close the file handle to free resources"""
        self._arr = None
        try:
            self._temp_dir.cleanup()
        except Exception as e:
            _LOGGER.error(f"Error removing references dir: {e}")


class MissingDatasetError(Exception):
    pass


class ImarisReader(DataReader):
    DEFAULT_DATA_PATH = "/DataSet/ResolutionLevel 0/TimePoint 0/Channel 0/Data"

    def __init__(self, filepath):
        super().__init__(filepath)
        self.handle = h5py.File(self.filepath, mode="r")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def as_dask_array(
        self, data_path: str = DEFAULT_DATA_PATH, chunks=True
    ) -> da.Array:
        """Return the image stack as a Dask Array

        Args:
            data_path: the path to the dataset
            chunks: the chunk shape

        Returns:
            dask.array.Array
        """
        # Shape of the full-res image before 0-padding added by ImarisWriter
        lvl = self._get_res_lvl(data_path)
        real_shape_at_lvl = self._get_shape_at_lvl(lvl)

        # This array is 0-padded to be divisible by the Imaris chunk size in each dimension
        a = da.from_array(self.get_dataset(data_path), chunks=chunks)
        # Crop to the "real" shape at the given resolution
        return a[
            : real_shape_at_lvl[0],
            : real_shape_at_lvl[1],
            : real_shape_at_lvl[2],
        ]

    def as_array(self, data_path=DEFAULT_DATA_PATH) -> np.ndarray:
        """
        Get the image stack as a ndarray

        Args:
            data_path: the path to the dataset
        """
        lvl = self._get_res_lvl(data_path)
        real_shape_at_lvl = self._get_shape_at_lvl(lvl)
        return self.get_dataset(data_path)[
            : real_shape_at_lvl[0],
            : real_shape_at_lvl[1],
            : real_shape_at_lvl[2],
        ]

    def get_dataset(self, data_path=DEFAULT_DATA_PATH) -> h5py.Dataset:
        """
        Get the dataset at the specified path

        Args:
            data_path: the path to the dataset

        Returns:
            the dataset
        """
        return self.handle[data_path]

    def get_shape(self) -> tuple:
        """
        Get the shape of the image
        """
        info = self.get_dataset_info()
        return (
            int(info.attrs["Z"].tobytes()),
            int(info.attrs["Y"].tobytes()),
            int(info.attrs["X"].tobytes()),
        )

    def get_chunks(self, data_path=DEFAULT_DATA_PATH) -> tuple:
        """
        Get the chunk shape of the image

        Args:
            data_path: the path to the dataset
        """
        return self.get_dataset(data_path).chunks

    def get_itemsize(self, data_path=DEFAULT_DATA_PATH) -> int:
        """
        Get the bytes per element of the image

        Args:
            data_path: the path to the dataset
        """
        return self.get_dataset(data_path).dtype.itemsize

    def get_handle(self) -> h5py.File:
        """
        Get the file handle
        """
        return self.handle

    def get_dask_pyramid(
        self,
        num_levels: int,
        timepoint: int = 0,
        channel: int = 0,
        chunks: Any = True,
    ) -> List[da.Array]:
        """
        Get a multiscale image pyramid as a list of dask arrays.
        The arrays are ordered from highest resolution to lowest.

        Args:
            num_levels: the number of resolution levels to get
            timepoint: the timepoint of the Imaris dataset
            channel: the channel of the Imaris dataset
            chunks: the chunk shape to use for each array.
                    If chunks are larger than the image shape in any dimension,
                    the chunks are clipped to the image shape in that dimension.

        Returns:
            the image pyramid as a list of dask arrays
        """
        darrays = []
        for lvl in range(0, num_levels):
            ds_path = f"/DataSet/ResolutionLevel {lvl}/TimePoint {timepoint}/Channel {channel}/Data"
            if ds_path not in self.get_handle():
                raise MissingDatasetError(f"{ds_path} does not exist")

            if isinstance(chunks, bool) or chunks is None:
                lvl_chunks = chunks
            else:
                lvl_shape = self._get_shape_at_lvl(lvl)
                assert len(chunks) == len(lvl_shape)
                lvl_chunks = list(chunks)
                for i in range(len(chunks)):
                    lvl_chunks[i] = min(chunks[i], lvl_shape[i])

            darrays.append(self.as_dask_array(ds_path, chunks=lvl_chunks))

        return darrays

    def get_dataset_info(self) -> h5py.Dataset:
        """
        Get the DataSetInfo for the image
        """
        return self.get_handle()[f"DataSetInfo/Image"]

    def get_origin(self) -> List[float]:
        """
        Get the origin for the image as a [Z,Y,X] coordinate list
        """
        info = self.get_dataset_info()
        x_min = float(info.attrs["ExtMin0"].tobytes())
        y_min = float(info.attrs["ExtMin1"].tobytes())
        z_min = float(info.attrs["ExtMin2"].tobytes())
        return [z_min, y_min, x_min]

    def get_extent(self) -> List[float]:
        """
        Get the extent for the image as a [Z,Y,X] coordinate list
        """
        info = self.get_dataset_info()
        x_max = float(info.attrs["ExtMax0"].tobytes())
        y_max = float(info.attrs["ExtMax1"].tobytes())
        z_max = float(info.attrs["ExtMax2"].tobytes())
        return [z_max, y_max, x_max]

    def get_voxel_size(self) -> Tuple[List[float], str]:
        """
        Get the voxel size of the image as a [Z,Y,X] list and the unit string

        Returns:
            the voxel size list and unit string
        """
        info = self.get_dataset_info()
        extmin = self.get_origin()
        extmax = self.get_extent()
        x = int(info.attrs["X"].tobytes())
        y = int(info.attrs["Y"].tobytes())
        z = int(info.attrs["Z"].tobytes())
        unit = info.attrs["Unit"].tobytes()
        voxsize = [
            (extmax[0] - extmin[0]) / z,
            (extmax[1] - extmin[1]) / y,
            (extmax[2] - extmin[2]) / x,
        ]
        return voxsize, unit

    def close(self) -> None:
        """Close the file handle to free resources"""
        if self.handle is not None:
            self.handle.close()
            self.handle = None

    def _get_res_lvl(self, data_path: str):
        lvl_rgx = r"/ResolutionLevel (\d+)/"
        m = re.search(lvl_rgx, data_path)
        if m is None:
            raise Exception(
                f"Could not parse resolution level from path {data_path}"
            )
        return int(m.group(1))

    def _get_shape_at_lvl(self, lvl: int):
        shape = self.get_shape()
        return tuple(int(math.ceil(s / 2**lvl)) for s in shape)

    @property
    def n_levels(self):
        lvl = 0
        while True:
            ds_path = (
                f"/DataSet/ResolutionLevel {lvl}/TimePoint 0/Channel 0/Data"
            )
            if ds_path not in self.get_handle():
                return lvl
            lvl += 1


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

    def create(self, filepath: str) -> DataReader:
        """
        Create the corresponding DataReader from the filepath

        Args:
            filepath: the path to the image

        Returns:
            the DataReader instance
        """
        _, ext = os.path.splitext(filepath)
        if ext not in self.VALID_EXTENSIONS:
            raise NotImplementedError(f"File type {ext} not supported")
        return self.factory[ext](filepath)


class BlockedArrayWriter:
    @staticmethod
    def gen_slices(
        arr_shape: Tuple[int, ...], block_shape: Tuple[int, ...]
    ) -> Generator:
        """
        Generate a series of slices that can be used to traverse an array in blocks of a given shape.

        The method generates tuples of slices, each representing a block of the array. The blocks are generated by
        iterating over the array in steps of the block shape along each dimension.

        Parameters
        ----------
        arr_shape : tuple of int
            The shape of the array to be sliced.

        block_shape : tuple of int
            The desired shape of the blocks. This should be a tuple of integers representing the size of each
            dimension of the block. The length of `block_shape` should be equal to the length of
            `arr_shape`. If the array shape is not divisible by the block shape along a dimension, the last slice
            along that dimension is truncated.

        Returns
        -------
        generator of tuple of slice
            A generator yielding tuples of slices. Each tuple can be used to index the input array.
        """
        if len(arr_shape) != len(block_shape):
            raise Exception(
                "array shape and block shape have different lengths"
            )

        def _slice_along_dim(dim: int) -> Generator:
            """A helper generator function that slices along one dimension."""
            # Base case: if the dimension is beyond the last one, return an empty tuple
            if dim >= len(arr_shape):
                yield ()
            else:
                # Iterate over the current dimension in steps of the block size
                for i in range(0, arr_shape[dim], block_shape[dim]):
                    # Calculate the end index for this block
                    end_i = min(i + block_shape[dim], arr_shape[dim])
                    # Generate slices for the remaining dimensions
                    for rest in _slice_along_dim(dim + 1):
                        yield (slice(i, end_i),) + rest

        # Start slicing along the first dimension
        return _slice_along_dim(0)

    @staticmethod
    def store(
        in_array: da.Array, out_array: ArrayLike, block_shape: tuple
    ) -> None:
        """
        Partitions the last 3 dimensions of a Dask array into non-overlapping blocks and
        writes them sequentially to a Zarr array. This is meant to reduce the scheduling burden
        for massive (terabyte-scale) arrays.

        :param in_array: The input Dask array
        :param block_shape: Tuple of (block_depth, block_height, block_width)
        :param out_array: The output array
        """
        # Iterate through the input array in steps equal to the block shape dimensions
        for sl in BlockedArrayWriter.gen_slices(in_array.shape, block_shape):
            block = in_array[sl]
            da.store(
                block,
                out_array,
                regions=sl,
                lock=False,
                compute=True,
                return_stored=False,
            )

    @staticmethod
    def get_block_shape(arr, target_size_mb=409600, mode="cycle"):
        """
        Given the shape and chunk size of a pre-chunked array, determine the optimal block shape
        closest to target_size. Expanded block dimensions are an integer multiple of the chunk dimension
        to ensure optimal access patterns.
        Args:
            arr: the input array
            target_size_mb: target block size in megabytes, default is 409600
            mode: strategy. Must be one of "cycle", or "iso"
        Returns:
            the block shape
        """
        if isinstance(arr, da.Array):
            chunks = arr.chunksize[-3:]
        else:
            chunks = arr.chunks[-3:]

        return expand_chunks(
            chunks,
            arr.shape[-3:],
            target_size_mb * 1024**2,
            arr.itemsize,
            mode,
        )
