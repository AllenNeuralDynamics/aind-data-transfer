from typing import Union, Tuple

import math
import numpy as np
import dask.array as da


class DimensionsError(Exception):
    pass


def ensure_array_5d(arr: Union[np.ndarray, da.Array]) -> Union[np.ndarray, da.Array]:
    """
    Checks that the array is 5D, adding singleton dimensions to the
    start of the array if less, throwing a DimensionsError if more
    Args:
        arr: the arraylike object
    Returns:
        the 5D array
    Raises:
        DimensionsError: if the array has more than 5 dimensions
    """
    if arr.ndim > 5:
        raise DimensionsError("Only arrays up to 5D are supported")
    while arr.ndim < 5:
        arr = arr[np.newaxis, ...]
    return arr


def ensure_shape_5d(shape: Tuple[int, ...]) -> Tuple[int, int, int, int, int]:
    """
    Checks that the shape is 5D, adding singleton dimensions to the
    start of the sequence if less, throwing a DimensionsError if more.
    Args:
        shape: the sequence
    Returns:
        the 5D tuple
    Raises:
        DimensionsError: if the input has more than 5 dimensions
    """
    if len(shape) > 5:
        raise DimensionsError("Only shapes up to 5D are supported")
    while len(shape) < 5:
        shape = (1, *shape)
    return shape


def guess_chunks(
        data_shape: Tuple[int, int, int, int, int],
        target_size: int,
        itemsize: int,
        mode: str = "z"
) -> Tuple[int, int, int, int, int]:
    """
    Given the shape of a 5D array, determine the optimal chunk shape
    closest to target_size.
    Args:
        data_shape: the shape of the input array
        target_size: target chunk size in bytes
        itemsize: the number of bytes per array element
        mode: chunking strategy. Must be one of "z", "cycle", or "iso"
    Returns:
        the optimal chunk shape
    """
    if mode == "z":
        plane_size = data_shape[3] * data_shape[4] * itemsize
        nplanes_per_chunk = int(math.ceil(target_size / plane_size))
        nplanes_per_chunk = min(nplanes_per_chunk, data_shape[2])
        chunks = (
            1,
            1,
            nplanes_per_chunk,
            data_shape[3],
            data_shape[4],
        )
    elif mode == "cycle":
        # get the spatial dimensions only
        spatial_dims = np.array(data_shape)[2:]
        idx = 0
        ndims = len(spatial_dims)
        while _get_size(spatial_dims, itemsize) > target_size:
            spatial_dims[idx % ndims] = int(
                math.ceil(
                    spatial_dims[idx % ndims] / 2.0
                )
            )
            idx += 1
        chunks = (
            1,
            1,
            spatial_dims[0],
            spatial_dims[1],
            spatial_dims[2]
        )
    elif mode == "iso":
        # TODO: should this be a power of 2?
        chunk_dim = int(math.ceil((target_size / itemsize) ** (1.0 / 3)))
        chunks = (
            1,
            1,
            min(data_shape[2], chunk_dim),
            min(data_shape[3], chunk_dim),
            min(data_shape[4], chunk_dim)
        )
    else:
        raise ValueError(f"Invalid mode {mode}")

    # convert numpy int64 to Python int or zarr will complain
    return tuple(int(d) for d in chunks)


def expand_chunks(
        chunks: Tuple[int, int, int, int, int],
        data_shape: Tuple[int, int, int, int, int],
        target_size: int,
        itemsize: int,
        mode: str = "iso",
) -> Tuple[int, int, int, int, int]:
    """
    Given the shape and chunk size of a pre-chunked 5D array, determine the optimal chunk shape
    closest to target_size. Expanded chunk dimensions are an integer multiple of the base chunk dimension,
    to ensure optimal access patterns.
    Args:
        chunks: the shape of the input array chunks
        data_shape: the shape of the input array
        target_size: target chunk size in bytes
        itemsize: the number of bytes per array element
        mode: chunking strategy. Must be one of "cycle", or "iso"
    Returns:
        the optimal chunk shape
    """
    if mode == "cycle":
        # get the spatial dimensions only
        spatial_chunks = np.array(chunks)[2:]
        current = spatial_chunks
        prev = current.copy()
        idx = 0
        ndims = len(spatial_chunks)
        while _get_size(current, itemsize) < target_size:
            prev = current.copy()
            current[idx % ndims] *= 2
            idx += 1
        current = _closer_to_target(current, prev, target_size, itemsize)
        expanded = (
            1,
            1,
            min(data_shape[2], current[0]),
            min(data_shape[3], current[1]),
            min(data_shape[4], current[2])
        )
    elif mode == "iso":
        spatial_chunks = np.array(chunks)[2:]
        current = spatial_chunks
        prev = current
        i = 2
        while _get_size(current, itemsize) < target_size:
            prev = current
            current = spatial_chunks * i
            i += 1
        current = _closer_to_target(current, prev, target_size, itemsize)
        expanded = (
            1,
            1,
            min(data_shape[2], current[0]),
            min(data_shape[3], current[1]),
            min(data_shape[4], current[2])
        )
    else:
        raise ValueError(f"Invalid mode {mode}")

    return tuple(int(d) for d in expanded)


def _closer_to_target(
        shape1: Tuple[int, ...],
        shape2: Tuple[int, ...],
        target_bytes: int,
        itemsize: int
) -> Tuple[int, ...]:
    """
    Given two shapes with the same number of dimensions,
    find which one is closer to target_bytes.
    Args:
        shape1: the first shape
        shape2: the second shape
        target_bytes: the target size for the returned shape
        itemsize: number of bytes per array element
    """
    size1 = _get_size(shape1, itemsize)
    size2 = _get_size(shape2, itemsize)
    if abs(size1 - target_bytes) < abs(size2 - target_bytes):
        return shape1
    return shape2


def _get_size(shape: Tuple[int, ...], itemsize: int) -> int:
    """
    Return the size of an array with the given shape, in bytes
    Args:
        shape: the shape of the array
        itemsize: number of bytes per array element
    Returns:
        the size of the array, in bytes
    """
    return np.product(shape) * itemsize