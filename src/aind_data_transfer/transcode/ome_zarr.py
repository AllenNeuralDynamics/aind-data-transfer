import fnmatch
import logging
import time
from pathlib import Path
from typing import Union, List

import dask
import numpy as np
import zarr
from aicsimageio.types import PhysicalPixelSizes
from aicsimageio.writers import OmeZarrWriter
from distributed import wait
from numpy.typing import NDArray

from aind_data_transfer.util.chunk_utils import (
    guess_chunks,
    expand_chunks,
    ensure_shape_5d,
    ensure_array_5d,
)
from aind_data_transfer.util.file_utils import collect_filepaths
from aind_data_transfer.util.io_utils import (
    DataReaderFactory,
    ImarisReader,
    MissingDatasetError,
)

logging.basicConfig(format="%(asctime)s %(message)s", datefmt="%Y-%m-%d %H:%M")
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)


def write_files(
    image_paths: list,
    output: str,
    n_levels: int,
    scale_factor: float,
    overwrite: bool = True,
    chunk_size: float = 64,  # MB
    chunk_shape: tuple = None,
    voxel_size: tuple = None,
    storage_options: dict = None,
) -> list:
    """
    Write each image as a separate group to a Zarr store.
    Each group will be named according to the image filename minus extension.
    For example, if a file has path /data/tiff/tile_0_0.tif, the group name
    for that image is tile_0_0. Within each group there are n_levels arrays,
    one for each resolution level.
    Args:
        image_paths: the list of image filepaths to convert to Zarr
        output: the location of the output Zarr store (filesystem, s3, gs)
        n_levels: number of downsampling levels
        scale_factor: scale factor for downsampling in X, Y and Z
        overwrite: whether to overwrite image groups that already exist
        chunk_size: the target chunk size in MB
        chunk_shape: the chunk shape, if None will be computed from chunk_size
        voxel_size: three element tuple giving physical voxel sizes in Z,Y,X order
        storage_options: a dictionary of options to pass to the Zarr storage backend, e.g., "compressor"
    Returns:
        A list of metrics for each converted image
    """
    if chunk_shape is None and chunk_size <= 0:
        raise ValueError(
            "Either chunk_shape must be set or chunk_size must be greater than 0"
        )
    if chunk_shape is not None and any(s < 1 for s in chunk_shape):
        raise ValueError("chunk_shape must be at least 1 in all dimensions")

    if not image_paths:
        LOGGER.warning("No images found. Exiting.")
        return []

    if storage_options is None:
        storage_options = {}

    writer = OmeZarrWriter(output)

    all_metrics = []

    for impath in image_paths:
        LOGGER.info(f"Writing tile {impath}")

        tile_name = Path(impath).stem

        if not overwrite and _tile_exists(output, tile_name, n_levels):
            LOGGER.info(f"Skipping tile {tile_name}, already exists.")
            continue

        tile_metrics = {
            "tile": Path(impath).absolute(),
        }
        try:
            tile_metrics.update(storage_options["params"])
        except KeyError:
            LOGGER.warning("compression parameters will not be logged")

        # Create reader, but don't construct dask array
        # until we know the optimal chunk shape.
        reader = DataReaderFactory().create(impath)

        # TODO: pass units to zarr writer
        if voxel_size is None:
            try:
                voxel_size, unit = reader.get_voxel_size()
            except Exception:
                voxel_size = [1.0, 1.0, 1.0]
                unit = "um"
        LOGGER.info(f"Using voxel size: {voxel_size}")
        physical_pixel_sizes = PhysicalPixelSizes(
            Z=voxel_size[0], Y=voxel_size[1], X=voxel_size[2]
        )

        # Attempt to parse tile origin
        origin = _parse_origin(reader)

        # We determine the chunk size before creating the dask array since
        # rechunking an existing dask array, e.g, data = data.rechunk(chunks),
        # causes memory use to grow (unbounded?) during the zarr write step.
        # See https://github.com/dask/dask/issues/5105.
        if chunk_shape is None:
            chunks = _compute_chunks(reader, chunk_size)
        else:
            chunks = tuple(chunk_shape)

        LOGGER.info(
            f"chunks: {chunks}, {np.product(chunks) * reader.get_itemsize() / (1024 ** 2)} MiB"
        )

        tile_metrics["chunks"] = chunks

        # Get the chunk dimensions that exist in the original, un-padded image
        reader_chunks = chunks[len(chunks) - len(reader.get_shape()) :]

        pyramid = _get_or_create_pyramid(reader, n_levels, reader_chunks)

        for i in range(len(pyramid)):
            pyramid[i] = ensure_array_5d(pyramid[i])

        LOGGER.info(f"{pyramid[0]}")

        LOGGER.info("Starting write...")
        t0 = time.time()
        jobs = writer.write_multiscale(
            pyramid=pyramid,
            image_name=tile_name,
            physical_pixel_sizes=physical_pixel_sizes,
            translation=origin,
            channel_names=None,
            channel_colors=None,
            scale_factor=(scale_factor,) * 3,
            chunks=chunks,
            storage_options=storage_options,
            compute_dask=False,
        )
        if jobs:
            LOGGER.info("Computing dask arrays...")
            arrs = dask.persist(*jobs)
            wait(arrs)
        write_time = time.time() - t0

        _populate_metrics(
            tile_metrics,
            tile_name,
            output,
            _get_bytes(pyramid),
            write_time,
            n_levels,
            pyramid[0].shape,
            pyramid[0].dtype,
        )

        LOGGER.info(
            f"Finished writing tile {tile_name}.\n"
            f"Took {write_time}s. {tile_metrics['write_bps'] / (1024 ** 2)} MiB/s"
        )

        all_metrics.append(tile_metrics)

        reader.close()

    return all_metrics


def write_folder(
    input: str,
    output: str,
    n_levels: int,
    scale_factor: float,
    overwrite: bool = True,
    chunk_size: float = 64,  # MB
    chunk_shape: tuple = None,
    voxel_size: tuple = None,
    exclude: list = None,
    storage_options: dict = None,
    recursive: bool = False,
) -> list:
    """
    Write each image in the input directory as a separate group
    to a Zarr store. Each group will be named according to the image filename
    minus extension. For example, if a file has path /data/tiff/tile_0_0.tif, the group name
    for that image is tile_0_0. Within each group there are n_levels arrays, one for each resolution level.
    Args:
        input: the directory of images to convert to Zarr
        output: the location of the output Zarr store (filesystem, s3, gs)
        n_levels: number of downsampling levels
        scale_factor: scale factor for downsampling in X, Y and Z
        overwrite: whether to overwrite image groups that already exist
        chunk_size: the target chunk size in MB
        chunk_shape: the chunk shape, if None will be computed from chunk_size
        voxel_size: three element tuple giving physical voxel sizes in Z,Y,X order
        exclude: a list of filename patterns to exclude from conversion
        storage_options: a dictionary of options to pass to the Zarr storage backend, e.g., "compressor"
        recursive: whether to convert all images in all subfolders
    Returns:
        A list of metrics for each converted image
    """
    if exclude is None:
        exclude = []

    image_paths = collect_filepaths(
        input,
        recursive=recursive,
        include_exts=DataReaderFactory().VALID_EXTENSIONS,
    )

    exclude_paths = set()
    for path in image_paths:
        if any(fnmatch.fnmatch(path, pattern) for pattern in exclude):
            exclude_paths.add(path)

    image_paths = [p for p in image_paths if p not in exclude_paths]
    LOGGER.info(f"Found {len(image_paths)} images to process")

    return write_files(
        image_paths,
        output,
        n_levels,
        scale_factor,
        overwrite,
        chunk_size,
        chunk_shape,
        voxel_size,
        storage_options,
    )


def _parse_origin(reader):
    origin = None
    try:
        origin = reader.get_origin()
        while len(origin) < 5:
            origin = (0, *origin)
    except AttributeError:
        LOGGER.warning("Origin metadata could not be parsed from image")
    LOGGER.info(f"Using origin: {origin}")
    return origin


def _tile_exists(zarr_path, tile_name, n_levels):
    z = zarr.open(zarr_path, "r")
    try:
        # TODO: only re-upload missing levels
        a = z[f"{tile_name}/{n_levels - 1}"]
        return a.nbytes > 0
    except KeyError:
        return False


def _compute_chunks(reader, target_size_mb):
    target_size_bytes = target_size_mb * 1024 * 1024
    padded_chunks = ensure_shape_5d(reader.get_chunks())
    padded_shape = ensure_shape_5d(reader.get_shape())
    spatial_chunks = padded_chunks[2:]
    spatial_shape = padded_shape[2:]
    LOGGER.info(f"Using multiple of base chunk size: {padded_chunks}")
    if spatial_chunks[1:] == spatial_shape[1:]:
        LOGGER.info(
            "chunks and shape have same XY dimensions, "
            "will chunk along Z only."
        )
        chunks = guess_chunks(
            spatial_shape, target_size_bytes, reader.get_itemsize(), mode="z"
        )
    else:
        chunks = expand_chunks(
            spatial_chunks,
            spatial_shape,
            target_size_bytes,
            reader.get_itemsize(),
            mode="cycle",
        )
    chunks = ensure_shape_5d(chunks)
    return chunks


def _create_pyramid(data, n_lvls):
    from xarray_multiscale import multiscale
    from xarray_multiscale.reducers import windowed_mean

    pyramid = multiscale(
        data,
        windowed_mean,  # func
        (2,) * data.ndim,  # scale factors
        depth=n_lvls - 1,
        preserve_dtype=True,
    )
    return [arr.data for arr in pyramid]


def _get_or_create_pyramid(reader, n_levels, chunks):
    if isinstance(reader, ImarisReader):
        try:
            pyramid = reader.get_dask_pyramid(
                n_levels,
                timepoint=0,
                channel=0,
                # Use only the dimensions that exist in the base image.
                chunks=chunks,
            )
        except MissingDatasetError as e:
            LOGGER.error(e)
            LOGGER.warning(
                f"{reader.get_filepath()} does not contain all requested scales."
                f"Computing them instead..."
            )
            pyramid = _create_pyramid(
                reader.as_dask_array(chunks=chunks), n_levels
            )
    else:
        pyramid = _create_pyramid(
            reader.as_dask_array(chunks=chunks), n_levels
        )

    return pyramid


def _get_bytes(data: Union[List[NDArray], NDArray]):
    if isinstance(data, list):
        total_bytes = 0
        for arr in data:
            total_bytes += arr.nbytes
        return total_bytes
    return data.nbytes


def _get_storage_ratio(zarr_path: str, dataset_name: str):
    z = zarr.open(zarr_path, "r")
    full_res = z[f"{dataset_name}/0"]
    return full_res.nbytes / full_res.nbytes_stored


def _get_bytes_stored(zarr_path: str, dataset_name: str, n_levels: int):
    z = zarr.open(zarr_path, "r")
    total_bytes_stored = 0
    for res in range(n_levels):
        arr = z[f"{dataset_name}/{res}"]
        total_bytes_stored += arr.nbytes_stored
    return total_bytes_stored


def _populate_metrics(
    tile_metrics,
    tile_name,
    out_zarr,
    bytes_read,
    write_time,
    n_levels,
    shape,
    dtype,
):
    tile_metrics["n_levels"] = n_levels
    tile_metrics["shape"] = shape
    tile_metrics["dtype"] = dtype
    tile_metrics["write_time"] = write_time
    tile_metrics["bytes_read"] = bytes_read
    tile_metrics["write_bps"] = tile_metrics["bytes_read"] / write_time
    tile_metrics["bytes_stored"] = _get_bytes_stored(
        out_zarr, tile_name, n_levels
    )
    storage_ratio = _get_storage_ratio(out_zarr, tile_name)
    tile_metrics["storage_ratio"] = storage_ratio
    LOGGER.info(f"Compress ratio: {storage_ratio}")
