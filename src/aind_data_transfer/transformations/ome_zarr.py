import fnmatch
import logging
import time
from pathlib import Path
from typing import List, Optional

import dask
import dask.array
import tifffile
import zarr
from aicsimageio.types import PhysicalPixelSizes
from aicsimageio.writers import OmeZarrWriter
from distributed import wait
from numpy.typing import NDArray
from xarray_multiscale import multiscale
from xarray_multiscale.reducers import windowed_mean

from aind_data_transfer.util.chunk_utils import *
from aind_data_transfer.util.file_utils import collect_filepaths
from aind_data_transfer.util.io_utils import (
    DataReaderFactory,
    ImarisReader,
    MissingDatasetError,
    DataReader
)
from aind_data_transfer.transformations.deinterleave import ChannelParser, Deinterleave
from aind_data_transfer.transformations.flatfield_correction import BkgSubtraction

logging.basicConfig(format="%(asctime)s %(message)s", datefmt="%Y-%m-%d %H:%M")
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)


def write_files(
    image_paths: list,
    output: str,
    n_levels: int,
    scale_factor: int,
    overwrite: bool = True,
    chunk_size: float = 64,  # MB
    chunk_shape: tuple = None,
    voxel_size: tuple = None,
    storage_options: dict = None,
    bkg_img_dir: Optional[str] = None
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
        bkg_img_dir: the directory containing the background image Tiff file for each raw image.
                     If None, will not perform background subtraction.
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

    for impath in sorted(image_paths):
        LOGGER.info(f"Writing tile {impath}")

        # Create readers, but don't construct dask array
        # until we know the optimal chunk shape.
        with DataReaderFactory().create(impath) as reader:
            # TODO: pass units to zarr writer
            if voxel_size is None:
                try:
                    voxel_size, _ = reader.get_voxel_size()
                except Exception:
                    voxel_size = [1.0, 1.0, 1.0]
            LOGGER.info(f"Using voxel size: {voxel_size}")
            physical_pixel_sizes = PhysicalPixelSizes(
                Z=voxel_size[0], Y=voxel_size[1], X=voxel_size[2]
            )

            # Attempt to parse tile origin
            origin = _parse_origin(reader)

            if chunk_shape is None:
                chunks = _compute_chunks(reader, chunk_size)
            else:
                chunks = tuple(chunk_shape)

            LOGGER.info(
                f"chunks: {chunks}, {np.product(chunks) * reader.get_itemsize() / (1024 ** 2)} MiB"
            )

            try:
                channel_names = ChannelParser.parse_channel_names(impath)
                LOGGER.info(f"parsed channel wavelengths: {channel_names}")
                interleaved = len(channel_names) > 1
            except ValueError:
                interleaved = False
            LOGGER.info(f"Data is interleaved: {interleaved}")

            if interleaved:
                LOGGER.info("Deinterleaving during write")
                tile_metrics = _store_interleaved_file(
                    reader,
                    writer,
                    output,
                    channel_names,
                    n_levels,
                    scale_factor,
                    origin,
                    physical_pixel_sizes,
                    chunks,
                    overwrite,
                    storage_options
                )
                all_metrics.append(tile_metrics)
            else:
                tile_metrics = _store_file(
                    reader,
                    writer,
                    output,
                    n_levels,
                    scale_factor,
                    origin,
                    physical_pixel_sizes,
                    chunks,
                    overwrite,
                    storage_options,
                    bkg_img_dir=bkg_img_dir
                )
                all_metrics.append(tile_metrics)

    return all_metrics


def _store_file(
        reader: DataReader,
        writer: OmeZarrWriter,
        output: str,
        n_levels: int,
        scale_factor: int,
        origin: list,
        physical_pixel_sizes: PhysicalPixelSizes,
        chunks: tuple,
        overwrite: bool,
        storage_options: dict,
        bkg_img_dir: Optional[str] = None
) -> dict:
    """
    Write an image to an existing Zarr store

    Args:
        reader: the reader for the image
        writer: the OmeZarrWriter instance
        output: the location of the output Zarr store (filesystem, s3, gs)
        n_levels: number of downsampling levels
        scale_factor: scale factor for downsampling in X, Y and Z
        origin: the tile origin coordinates in ZYX order
        physical_pixel_sizes: the voxel size of the image
        chunks: the chunk shape
        overwrite: whether to overwrite image groups that already exist
        storage_options: a dictionary of options to pass to the Zarr storage backend, e.g., "compressor"
        bkg_img_dir: the directory containing the background image Tiff file for each raw image.
                     If None, will not perform background subtraction.
    Returns:
        A list of metrics for each converted image
    """
    tile_name = Path(reader.get_filepath()).stem + ".zarr"
    LOGGER.info(f"new tile name: {tile_name}")

    tile_metrics = {"chunks": chunks, "tile": tile_name}
    try:
        tile_metrics.update(storage_options["params"])
    except KeyError:
        LOGGER.warning("compression parameters will not be logged")

    if not overwrite and _tile_exists(output, tile_name, n_levels):
        LOGGER.info(f"Skipping tile {tile_name}, already exists.")
        return tile_metrics

    # Get the chunk dimensions that exist in the original, un-padded image
    reader_chunks = chunks[len(chunks) - len(reader.get_shape()):]

    pyramid = _get_or_create_pyramid(reader, n_levels, reader_chunks)

    if bkg_img_dir is not None:
        bkg_img_pyramid = _create_pyramid(
            tifffile.imread(BkgSubtraction.get_bkg_path(reader.get_filepath(), bkg_img_dir)),
            n_levels
        )
        for i in range(len(bkg_img_pyramid)):
            pyramid[i] = BkgSubtraction.subtract(
                pyramid[i],
                da.from_array(bkg_img_pyramid[i], chunks=(256, 256))
            )

    pyramid = [ensure_array_5d(arr) for arr in pyramid]

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

    return tile_metrics


def _store_interleaved_file(
        reader: DataReader,
        writer: OmeZarrWriter,
        output: str,
        channel_names: list,
        n_levels: int,
        scale_factor: int,
        origin: list,
        physical_pixel_sizes: PhysicalPixelSizes,
        chunks: tuple,
        overwrite: bool,
        storage_options: dict,
) -> dict:
    """
    Write an image to an existing Zarr store

    Args:
        reader: the reader for the image
        writer: the OmeZarrWriter instance
        output: the location of the output Zarr store (filesystem, s3, gs)
        channel_names: the wavelength string of each channel, e.g., ["488, "561","689"]
        n_levels: number of downsampling levels to compute
        scale_factor: scale factor for downsampling in X, Y and Z
        origin: the tile origin coordinates in ZYX order
        physical_pixel_sizes: the voxel size of the image
        chunks: the chunk shape
        overwrite: whether to overwrite image groups that already exist
        storage_options: a dictionary of options to pass to the Zarr storage backend, e.g., "compressor"
    Returns:
        A list of metrics for each converted image
    """
    tile_metrics = {"chunks": chunks}
    try:
        tile_metrics.update(storage_options["params"])
    except KeyError:
        LOGGER.warning("compression parameters will not be logged")

    # Get the chunk dimensions that exist in the original, un-padded image
    reader_chunks = chunks[len(chunks) - len(reader.get_shape()):]
    channels = Deinterleave.deinterleave(
        reader.as_dask_array(reader_chunks),
        len(channel_names),
        axis=0
    )
    for channel_idx, channel in enumerate(channels):
        tile_prefix = ChannelParser.parse_tile_xyz_loc(reader.get_filepath())
        tile_name = tile_prefix + f"_ch_{channel_names[channel_idx]}" + ".zarr"
        LOGGER.info(f"new tile name: {tile_name}")

        if not overwrite and _tile_exists(output, tile_name, n_levels):
            LOGGER.info(f"Skipping tile {tile_name}, already exists.")
            continue

        tile_metrics["tile"] = tile_name

        channel_pyramid = _create_pyramid(channel, n_levels, chunks=reader_chunks)
        channel_pyramid = [ensure_array_5d(arr) for arr in channel_pyramid]

        LOGGER.info(f"{channel_pyramid[0]}")

        LOGGER.info("Starting write...")
        t0 = time.time()
        jobs = writer.write_multiscale(
            pyramid=channel_pyramid,
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
            _get_bytes(channel_pyramid),
            write_time,
            n_levels,
            channel_pyramid[0].shape,
            channel_pyramid[0].dtype,
        )

        LOGGER.info(
            f"Finished writing tile {tile_name}.\n"
            f"Took {write_time}s. {tile_metrics['write_bps'] / (1024 ** 2)} MiB/s"
        )

    return tile_metrics


def write_folder(
    input: str,
    output: str,
    n_levels: int,
    scale_factor: int,
    overwrite: bool = True,
    chunk_size: float = 64,  # MB
    chunk_shape: tuple = None,
    voxel_size: tuple = None,
    exclude: list = None,
    storage_options: dict = None,
    recursive: bool = False,
    bkg_img_dir: Optional[str] = None
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
        bkg_img_dir: the directory containing the background image Tiff file for each raw image.
                     If None, will not perform background subtraction.
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
        bkg_img_dir=bkg_img_dir
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


def _create_pyramid(
    data: Union[NDArray, dask.array.Array], n_lvls: int, chunks: Union[str, tuple] = "preserve"
) -> List[Union[NDArray, dask.array.Array]]:
    """
    Create a lazy multiscale image pyramid using data as the full-resolution layer.
    To evaluate the result, call dask.compute(*pyramid)

    Args:
        data: the numpy or dask array to create a pyramid from.
        This will be used as the highest resolution layer.
        n_lvls: the number of pyramid levels to produce
    Returns:
        A list of dask or numpy arrays, ordered from highest resolution to lowest
    """
    pyramid = multiscale(
        data,
        windowed_mean,  # func
        (2,) * data.ndim,  # scale factors
        preserve_dtype=True,
        chunks=chunks
    )[:n_lvls]
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
                reader.as_dask_array(chunks=chunks), n_levels, chunks
            )
    else:
        pyramid = _create_pyramid(
            reader.as_dask_array(chunks=chunks), n_levels, chunks
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
