import fnmatch
import logging
import time
from pathlib import Path
from typing import List, Optional, Dict, cast

import s3fs
import zarr
from numcodecs.abc import Codec
from numpy.typing import NDArray
from ome_zarr.format import CurrentFormat
from ome_zarr.writer import write_multiscales_metadata
from xarray_multiscale import multiscale
from xarray_multiscale.reducers import windowed_mean, WindowedReducer

from aind_data_transfer.transformations.deinterleave import (
    ChannelParser, Deinterleave,
)
from aind_data_transfer.transformations.flatfield_correction import (
    BkgSubtraction,
)
from aind_data_transfer.util.chunk_utils import *
from aind_data_transfer.util.file_utils import collect_filepaths
from aind_data_transfer.util.io_utils import (
    DataReaderFactory, ImarisReader, DataReader, BlockedArrayWriter,
)

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)


_MAX_S3_RETRIES = 1000
_S3_RETRY_MODE = "adaptive"


def write_files(
    image_paths: list,
    output: str,
    n_levels: int,
    scale_factor: int,
    overwrite: bool = True,
    chunk_size: float = 64,  # MB
    chunk_shape: tuple = None,
    voxel_size: tuple = None,
    compressor: Codec = None,
    bkg_img_dir: Optional[str] = None,
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

    if output.startswith("s3://"):
        s3 = s3fs.S3FileSystem(
            anon=False,
            config_kwargs={
                'retries': {
                    'total_max_attempts': _MAX_S3_RETRIES,
                    'mode': _S3_RETRY_MODE,
                }
            },
            use_ssl=True,
        )
        # Create a Zarr group on S3
        store = s3fs.S3Map(root=output, s3=s3, check=False)
    else:
        store = zarr.DirectoryStore(output, dimension_separator="/")

    root_group = zarr.group(store=store, overwrite=False)

    all_metrics = []

    check_voxel_size = voxel_size is None

    for impath in sorted(image_paths):
        LOGGER.info(f"Writing tile {impath}")

        # Create readers, but don't construct dask array
        # until we know the optimal chunk shape.
        with DataReaderFactory().create(impath) as reader:
            # TODO: pass units to zarr writer
            if check_voxel_size:
                try:
                    voxel_size, _ = reader.get_voxel_size()
                except Exception:
                    voxel_size = [1.0, 1.0, 1.0]
            LOGGER.info(f"Using voxel size: {voxel_size}")

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
                    root_group,
                    output,
                    channel_names,
                    n_levels,
                    (scale_factor,) * len(reader.get_shape()),
                    origin,
                    voxel_size,
                    chunks,
                    overwrite,
                    compressor,
                )
                all_metrics.append(tile_metrics)
            else:
                tile_metrics = _store_file(
                    reader,
                    root_group,
                    output,
                    n_levels,
                    (scale_factor,) * len(reader.get_shape()),
                    origin,
                    voxel_size,
                    chunks,
                    overwrite,
                    compressor,
                    bkg_img_dir=bkg_img_dir,
                )
                all_metrics.append(tile_metrics)

    return all_metrics


def _store_file(
    reader: DataReader,
    root_group: zarr.Group,
    output: str,
    n_levels: int,
    scale_factors: tuple,
    origin: list,
    voxel_size: tuple,
    chunks: tuple,
    overwrite: bool,
    compressor: Codec = None,
    bkg_img_dir: Optional[str] = None,
) -> dict:
    """
    Write an image to an existing Zarr store

    Args:
        reader: the reader for the image
        root_group: the top-level zarr Group
        output: the uri of the output Zarr store (filesystem, s3, gs)
        n_levels: number of downsampling levels
        scale_factors: scale factors for downsampling in Z,Y,X
        origin: the tile origin coordinates in ZYX order
        voxel_size: the voxel size of the image
        chunks: the chunk shape
        overwrite: whether to overwrite image groups that already exist
        compressor: numcodecs Codec instance to use for compression
        bkg_img_dir: the directory containing the background image Tiff file for each raw image.
                     If None, will not perform background subtraction.
    Returns:
        A list of metrics for each converted image
    """
    tile_name = Path(reader.get_filepath()).stem + ".zarr"
    LOGGER.info(f"new tile name: {tile_name}")

    tile_metrics = {"chunks": chunks, "tile": tile_name}

    if not overwrite and _tile_exists(output, tile_name, n_levels):
        LOGGER.info(f"Skipping tile {tile_name}, already exists.")
        return tile_metrics

    reader_chunks = chunks[len(chunks) - len(reader.get_shape()) :]

    group = root_group.create_group(tile_name, overwrite=True)

    if isinstance(reader, ImarisReader) and reader.n_levels >= n_levels:
        # Use the pre-computed pyramid in the Imaris file
        pyramid = reader.get_dask_pyramid(
            n_levels,
            timepoint=0,
            channel=0,
            # Use only the dimensions that exist in the base image.
            chunks=reader_chunks,
        )

        if bkg_img_dir is not None:
            bkg = BkgSubtraction.darray_from_bkg_path(
                BkgSubtraction.get_bkg_path(
                    reader.get_filepath(), bkg_img_dir
                ),
                reader.get_shape()[-2:],
                chunks[-2:],
            )
            # keep background image in distributed memory
            bkg = bkg.persist()
            bkg_img_pyramid = create_pyramid(bkg, n_levels, scale_factors[1:])
            for i in range(len(bkg_img_pyramid)):
                pyramid[i] = BkgSubtraction.subtract(
                    pyramid[i],
                    bkg_img_pyramid[i],
                )

        # The background subtraction can change the chunks,
        # so rechunk before storing
        pyramid = [ensure_array_5d(arr).rechunk(chunks) for arr in pyramid]
        LOGGER.info(f"input array: {pyramid[0]}")
        LOGGER.info(f"input array size: {pyramid[0].nbytes / 2 ** 20} MiB")

        block_shape = ensure_shape_5d(
            BlockedArrayWriter.get_block_shape(pyramid[0])
        )
        LOGGER.info(f"block shape: {block_shape}")

        write_ome_ngff_metadata(
            group,
            pyramid[0],
            tile_name,
            n_levels,
            scale_factors,
            voxel_size,
            origin,
        )

        t0 = time.time()
        for arr_index, arr in enumerate(pyramid):
            store_array(arr, group, str(arr_index), block_shape, compressor)
        write_time = time.time() - t0

    else:
        # Downsample from scratch
        arr = reader.as_dask_array(chunks=reader_chunks)

        if bkg_img_dir is not None:
            bkg = BkgSubtraction.darray_from_bkg_path(
                BkgSubtraction.get_bkg_path(
                    reader.get_filepath(), bkg_img_dir
                ),
                reader.get_shape()[-2:],
                chunks[-2:],
            )
            # keep background image in distributed memory
            bkg = bkg.persist()
            arr = BkgSubtraction.subtract(arr, bkg)

        arr = ensure_array_5d(arr)
        # The background subtraction can change the chunks,
        # so rechunk before storing
        arr = arr.rechunk(chunks)

        t0 = time.time()
        pyramid = _gen_and_store_pyramid(
            arr,
            group,
            tile_name,
            n_levels,
            scale_factors,
            voxel_size,
            origin,
            compressor,
        )
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
    root_group: zarr.Group,
    output: str,
    channel_names: list,
    n_levels: int,
    scale_factors: tuple,
    origin: list,
    voxel_size: tuple,
    chunks: tuple,
    overwrite: bool,
    compressor: Codec = None,
) -> dict:
    """
    Write an image to an existing Zarr store

    Args:
        reader: the reader for the image
        root_group: the root Zarr group in which to store the Array
        output: the location of the output Zarr store (filesystem, s3, gs)
        channel_names: the wavelength string of each channel, e.g., ["488, "561","689"]
        n_levels: number of downsampling levels to compute
        scale_factors: scale factors for downsampling in X, Y and Z
        origin: the tile origin coordinates in ZYX order
        voxel_size: the voxel size of the image
        chunks: the chunk shape
        overwrite: whether to overwrite image groups that already exist
        compressor: numcodecs Codec instance to use for compression
    Returns:
        A list of metrics for each converted image
    """
    tile_metrics = {"chunks": chunks}

    # Get the chunk dimensions that exist in the original, un-padded image
    reader_chunks = chunks[len(chunks) - len(reader.get_shape()) :]
    channels = Deinterleave.deinterleave(
        reader.as_dask_array(reader_chunks), len(channel_names), axis=0
    )
    for channel_idx, channel in enumerate(channels):
        tile_prefix = ChannelParser.parse_tile_xyz_loc(reader.get_filepath())
        tile_name = tile_prefix + f"_ch_{channel_names[channel_idx]}" + ".zarr"
        LOGGER.info(f"new tile name: {tile_name}")

        if not overwrite and _tile_exists(output, tile_name, n_levels):
            LOGGER.info(f"Skipping tile {tile_name}, already exists.")
            continue

        group = root_group.create_group(tile_name, overwrite=True)

        tile_metrics["tile"] = tile_name

        channel = ensure_array_5d(channel)
        # Deinterleaving divides the chunk size in Z by the number
        # of channels, so rechunk back to the desired shape
        channel = channel.rechunk(chunks)

        t0 = time.time()
        pyramid = _gen_and_store_pyramid(
            channel,
            group,
            tile_name,
            n_levels,
            scale_factors,
            voxel_size,
            origin,
            compressor,
        )
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


def _gen_and_store_pyramid(
    arr,
    group,
    tile_name,
    n_levels,
    scale_factors,
    voxel_size,
    origin,
    compressor,
):
    """
    Progressively downsample the input array and store the results as separate arrays in a Zarr group.

    Parameters
    ----------
    arr : da.Array
        The input Dask array.
    group : zarr.Group
        The output Zarr group.
    tile_name: str
        The name of the tile.
    n_lvls : int
        The number of pyramid levels.
    scale_factors : Tuple
        The scale factors for downsampling along each dimension.
    voxel_size: Tuple
        The physical voxel size in Z, Y, X order.
    origin: Tuple
        The origin of the tile in Z, Y, X order.
    compressor : numcodecs.abc.Codec, optional
        The compression codec to use for the output Zarr array.
    """
    LOGGER.info(f"input array: {arr}")
    LOGGER.info(f"input array size: {arr.nbytes / 2 ** 20} MiB")

    write_ome_ngff_metadata(
        group,
        arr,
        tile_name,
        n_levels,
        scale_factors[-3:],  # must be 3D
        voxel_size[-3:],  # must be 3D
        origin,
    )

    scale_factors = ensure_shape_5d(scale_factors)

    block_shape = ensure_shape_5d(BlockedArrayWriter.get_block_shape(arr))
    LOGGER.info(f"block shape: {block_shape}")

    store_array(arr, group, "0", block_shape, compressor)
    pyramid = downsample_and_store(
        arr, group, n_levels, scale_factors, block_shape, compressor
    )

    return pyramid


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
    compressor: Codec = None,
    recursive: bool = False,
    bkg_img_dir: Optional[str] = None,
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
        compressor: numcodecs Codec instance to use for compression
        recursive: whether to convert all images in all subfolders
        bkg_img_dir: the directory containing the background image Tiff file for each raw image.
                     If None, will not perform background subtraction.
    Returns:
        A list of metrics for each converted image
    """
    if exclude is None:
        exclude = []

    image_paths = list(
        collect_filepaths(
            input,
            recursive=recursive,
            include_exts=DataReaderFactory().VALID_EXTENSIONS,
        )
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
        compressor,
        bkg_img_dir=bkg_img_dir,
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
    spatial_chunks = reader.get_chunks()[-3:]
    spatial_shape = reader.get_shape()[-3:]
    LOGGER.info(f"Using multiple of base chunk size: {spatial_chunks}")
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
    return ensure_shape_5d(chunks)


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


def _build_ome(
    data_shape: Tuple[int, ...],
    image_name: str,
    channel_names: Optional[List[str]] = None,
    channel_colors: Optional[List[int]] = None,
    channel_minmax: Optional[List[Tuple[float, float]]] = None,
) -> Dict:
    """
    Create the necessary metadata for an OME tiff image

    Parameters
    ----------
    data_shape: A 5-d tuple, assumed to be TCZYX order
    image_name: The name of the image
    channel_names: The names for each channel
    channel_colors: List of all channel colors
    channel_minmax: List of all (min, max) pairs of channel intensities

    Returns
    -------
    Dict: An "omero" metadata object suitable for writing to ome-zarr
    """
    if channel_names is None:
        channel_names = [
            f"Channel:{image_name}:{i}" for i in range(data_shape[1])
        ]
    if channel_colors is None:
        channel_colors = [i for i in range(data_shape[1])]
    if channel_minmax is None:
        channel_minmax = [(0.0, 1.0) for _ in range(data_shape[1])]
    ch = []
    for i in range(data_shape[1]):
        ch.append(
            {
                "active": True,
                "coefficient": 1,
                "color": f"{channel_colors[i]:06x}",
                "family": "linear",
                "inverted": False,
                "label": channel_names[i],
                "window": {
                    "end": float(channel_minmax[i][1]),
                    "max": float(channel_minmax[i][1]),
                    "min": float(channel_minmax[i][0]),
                    "start": float(channel_minmax[i][0]),
                },
            }
        )

    omero = {
        "id": 1,  # ID in OMERO
        "name": image_name,  # Name as shown in the UI
        "version": "0.4",  # Current version
        "channels": ch,
        "rdefs": {
            "defaultT": 0,  # First timepoint to show the user
            "defaultZ": data_shape[2] // 2,  # First Z section to show the user
            "model": "color",  # "color" or "greyscale"
        },
    }
    return omero


def _compute_scales(
    scale_num_levels: int,
    scale_factor: Tuple[float, float, float],
    pixelsizes: Tuple[float, float, float],
    chunks: Tuple[int, int, int, int, int],
    data_shape: Tuple[int, int, int, int, int],
    translation: Optional[List[float]] = None,
) -> Tuple[List, List]:
    """Generate the list of coordinate transformations and associated chunk options.

    Parameters
    ----------
    scale_num_levels: the number of downsampling levels
    scale_factor: a tuple of scale factors in each spatial dimension (Z, Y, X)
    pixelsizes: a list of pixel sizes in each spatial dimension (Z, Y, X)
    chunks: a 5D tuple of integers with size of each chunk dimension (T, C, Z, Y, X)
    data_shape: a 5D tuple of the full resolution image's shape
    translation: a 5 element list specifying the offset in physical units in each dimension

    Returns
    -------
    A tuple of the coordinate transforms and chunk options
    """
    transforms = [
        [
            # the voxel size for the first scale level
            {
                "type": "scale",
                "scale": [
                    1.0,
                    1.0,
                    pixelsizes[0],
                    pixelsizes[1],
                    pixelsizes[2],
                ],
            }
        ]
    ]
    if translation is not None:
        transforms[0].append(
            {"type": "translation", "translation": translation}
        )
    chunk_sizes = []
    lastz = data_shape[2]
    lasty = data_shape[3]
    lastx = data_shape[4]
    opts = dict(
        chunks=(
            1,
            1,
            min(lastz, chunks[2]),
            min(lasty, chunks[3]),
            min(lastx, chunks[4]),
        )
    )
    chunk_sizes.append(opts)
    if scale_num_levels > 1:
        for i in range(scale_num_levels - 1):
            last_transform = transforms[-1][0]
            last_scale = cast(List, last_transform["scale"])
            transforms.append(
                [
                    {
                        "type": "scale",
                        "scale": [
                            1.0,
                            1.0,
                            last_scale[2] * scale_factor[0],
                            last_scale[3] * scale_factor[1],
                            last_scale[4] * scale_factor[2],
                        ],
                    }
                ]
            )
            if translation is not None:
                transforms[-1].append(
                    {"type": "translation", "translation": translation}
                )
            lastz = int(math.ceil(lastz / scale_factor[0]))
            lasty = int(math.ceil(lasty / scale_factor[1]))
            lastx = int(math.ceil(lastx / scale_factor[2]))
            opts = dict(
                chunks=(
                    1,
                    1,
                    min(lastz, chunks[2]),
                    min(lasty, chunks[3]),
                    min(lastx, chunks[4]),
                )
            )
            chunk_sizes.append(opts)

    return transforms, chunk_sizes


def _get_axes_5d(
    time_unit: str = "millisecond", space_unit: str = "micrometer"
) -> List[Dict]:
    """Generate the list of axes.

    Parameters
    ----------
    time_unit: the time unit string, e.g., "millisecond"
    space_unit: the space unit string, e.g., "micrometer"

    Returns
    -------
    A list of dictionaries for each axis
    """
    axes_5d = [
        {"name": "t", "type": "time", "unit": f"{time_unit}"},
        {"name": "c", "type": "channel"},
        {"name": "z", "type": "space", "unit": f"{space_unit}"},
        {"name": "y", "type": "space", "unit": f"{space_unit}"},
        {"name": "x", "type": "space", "unit": f"{space_unit}"},
    ]
    return axes_5d


def create_pyramid(
    arr: Union[np.ndarray, da.Array],
    n_lvls: int,
    scale_factors: tuple,
    chunks: Union[str, tuple] = "preserve",
    reducer: WindowedReducer = windowed_mean,
) -> list:
    """
    Create a multiscale pyramid of the input data.

    Parameters
    ----------
    arr : array-like
        The input data to be downsampled.
    n_lvls : int
        The number of pyramid levels to generate.
    scale_factors : tuple
        The scale factors for downsampling along each dimension.
    chunks : tuple
        The chunk size to use for the output arrays.

    Returns
    -------
    list
        A list of Dask arrays representing the pyramid levels.
    """
    pyramid = multiscale(
        array=arr,
        reduction=reducer,  # func
        scale_factors=scale_factors,
        preserve_dtype=True,
        chunks=chunks,
    )[:n_lvls]

    return [arr.data for arr in pyramid]


def store_array(
    arr: da.Array,
    group: zarr.Group,
    path: str,
    block_shape: tuple,
    compressor: Codec = None,
    dimension_separator: str = "/",
) -> zarr.Array:
    """
    Store the full resolution layer of a Dask pyramid into a Zarr group.

    Parameters
    ----------
    arr : da.Array
        The input Dask array.
    group : zarr.Group
        The output Zarr group.
    block_shape : Tuple
        The shape of blocks to use for partitioning the array.
    compressor : numcodecs.abc.Codec, optional
        The compression codec to use for the output Zarr array. Default is Blosc with "zstd" method and compression
        level 1.

    Returns
    -------
    zarr.Array
        The output Zarr array.
    """
    ds = group.create_dataset(
        path,
        shape=arr.shape,
        chunks=arr.chunksize,
        dtype=arr.dtype,
        compressor=compressor,
        dimension_separator=dimension_separator,
        overwrite=True,
    )

    BlockedArrayWriter.store(arr, ds, block_shape)

    return ds


def downsample_and_store(
    arr: da.Array,
    group: zarr.Group,
    n_lvls: int,
    scale_factors: Tuple,
    block_shape: Tuple,
    compressor: Codec = None,
    reducer: WindowedReducer = windowed_mean,
) -> list:
    """
    Progressively downsample the input array and store the results as separate arrays in a Zarr group.

    Parameters
    ----------
    arr : da.Array
        The full-resolution Dask array.
    group : zarr.Group
        The output Zarr group.
    n_lvls : int
        The number of pyramid levels.
    scale_factors : Tuple
        The scale factors for downsampling along each dimension.
    block_shape : Tuple
        The shape of blocks to use for partitioning the array.
    compressor : numcodecs.abc.Codec, optional
        The compression codec to use for the output Zarr array. Default is Blosc with "zstd" method and compression
        level 1.
    """
    pyramid = [arr]

    for arr_index in range(1, n_lvls):
        first_mipmap = _get_first_mipmap_level(arr, scale_factors, reducer)

        ds = group.create_dataset(
            str(arr_index),
            shape=first_mipmap.shape,
            chunks=first_mipmap.chunksize,
            dtype=first_mipmap.dtype,
            compressor=compressor,
            dimension_separator="/",
            overwrite=True,
        )

        BlockedArrayWriter.store(first_mipmap, ds, block_shape)

        arr = da.from_array(ds, ds.chunks)
        pyramid.append(arr)

    return pyramid


def _get_first_mipmap_level(
    arr: da.Array,
    scale_factors: tuple,
    reducer: WindowedReducer = windowed_mean,
) -> da.Array:
    """
    Generate a mipmap pyramid from the input array and return the first mipmap level.

    Parameters:
    - arr: dask.array.Array
        The input array for which the mipmap pyramid is to be generated.
    - scale_factors: tuple
        A 5D tuple of scale factors for each dimension used to generate the mipmap pyramid.

    Returns:
    - dask.array.Array
        The first mipmap level of the input array.
    """
    n_lvls = 2
    pyramid = create_pyramid(
        arr, n_lvls, scale_factors, arr.chunksize, reducer
    )
    return ensure_array_5d(pyramid[1])


def write_ome_ngff_metadata(
    group: zarr.Group,
    arr: da.Array,
    image_name: str,
    n_lvls: int,
    scale_factors: tuple,
    voxel_size: tuple,
    origin: list = None,
    metadata: dict = None,
) -> None:
    """
    Write OME-NGFF metadata to a Zarr group.

    Parameters
    ----------
    group : zarr.Group
        The output Zarr group.
    arr : array-like
        The input array.
    image_name : str
        The name of the image.
    n_lvls : int
        The number of pyramid levels.
    scale_factors : tuple
        The scale factors for downsampling along each dimension.
    voxel_size : tuple
        The voxel size along each dimension.
    """
    if metadata is None:
        metadata = {}
    fmt = CurrentFormat()
    ome_json = _build_ome(
        arr.shape,
        image_name,
        channel_names=None,
        channel_colors=None,
        channel_minmax=None,
    )
    group.attrs["omero"] = ome_json
    axes_5d = _get_axes_5d()
    coordinate_transformations, chunk_opts = _compute_scales(
        n_lvls, scale_factors, voxel_size, arr.chunksize, arr.shape, origin
    )
    fmt.validate_coordinate_transformations(
        arr.ndim, n_lvls, coordinate_transformations
    )
    datasets = [{"path": str(i)} for i in range(n_lvls)]
    if coordinate_transformations is not None:
        for dataset, transform in zip(datasets, coordinate_transformations):
            dataset["coordinateTransformations"] = transform

    write_multiscales_metadata(group, datasets, fmt, axes_5d, **metadata)
