import argparse
import logging
import re
import time
from pathlib import Path
from typing import List

import dask.array as da
import xarray_multiscale
import zarr
from numcodecs import blosc
from natsort import natsorted

from aind_data_transfer.transformations.ome_zarr import (
    downsample_and_store,
    store_array,
    write_ome_ngff_metadata,
)
from aind_data_transfer.util.chunk_utils import (
    ensure_array_5d,
    ensure_shape_5d,
)
from aind_data_transfer.util.dask_utils import get_client
from aind_data_transfer.util.io_utils import BlockedArrayWriter
from xarray_multiscale import windowed_mean

blosc.use_threads = False

logging.basicConfig(format="%(asctime)s %(message)s", datefmt="%Y-%m-%d %H:%M")
_LOGGER = logging.getLogger(__name__)
_LOGGER.setLevel(logging.INFO)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert an N5 dataset to OME-Zarr."
    )
    parser.add_argument(
        "--n5", type=str, help="Path to the input N5 container.", required=True
    )
    parser.add_argument(
        "--output-zarr",
        type=str,
        help="Path to the output Zarr array.",
        required=True,
    )
    parser.add_argument(
        "--voxel-size", type=float, nargs="+", default=[1.0, 1.0, 1.0]
    )
    parser.add_argument("--n-levels", type=int, default=1)
    parser.add_argument(
        "--scale-factors", type=int, nargs=3, default=[2, 2, 2]
    )
    parser.add_argument("--deployment", type=str, default="slurm")

    return parser.parse_args()


def get_n5_scales(n5_group: zarr.Group) -> List[da.Array]:
    """
    Returns a list of Dask arrays corresponding to scale datasets in an N5 container,
    where the dataset name matches the pattern "s" followed by one or more digits.

    Parameters
    ----------
    n5_group : Group
        A Group in an N5/Zarr container.

    Returns
    -------
    list
        A list of Dask arrays corresponding to the scales.
    """
    scales = []
    pattern = re.compile("^s[0-9]+$")
    for item in natsorted(n5_group.keys()):
        if pattern.match(item):
            _LOGGER.info(f"Found scale {item}")
            ds = n5_group[item]
            scales.append(da.from_array(ds, chunks=ds.chunks))
    return scales


def main():
    args = parse_arguments()

    n_levels = args.n_levels
    if n_levels <= 0:
        raise ValueError("Number of levels must be > 0")

    scale_factors = args.scale_factors
    if len(scale_factors) != 3:
        raise ValueError("Scale factors must be 3D")
    if any(s <= 0 for s in scale_factors):
        raise ValueError("Scale factors must be > 0")

    voxel_size = args.voxel_size
    if len(voxel_size) != 3:
        raise ValueError("Voxel size must be 3D")
    if any(s <= 0 for s in voxel_size):
        raise ValueError("Voxel size must be > 0")

    client, _ = get_client(deployment=args.deployment)

    # Load the input Dask array(s) from N5 container
    z = zarr.open(zarr.N5FSStore(args.n5), "r")
    scales = get_n5_scales(z)
    if not scales:
        _LOGGER.error("No scales found")
        return

    scales = [ensure_array_5d(scale) for scale in scales]
    _LOGGER.info(f"input array: {scales[0]}")
    _LOGGER.info(f"input array size: {scales[0].nbytes / 2 ** 20} MiB")

    # TODO: let user set chunk shape?

    block_shape = ensure_shape_5d(
        BlockedArrayWriter.get_block_shape(scales[0], target_size_mb=409600)
    )
    _LOGGER.info(f"block shape: {block_shape}")

    scale_factors = ensure_shape_5d(scale_factors)
    _LOGGER.info(f"scale factors: {scale_factors}")

    _LOGGER.info(f"Writing OME-Zarr to {args.output_zarr}")

    root_group: zarr.Group = zarr.open(args.output_zarr, mode="w")

    codec = blosc.Blosc(cname="zstd", clevel=1, shuffle=blosc.SHUFFLE)

    metadata = {}

    t0 = time.time()
    if len(scales) < n_levels:
        # downsample from scratch
        reducer = windowed_mean
        metadata["metadata"] = {
            "description": "Pyramid generated with xarray_multiscale",
            "method": f"{reducer.__module__}.{reducer.__name__}",
            "version": xarray_multiscale.__version__,
            "args": None,
            "kwargs": {},
        }
        _LOGGER.info(
            "Num existing scales is less than num levels, downsampling from scratch."
        )
        store_array(scales[0], root_group, "0", block_shape, codec)
        downsample_and_store(
            scales[0],
            root_group,
            n_levels,
            scale_factors,
            block_shape,
            codec,
            reducer,
        )

    else:
        for i, s in enumerate(scales):
            store_array(s, root_group, str(i), block_shape, codec)
            if i == n_levels - 1:
                break
    write_ome_ngff_metadata(
        root_group,
        scales[0],
        Path(args.output_zarr).stem,
        n_levels,
        scale_factors[-3:],  # must be 3D
        tuple(reversed(args.voxel_size)),  # must be 3D ZYX
        metadata=metadata,
    )
    _LOGGER.info(f"Done. Took {time.time() - t0} seconds")


if __name__ == "__main__":
    main()
