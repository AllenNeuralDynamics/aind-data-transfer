import argparse
import fnmatch
import logging
import os
import time
from pathlib import Path
from typing import List

import tifffile

from aind_data_transfer.transcode.ome_zarr import _compute_chunks
from aind_data_transfer.util.io_utils import DataReaderFactory
from aind_data_transfer.util.file_utils import collect_filepaths
from aind_data_transfer.util.dask_utils import get_client

logging.basicConfig(format="%(asctime)s %(message)s", datefmt="%Y-%m-%d %H:%M")
_LOGGER = logging.getLogger(__name__)
_LOGGER.setLevel(logging.INFO)


_AXES = {
    "XY": 0,
    "XZ": 1,
    "YZ": 2
}


class HDF5PluginError(Exception):
    pass


def _find_hdf5plugin_path():
    # this should work with both conda environments and virtualenv
    # see https://stackoverflow.com/a/46071447
    import sysconfig

    site_packages = sysconfig.get_paths()["purelib"]
    plugin_path = os.path.join(site_packages, "hdf5plugin/plugins")
    if not os.path.isdir(plugin_path):
        raise HDF5PluginError(
            f"Could not find hdf5plugin in site-packages, "
            f"{plugin_path} does not exist. "
            f"Try setting --hdf5_plugin_path manually."
        )
    return plugin_path


def _any_hdf5(filepaths: List[str]):
    return any(fp.endswith((".h5", ".ims")) for fp in filepaths)


def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input",
        type=str,
        help="directory of images to transcode",
    )
    parser.add_argument(
        "--output",
        type=str,
        help="directory to output MIPs"
    )
    parser.add_argument(
        "--deployment",
        type=str,
        default="local",
        help="cluster deployment type",
    )
    parser.add_argument("--log_level", type=int, default=logging.INFO)
    parser.add_argument(
        "--axes",
        type=str,
        nargs="+",
        default=["XY", "XZ", "YZ"],
        help="the projections to create"
    )
    parser.add_argument(
        "--exclude",
        default=[],
        type=str,
        nargs="+",
        help="filename patterns to exclude, e.g., \"*.tif\", \"*.memento\", etc"
    )
    parser.add_argument(
        "--overwrite",
        default=False,
        action="store_true",
        help="Overwrite MIPs if they already exist on disk."
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=128,
        help="dask chunk size (MB)"
    )
    args = parser.parse_args()
    return args


def project_and_write(arr, axis, out_dir, overwrite=False):
    _LOGGER.info(f"computing axis {axis}")
    out_tiff = os.path.join(out_dir, f"MIP_axis_{axis}.tiff")
    if not overwrite and os.path.isfile(out_tiff):
        _LOGGER.info(f"{out_tiff} exists, skipping.")
        return
    norm_axis = _AXES[axis]
    t0 = time.time()
    res = arr.max(axis=norm_axis).compute()
    t1 = time.time()
    _LOGGER.info(f"{t1 - t0}s")
    _LOGGER.info(res.shape)
    tifffile.imwrite(out_tiff, res, imagej=True)


def main():
    args = _parse_args()

    image_paths = collect_filepaths(
        args.input,
        recursive=True,
        include_exts=DataReaderFactory().VALID_EXTENSIONS
    )

    exclude_paths = set()
    for path in image_paths:
        if any(fnmatch.fnmatch(path, pattern) for pattern in args.exclude):
            exclude_paths.add(path)

    image_paths = [p for p in image_paths if p not in exclude_paths]

    _LOGGER.info(f"Found {len(image_paths)} images to process")

    if not image_paths:
        _LOGGER.warning("No images found. Exiting.")
        return

    worker_options = {}
    if _any_hdf5(image_paths):
        os.environ["HDF5_USE_FILE_LOCKING"] = "FALSE"
        worker_options["env"] = {
                "HDF5_PLUGIN_PATH": _find_hdf5plugin_path(),
                "HDF5_USE_FILE_LOCKING": "FALSE"
            }

    client, _ = get_client(args.deployment, worker_options=worker_options)

    for impath in image_paths:

        _LOGGER.info(f"Processing {impath}")

        reader = DataReaderFactory().create(impath)
        chunks = _compute_chunks(reader, args.chunk_size)[2:]  # MB
        _LOGGER.info(f"chunks: {chunks}")

        arr = reader.as_dask_array(chunks=chunks)

        tile_name = Path(impath).stem

        out_dir = os.path.join(args.output, tile_name)
        os.makedirs(out_dir, exist_ok=True)

        # FIXME: compute the projections serially since
        #  dask worker memory blows up if doing
        #  xy, xz, yz = dask.compute(arr.max(axis=0), arr.max(axis=1), arr.max(axis=2))
        for axis in args.axes:
            project_and_write(arr, axis, out_dir, overwrite=args.overwrite)


if __name__ == "__main__":
    main()
