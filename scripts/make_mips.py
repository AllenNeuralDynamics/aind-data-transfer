import argparse
import fnmatch
import logging
import os
import time
from pathlib import Path

import h5py

from aind_data_transfer.transcode.ome_zarr import _compute_chunks
from aind_data_transfer.util.io_utils import DataReaderFactory
from aind_data_transfer.util.file_utils import collect_filepaths
from aind_data_transfer.util.dask_utils import get_client

logging.basicConfig(format="%(asctime)s %(message)s", datefmt="%Y-%m-%d %H:%M")
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)


class HDF5PluginError(Exception):
    pass


def find_hdf5plugin_path():
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


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input",
        type=str,
        default="/net/172.20.102.30/aind/exaSPIM/20220805_172536_brain1/",
        # default=r"/allen/programs/aind/workgroups/msma/cameron.arshadi/test_ims",
        help="directory of images to transcode",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="/allen/programs/aind/workgroups/msma/exaSPIM-projections/20220805_172536",
        help="output Zarr path, e.g., s3://bucket/tiles.zarr",
    )
    parser.add_argument(
        "--deployment",
        type=str,
        default="slurm",
        help="cluster deployment type",
    )
    parser.add_argument("--log_level", type=int, default=logging.INFO)
    parser.add_argument(
        "--hdf5-plugin-path",
        type=str,
        default="/allen/programs/aind/workgroups/msma/cameron.arshadi/miniconda3/envs/nd-data-transfer/lib/python3.10/site-packages/hdf5plugin/plugins",
        help="path to HDF5 filter plugins. Specifying this is necessary if transcoding HDF5 or IMS files on HPC."
    )
    parser.add_argument(
        "--do-xy",
        default=False,
        action="store_true",
        help="save the XY MIP in addition to XZ and YZ"
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
    args = parser.parse_args()
    return args


def mip_exists(out_h5):
    if not os.path.exists(out_h5):
        return False
    with h5py.File(out_h5) as f:
        try:
            check = f['data']
            return check.nbytes > 0
        except KeyError:
            return False


def project_and_write(arr, axis, out_dir, overwrite=False):
    LOGGER.info(f"computing axis {axis}")
    out_h5 = os.path.join(out_dir, f"MIP_axis_{axis}.h5")
    if not overwrite and mip_exists(out_h5):
        LOGGER.info(f"{out_h5} exists, skipping.")
        return
    t0 = time.time()
    res = arr.max(axis=axis).compute()
    t1 = time.time()
    LOGGER.info(f"{t1 - t0}s")
    LOGGER.info(res.shape)
    with h5py.File(out_h5, 'w') as f:
        f.create_dataset("data", data=res, chunks=(128, 128))


def main():
    args = parse_args()

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

    LOGGER.info(f"Found {len(image_paths)} images to process")

    if not image_paths:
        LOGGER.warning("No images found. Exiting.")
        return

    worker_options = {
        "env": {
            "HDF5_PLUGIN_PATH": find_hdf5plugin_path(),
            "HDF5_USE_FILE_LOCKING": "FALSE"
        }
    }
    client, _ = get_client(args.deployment, worker_options=worker_options)

    for impath in image_paths:

        LOGGER.info(f"Processing {impath}")

        reader = DataReaderFactory().create(impath)
        chunks = _compute_chunks(reader, 1024)[2:]  # MB
        LOGGER.info(f"chunks: {chunks}")

        arr = reader.as_dask_array(chunks=chunks)

        tile_name = Path(impath).stem

        out_dir = os.path.join(args.output, tile_name)
        os.makedirs(out_dir, exist_ok=True)

        # FIXME: compute the projections serially since
        #  dask worker memory blows up if doing
        #  xy, xz, yz = dask.compute(arr.max(axis=0), arr.max(axis=1), arr.max(axis=2))
        axes = [1, 2]
        if args.do_xy:
            axes.insert(0, 0)
        for a in axes:
            project_and_write(arr, a, out_dir, overwrite=args.overwrite)


if __name__ == "__main__":
    main()
