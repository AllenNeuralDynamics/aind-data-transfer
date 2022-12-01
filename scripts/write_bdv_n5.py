import argparse
import json
import logging
import os

import dask.array
import numpy as np
import zarr
from dask_jobqueue import SLURMCluster
from distributed import Client, LocalCluster, wait
from numcodecs import GZip

from aind_data_transfer.transcode.ome_zarr import _get_or_create_pyramid
from aind_data_transfer.util.file_utils import *
from aind_data_transfer.util.io_utils import DataReaderFactory, ImarisReader

from cluster.config import load_jobqueue_config

logging.basicConfig(format="%(asctime)s %(message)s", datefmt="%Y-%m-%d %H:%M")
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)


def set_hdf5_env_vars(hdf5_plugin_path=None):
    if hdf5_plugin_path is not None:
        os.environ["HDF5_PLUGIN_PATH"] = hdf5_plugin_path
    os.environ["HDF5_USE_FILE_LOCKING"] = "FALSE"


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


def check_any_hdf5(filepaths):
    return any(f.endswith((".ims", ".h5")) for f in filepaths)


def get_dask_kwargs(hdf5_plugin_path=None):
    my_dask_kwargs = {"env_extra": []}
    if hdf5_plugin_path is not None:
        # Override plugin path in each Dask worker
        my_dask_kwargs["env_extra"].append(
            f"export HDF5_PLUGIN_PATH={hdf5_plugin_path}"
        )
    my_dask_kwargs["env_extra"].append("export HDF5_USE_FILE_LOCKING=FALSE")
    return my_dask_kwargs


def get_downscale_factors(n_levels, scale_factors=(2, 2, 2)):
    return [
        [scale_factors[0] ** i, scale_factors[1] ** i, scale_factors[2] ** i]
        for i in range(n_levels)
    ]


def get_client(deployment="slurm", **kwargs):
    if deployment == "slurm":
        base_config = load_jobqueue_config()
        config = base_config["jobqueue"]["slurm"]
        # cluster config is automatically populated from
        # ~/.config/dask/jobqueue.yaml
        cluster = SLURMCluster(**kwargs)
        cluster.scale(config["n_workers"])
        LOGGER.info(cluster.job_script())
    elif deployment == "local":
        import platform

        use_procs = False if platform.system() == "Windows" else True
        cluster = LocalCluster(processes=use_procs, threads_per_worker=1)
        config = None
    else:
        raise NotImplementedError

    client = Client(cluster)
    return client, config


def get_datatype_str(dtype):
    if dtype == np.uint8:
        return "uint8"
    elif dtype == np.uint16:
        return "uint16"
    else:
        raise NotImplementedError(f"dtype {dtype} not implemented")


def write_attributes(n5_path, attrs, fs):
    # HACK: setting group attributes directly does not work
    #  when writing to google cloud storage with N5FSStore
    #  e.g., group.attrs['field'] = value
    with fs.open(os.path.join(n5_path, "attributes.json"), mode="w") as f:
        json.dump(attrs, f)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input",
        type=str,
        default="/mnt/vast/aind/exaSPIM/exaSPIM_125L_20220805_172536/micr",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="gs://aind-msma-data/exaSPIM_125L_20220805_172536_n5",
    )
    parser.add_argument(
        "--n_levels", type=int, default=8, help="number of resolution levels"
    )
    parser.add_argument(
        "--scale-factor",
        type=float,
        default=2.0,
        help="scale factor for downsampling",
    )
    parser.add_argument(
        "--deployment",
        type=str,
        default="local",
        help="cluster deployment type",
    )
    parser.add_argument(
        "--block-size",
        type=int,
        nargs="+",
        default=[512, 512, 256],
        help="N5 block size (analogous to zarr chunks) in XYZ order",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    imdir = args.input
    images = get_images(imdir)
    LOGGER.info(f"Found {len(images)} images")

    if not images:
        LOGGER.warning("No images found, exiting.")
        return

    out_dir = args.output
    out_n5 = os.path.join(out_dir, "dataset.n5")
    # metadata used to reconstruct the xml
    out_meta = os.path.join(out_dir, "meta.json")

    store = zarr.N5FSStore(out_n5)
    fs = store.fs
    root = zarr.group(store, overwrite=True)

    n_levels = args.n_levels
    downscale_factors = (args.scale_factor,) * 3

    factors_by_lvl = get_downscale_factors(
        n_levels, scale_factors=downscale_factors
    )

    my_dask_kwargs = {}
    if check_any_hdf5(images):
        hdf5_plugin_path = find_hdf5plugin_path()
        set_hdf5_env_vars(hdf5_plugin_path)
        my_dask_kwargs.update(get_dask_kwargs(hdf5_plugin_path))
    LOGGER.info(f"dask kwargs: {my_dask_kwargs}")

    client, _ = get_client(args.deployment, **my_dask_kwargs)

    meta = {}

    for i, impath in enumerate(images):
        LOGGER.info(f"Writing tile {impath}")

        reader = DataReaderFactory().create(impath)
        if not isinstance(reader, ImarisReader):
            raise NotImplementedError("Only IMS is currently supported")

        # Axis order must be XYZ
        shape = list(reversed(reader.get_shape()))

        origin = list(reversed(reader.get_origin()))

        voxel_size, unit = reader.get_voxel_size()
        voxel_size = list(reversed(voxel_size))
        unit = unit.decode("utf-8")

        pyramid = _get_or_create_pyramid(
            reader, n_levels, chunks=tuple(reversed(args.block_size))
        )

        setup_name = f"setup{i}"
        setup = root.create_group(setup_name)
        setup_attrs = {
            "downsamplingFactors": factors_by_lvl,
            "dataType": get_datatype_str(pyramid[0].dtype)
        }
        write_attributes(
            os.path.join(out_n5, setup_name),
            setup_attrs,
            fs
        )

        # TODO: support multiple timepoints?
        timepoint_name = "timepoint0"
        timepoint = setup.create_group(timepoint_name)
        timepoint_attrs = {
            "resolution": voxel_size,
            "saved_completely": True,
            "multiScale": True
        }
        write_attributes(
            os.path.join(out_n5, setup_name, timepoint_name),
            timepoint_attrs,
            fs
        )

        im_meta = {}
        im_meta["tileName"] = Path(impath).name
        im_meta["voxelSize"] = {
            "size": voxel_size,
            "unit": unit,
        }
        im_meta["id"] = i
        im_meta["name"] = i
        im_meta["size"] = shape
        im_meta["origin"] = origin
        im_meta["attributes"] = {
            "illumination": 0,
            "channel": 0,
            "tile": i,
            "angle": 0,
        }
        LOGGER.info(im_meta)

        meta[Path(impath).name] = im_meta

        futures = []
        for i, arr in enumerate(pyramid):
            path = f"s{i}"
            fut = dask.array.to_zarr(
                array_key=path,
                arr=arr,
                url=timepoint.store,
                component=str(Path(timepoint.path, str(path))),
                overwrite=True,
                compute=False,
                compressor=GZip(1),
            )
            futures.append(fut)
            # Setting array attributes directly works??
            timepoint[path].attrs["downsamplingFactors"] = factors_by_lvl[i]

        ret = dask.persist(*futures)
        wait(ret)

    with fs.open(out_meta, "w") as f:
        json.dump(meta, f, indent=4)


if __name__ == "__main__":
    main()
