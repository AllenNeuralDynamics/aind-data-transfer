import logging
import os

import dask.array as da
import numpy as np
import xarray as xr
import zarr
from aind_data_transfer.util.io_utils import ImarisReader
from dask_jobqueue import SLURMCluster
from distributed import Client, LocalCluster
from numcodecs import blosc

blosc.use_threads = False

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


def get_dask_kwargs(hdf5_plugin_path=None):
    my_dask_kwargs = {"env_extra": []}
    if hdf5_plugin_path is not None:
        # TODO: figure out why this is necessary
        # Override plugin path in each Dask worker
        my_dask_kwargs["env_extra"].append(
            f"export HDF5_PLUGIN_PATH={hdf5_plugin_path}"
        )
    my_dask_kwargs["env_extra"].append("export HDF5_USE_FILE_LOCKING=FALSE")
    return my_dask_kwargs


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


def get_shape(path, level):
    reader = ImarisReader(path)
    shape = reader.get_shape(
        f"/DataSet/ResolutionLevel {level}/TimePoint 0/Channel 0/Data"
    )
    return np.array(shape)


def get_origin_mat(paths):
    origins = []
    for p in paths:
        reader = ImarisReader(p)
        origin = reader.get_origin()
        origins.append(origin)
    return np.array(origins)


def get_scale(path):
    reader = ImarisReader(path)
    scale, _ = reader.get_voxel_size()
    return np.array(scale)


if __name__ == "__main__":
    my_dask_kwargs = {}
    hdf5_plugin_path = find_hdf5plugin_path()
    set_hdf5_env_vars(hdf5_plugin_path)
    my_dask_kwargs.update(get_dask_kwargs(hdf5_plugin_path))
    LOGGER.info(f"dask kwargs: {my_dask_kwargs}")

    client, _ = get_client(deployment="local", **my_dask_kwargs)

    # 0-based index
    min_level = 4
    max_level = 7

    im_dir = (
        "/mnt/vast/aind/exaSPIM/exaSPIM_609281_2022-11-03_13-49-18/exaSPIM"
    )
    paths = [
        os.path.join(im_dir, f)
        for f in os.listdir(im_dir)
        if f.endswith(".ims")
    ]
    paths = [
        p
        for p in paths
        if not p.endswith("test.ims") and not p.endswith("celldemo.ims")
    ]
    paths = list(sorted(paths))

    out_path = (
        f"/allen/scratch/aindtemp/cameron.arshadi/cold-stitch/align-test.zarr"
    )
    store = zarr.storage.DirectoryStore(out_path, dimension_separator="/")
    out_zarr = zarr.open(store=store, mode="w")

    origins = get_origin_mat(paths)
    scale = get_scale(next(iter(paths)))

    print(origins.shape)
    print(scale)

    scales_by_level = {}

    for level in range(min_level, max_level + 1):

        scale_lvl = scale * 2**level
        scales_by_level[level] = scale_lvl
        print(scale_lvl)

        global_min = np.min(origins, axis=0)
        print(global_min)

        # shift so the origin of the tile in the top-left corner is the global (0,0,0)
        new_origins = origins - global_min
        new_origins_vx = np.round(new_origins / scale_lvl).astype(int)

        full_shape = np.max(new_origins_vx, axis=0) + get_shape(
            paths[0], level
        )

        # Create a dummy array with the shape of the aligned dataset.
        # We will initialize the empty zarr store with this array.
        # For details, see https://docs.xarray.dev/en/stable/user-guide/io.html?appending-to-existing-zarr-stores=#appending-to-existing-zarr-stores
        d = da.zeros(shape=full_shape, dtype=np.uint16, chunks=(128, 128, 128))
        arr = xr.DataArray(data=d, dims=["z", "y", "x"], name=f"{level}")
        ds = arr.to_dataset()

        compressor = blosc.Blosc(cname="zstd", clevel=1, shuffle=blosc.SHUFFLE)

        # create the initial store using the dummy array.
        ds.to_zarr(
            store=store,
            compute=False,
            mode="a",
            encoding={f"{level}": {"compressor": compressor}},
        )

        # Now go through each tile at the current level and write it into its corresponding location
        for i, k in enumerate(paths):
            LOGGER.info(f"writing tile: {k}")
            tile_dask = ImarisReader(k).as_dask_array(
                f"/DataSet/ResolutionLevel {level}/TimePoint 0/Channel 0/Data",
                chunks=(128, 128, 128),
            )
            # tile_dask = ensure_array_5d(tile_dask)

            o = new_origins_vx[i]
            s = tile_dask.shape

            # Construct a Dataset from the tile, the name argument corresponds to the key of the zarr array
            # in the top-level group, e.g., "output.zarr/name".
            ds = xr.DataArray(
                data=tile_dask, dims=["z", "y", "x"], name=f"{level}"
            ).to_dataset()

            attempt = 0
            max_attempts = 20
            # If writing directly to s3, this will randomly fail often.
            # Most likely a bug somewhere in the xarray -> dask -> zarr-python -> s3fs stack
            # This is not necessary if writing to a filesystem.
            while attempt < max_attempts:
                try:
                    # It will write to the component given by the Dataset name
                    ds.to_zarr(
                        store=store,
                        region={
                            "z": slice(o[0], o[0] + s[0]),
                            "y": slice(o[1], o[1] + s[1]),
                            "x": slice(o[2], o[2] + s[2]),
                        },
                    )
                    break
                except Exception as e:
                    print(e)
                    print(
                        f"failed to write tile: attempt {attempt}/{max_attempts}"
                    )
                    attempt += 1

    # Now write the OME-NGFF metadata at the end, so it is not overwritten.
    out_zarr = zarr.open(store=store, mode="r+")
    out_zarr.attrs["omero"] = {
        "channels": [
            {
                "active": True,
                "coefficient": 1,
                "color": "000000",
                "family": "linear",
                "inverted": False,
                "label": "Channel:aligned.zarr:0",
                "window": {
                    "end": 1000.0,
                    "max": 1000.0,
                    "min": 0.0,
                    "start": 0.0,
                },
            }
        ],
        "id": 1,
        "name": "aligned.zarr",
        "rdefs": {"defaultT": 0, "defaultZ": 12288, "model": "color"},
        "version": "0.4",
    }
    out_zarr.attrs["multiscales"] = [
        {
            "name": "aligned.zarr",
            "axes": [
                {"name": "z", "type": "space", "unit": "micrometer"},
                {"name": "y", "type": "space", "unit": "micrometer"},
                {"name": "x", "type": "space", "unit": "micrometer"},
            ],
            "datasets": [
                {
                    "coordinateTransformations": [
                        {
                            "scale": scales_by_level[level].tolist(),
                            "type": "scale",
                        }
                    ],
                    "path": f"{level}",
                }
                for level in range(min_level, max_level + 1)
            ],
        }
    ]
