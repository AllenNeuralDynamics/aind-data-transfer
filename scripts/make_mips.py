import argparse
import fnmatch
import logging
import os
import time
from pathlib import Path

import h5py
from dask_jobqueue import SLURMCluster
from distributed import LocalCluster, Client
from transfer.transcode.io import DataReaderFactory
from transfer.util.fileutils import collect_filepaths

from cluster.config import load_jobqueue_config
from transfer.util.arrayutils import ensure_shape_5d, guess_chunks, expand_chunks

logging.basicConfig(format="%(asctime)s %(message)s", datefmt="%Y-%m-%d %H:%M")
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)


def set_hdf5_env_vars(hdf5_plugin_path=None):
    if hdf5_plugin_path is not None:
        os.environ["HDF5_PLUGIN_PATH"] = hdf5_plugin_path
    os.environ["HDF5_USE_FILE_LOCKING"] = "FALSE"


def get_dask_kwargs(hdf5_plugin_path=None):
    my_dask_kwargs = {'env_extra': []}
    if hdf5_plugin_path is not None:
        # TODO: figure out why this is necessary
        # Override plugin path in each Dask worker
        my_dask_kwargs['env_extra'].append(f"export HDF5_PLUGIN_PATH={hdf5_plugin_path}")
    my_dask_kwargs['env_extra'].append("export HDF5_USE_FILE_LOCKING=FALSE")
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
    args = parser.parse_args()
    return args


def compute_chunks(reader, target_size_mb):
    target_size_bytes = target_size_mb * 1024 * 1024
    padded_chunks = ensure_shape_5d(reader.get_chunks())
    padded_shape = ensure_shape_5d(reader.get_shape())
    LOGGER.info(f"Using multiple of base chunk size: {padded_chunks}")
    if padded_chunks[-2:] == padded_shape[-2:]:
        LOGGER.info("chunks and shape have same XY dimensions, "
                    "will chunk along Z only.")
        chunks = guess_chunks(
            padded_shape,
            target_size_bytes,
            reader.get_itemsize(),
            mode="z"
        )
    else:
        chunks = expand_chunks(
            padded_chunks,
            padded_shape,
            target_size_bytes,
            reader.get_itemsize(),
            mode="iso"
        )
    return chunks


def project_and_write(arr, axis, out_dir):
    LOGGER.info(f"computing axis {axis}")
    t0 = time.time()
    res = arr.max(axis=axis).compute()
    t1 = time.time()
    LOGGER.info(f"{t1 - t0}s")
    LOGGER.info(res.shape)
    with h5py.File(os.path.join(out_dir, f"MIP_axis_{axis}.h5"), 'w') as f:
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

    set_hdf5_env_vars(args.hdf5_plugin_path)

    my_dask_kwargs = get_dask_kwargs(args.hdf5_plugin_path)

    client, _ = get_client(args.deployment, **my_dask_kwargs)

    for impath in image_paths:

        LOGGER.info(f"Processing {impath}")

        reader = DataReaderFactory().create(impath)
        chunks = compute_chunks(reader, 1024)[2:]  # MB
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
            project_and_write(arr, a, out_dir)


if __name__ == "__main__":
    main()
