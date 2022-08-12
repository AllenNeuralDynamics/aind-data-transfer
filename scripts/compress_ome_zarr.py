import argparse
import logging
import math
import os
import time
from pathlib import Path

import numpy as np
import h5py
# Importing this alone doesn't work on HPC
# Must manually override HDF5_PLUGIN_PATH environment variable
# in each Dask worker
import hdf5plugin
from aicsimageio.writers import OmeZarrWriter
from bids import BIDSLayout
from cluster.config import load_jobqueue_config
from dask_jobqueue import SLURMCluster
from distributed import Client, LocalCluster
from numcodecs import blosc

from transfer.transcode.io import DataReaderFactory

blosc.use_threads = False

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


def parse_bids_dir(indir):
    layout = BIDSLayout(indir)
    print(layout)
    all_files = layout.get()
    print(all_files)


def get_blosc_codec(codec, clevel):
    return blosc.Blosc(cname=codec, clevel=clevel, shuffle=blosc.SHUFFLE)


def get_images(input_dir):
    valid_exts = DataReaderFactory().VALID_EXTENSIONS
    image_paths = []
    for root, _, files in os.walk(input_dir):
        for f in files:
            filepath = os.path.join(root, f)
            if not os.path.isfile(filepath):
                continue
            _, ext = os.path.splitext(filepath)
            if ext in valid_exts:
                image_paths.append(filepath)
    return image_paths


def get_client(deployment="slurm", **kwargs):
    base_config = load_jobqueue_config()
    if deployment == "slurm":
        config = base_config["jobqueue"]["slurm"]
        # cluster config is automatically populated from
        # ~/.config/dask/jobqueue.yaml
        cluster = SLURMCluster(**kwargs)
        cluster.scale(config["n_workers"])
        LOGGER.info(cluster.job_script())
    elif deployment == "local":
        cluster = LocalCluster(processes=True)
        config = None
    else:
        raise NotImplementedError

    client = Client(cluster)
    return client, config


def pad_array_5d(arr):
    while arr.ndim < 5:
        arr = arr[np.newaxis, ...]
    return arr


def guess_chunks(data_shape, target_size, bytes_per_pixel, mode="z"):
    if mode == "z":
        plane_size = data_shape[3] * data_shape[4] * bytes_per_pixel
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
        while np.product(spatial_dims) * bytes_per_pixel > target_size:
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
        chunk_dim = int(math.ceil((target_size / bytes_per_pixel) ** (1.0 / 3)))
        chunks = (
            1,
            1,
            chunk_dim,
            chunk_dim,
            chunk_dim
        )
    else:
        raise ValueError(f"Invalid mode {mode}")

    # convert numpy int64 to Python int or zarr will complain
    return tuple(int(d) for d in chunks)


def expand_chunks(chunks, target_size, bytes_per_pixel, mode="iso"):
    if mode == "cycle":
        # get the spatial dimensions only
        spatial_chunks = np.array(chunks)[2:]
        idx = 0
        ndims = len(spatial_chunks)
        while np.product(spatial_chunks) * bytes_per_pixel < target_size:
            spatial_chunks[idx % ndims] *= 2
            idx += 1
        expanded = (
            1,
            1,
            spatial_chunks[0],
            spatial_chunks[1],
            spatial_chunks[2]
        )
    elif mode == "iso":
        spatial_chunks = np.array(chunks)[2:]
        current = spatial_chunks
        i = 2
        while np.product(current) * bytes_per_pixel < target_size:
            current = spatial_chunks * i
            i += 1
        expanded = (
            1,
            1,
            current[0],
            current[1],
            current[2]
        )
    else:
        raise ValueError(f"Invalid mode {mode}")

    return tuple(int(d) for d in expanded)


def validate_output_path(output):
    # TODO cloud path validation
    if output.startswith("gs://"):
        pass
    elif output.startswith("s3://"):
        pass
    else:
        os.makedirs(output, exist_ok=True)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input",
        type=str,
        default=r"/mnt/vast/aind/cameron.arshadi/test_ims",
        help="directory of images to transcode",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="gs://aind-transfer-service-test/ome-zarr-test/test-file.zarr",
        help="output directory",
    )
    parser.add_argument("--codec", type=str, default="zstd")
    parser.add_argument("--clevel", type=int, default=1)
    parser.add_argument(
        "--chunk_size", type=float, default=128, help="chunk size in MB"
    )
    parser.add_argument(
        "--chunk_shape", type=int, nargs='+', default=None, help="5D sequence of chunk dimensions, in TCZYX order"
    )
    parser.add_argument(
        "--n_levels", type=int, default=1, help="number of resolution levels"
    )
    parser.add_argument(
        "--scale_factor",
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
    parser.add_argument("--log_level", type=int, default=logging.INFO)
    parser.add_argument(
        "--hdf5_plugin_path",
        type=str,
        default="/allen/programs/aind/workgroups/msma/cameron.arshadi/miniconda3/envs/nd-data-transfer/lib/python3.10/site-packages/hdf5plugin/plugins",
        help="path to HDF5 filter plugins. Specifying this is necessary if transcoding HDF5 or IMS files on HPC."
    )
    args = parser.parse_args()
    return args


def main():
    args = parse_args()

    LOGGER.setLevel(args.log_level)

    validate_output_path(args.output)

    set_hdf5_env_vars(args.hdf5_plugin_path)

    my_dask_kwargs = get_dask_kwargs(args.hdf5_plugin_path)

    client, _ = get_client(args.deployment, **my_dask_kwargs)

    compressor = get_blosc_codec(args.codec, args.clevel)
    opts = {
        "compressor": compressor,
    }

    image_paths = get_images(args.input)
    LOGGER.info(f"Found {len(image_paths)} images to process")
    for impath in image_paths:
        LOGGER.info(f"Writing tile {impath}")

        reader = DataReaderFactory().create(impath)
        data = reader.as_dask_array()
        # Force 3D Tile to TCZYX
        data = pad_array_5d(data)

        LOGGER.info(f"{data}")
        LOGGER.info(f"tile size: {data.nbytes / (1024 ** 2)} MB")

        tile_name = Path(impath).stem
        out_zarr = os.path.join(args.output, tile_name + ".zarr")

        writer = OmeZarrWriter(out_zarr)

        if args.chunk_shape is None:
            target_size_bytes = args.chunk_size * 1024 * 1024
            if hasattr(data, "chunksize"):
                # If we're working with a Dask array which is already chunked,
                # use a multiple of the base chunk size to ensure optimal access patterns.
                # Use the "chunksize" property instead of "chunks" since "chunks" is a tuple of tuples
                LOGGER.info(f"Using multiple of base chunksize: {data.chunksize}")
                chunks = expand_chunks(data.chunksize, target_size_bytes, data.itemsize, mode="iso")
            else:
                # Otherwise, hazard a guess
                chunks = guess_chunks(data.shape, target_size_bytes, data.itemsize, mode="iso")
        else:
            chunks = tuple(args.chunk_shape)
        LOGGER.info(f"chunks: {chunks}")

        t0 = time.time()
        writer.write_image(
            image_data=data,  # : types.ArrayLike,  # must be 5D TCZYX
            image_name=tile_name,  #: str,
            physical_pixel_sizes=None,
            channel_names=None,
            channel_colors=None,
            scale_num_levels=args.n_levels,  # : int = 1,
            scale_factor=args.scale_factor,  # : float = 2.0,
            chunks=chunks,
            storage_options=opts,
        )
        write_time = time.time() - t0
        LOGGER.info(
            f"Done. Took {write_time}s. {data.nbytes / write_time / (1024 ** 2)} MiB/s"
        )

        reader.close()


if __name__ == "__main__":
    main()
