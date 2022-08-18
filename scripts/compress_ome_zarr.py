from typing import Union

import argparse
import logging
import numpy as np
import os
import time
# Importing this alone doesn't work on HPC
# Must manually override HDF5_PLUGIN_PATH environment variable
# in each Dask worker
from aicsimageio.writers import OmeZarrWriter
from dask_jobqueue import SLURMCluster
from distributed import Client, LocalCluster
from numcodecs import blosc
from pathlib import Path
from transfer.transcode.io import DataReaderFactory, HDF5Reader, MissingDatasetError
from transfer.util.arrayutils import (ensure_array_5d, ensure_shape_5d,
                                      guess_chunks, expand_chunks)
from transfer.util.fileutils import collect_filepaths

from cluster.config import load_jobqueue_config

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


def get_blosc_codec(codec, clevel):
    return blosc.Blosc(cname=codec, clevel=clevel, shuffle=blosc.SHUFFLE)


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
        cluster = LocalCluster(processes=use_procs)
        config = None
    else:
        raise NotImplementedError

    client = Client(cluster)
    return client, config


def validate_output_path(output):
    # TODO cloud path validation
    if output.startswith("gs://"):
        pass
    elif output.startswith("s3://"):
        pass
    else:
        os.makedirs(output, exist_ok=True)


def compute_pyramid(data, n_lvls):
    from xarray_multiscale import multiscale
    from xarray_multiscale.reducers import windowed_mean

    pyramid = multiscale(
        data,
        windowed_mean,  # func
        (2,) * data.ndim,  # scale factors
        depth=n_lvls - 1,
        preserve_dtype=True
    )
    return [arr.data for arr in pyramid]


def get_or_create_pyramid(reader, n_levels, chunks):
    if isinstance(reader, HDF5Reader):
        try:
            pyramid = reader.get_dask_pyramid(
                n_levels,
                timepoint=0,
                channel=0,
                # Use only the dimensions that exist in the base image.
                chunks=chunks
            )
        except MissingDatasetError as e:
            LOGGER.error(e)
            LOGGER.warning(f"{reader.get_filepath()} does not contain all requested scales."
                           f"Computing them instead...")
            pyramid = compute_pyramid(
                reader.as_dask_array(chunks=chunks),
                n_levels
            )
    else:
        pyramid = compute_pyramid(
            reader.as_dask_array(chunks=chunks),
            n_levels
        )

    return pyramid


def _compute_chunks(reader, target_size_mb):
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
            mode="cycle"
        )
    return chunks


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input",
        type=str,
        default=r"/allen/programs/aind/workgroups/msma/cameron.arshadi/test_ims",
        help="directory of images to transcode",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="gs://aind-transfer-service-test/ome-zarr-test/exaSPIM-tile-test",
        help="output directory",
    )
    parser.add_argument("--codec", type=str, default="zstd")
    parser.add_argument("--clevel", type=int, default=1)
    parser.add_argument(
        "--chunk_size", type=float, default=64, help="chunk size in MB"
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

    image_paths = collect_filepaths(
        args.input,
        recursive=True,
        include_exts=DataReaderFactory().VALID_EXTENSIONS
    )
    LOGGER.info(f"Found {len(image_paths)} images to process")
    for impath in image_paths:
        LOGGER.info(f"Writing tile {impath}")

        # Create reader, but don't construct dask array
        # until we know the optimal chunk shape.
        reader = DataReaderFactory().create(impath)

        if args.chunk_shape is None:
            assert args.chunk_size > 0
            chunks = _compute_chunks(reader, args.chunk_size)
        else:
            chunks = tuple(args.chunk_shape)
            assert np.all(chunks)

        LOGGER.info(f"chunks: {chunks}, {np.product(chunks) * reader.get_itemsize() / (1024 ** 2)} MiB")

        # We determine the chunk size before creating the dask array since
        # rechunking an existing dask array, e.g, data = data.rechunk(chunks),
        # causes memory use to grow (unbounded?) during the zarr write step.
        # See https://github.com/dask/dask/issues/5105.

        tile_name = Path(impath).stem
        out_zarr = os.path.join(args.output, tile_name + ".zarr")
        writer = OmeZarrWriter(out_zarr)

        reader_chunks = chunks[len(chunks) - len(reader.get_shape()):]

        if args.n_levels > 1:
            pyramid = get_or_create_pyramid(reader, args.n_levels, reader_chunks)

            for i in range(len(pyramid)):
                pyramid[i] = ensure_array_5d(pyramid[i])

            LOGGER.info(f"{pyramid[0]}")
            LOGGER.info(f"tile size: {pyramid[0].nbytes / (1024 ** 2)} MB")

            t0 = time.time()
            LOGGER.info("Starting write...")
            writer.write_multiscale(
                pyramid=pyramid,  # : types.ArrayLike,  # must be 5D TCZYX
                image_name=tile_name,  #: str,
                physical_pixel_sizes=None,
                channel_names=None,
                channel_colors=None,
                scale_factor=(args.scale_factor,) * 3,  # : float = 2.0,
                chunks=chunks,
                storage_options=opts,
            )
            write_time = time.time() - t0
            LOGGER.info(
                f"Done. Took {write_time}s. {_get_bytes_written(pyramid) / write_time / (1024 ** 2)} MiB/s"
            )

        else:
            data = reader.as_dask_array(chunks=reader_chunks)
            # Force 3D Tile to TCZYX
            data = ensure_array_5d(data)

            LOGGER.info(f"{data}")
            LOGGER.info(f"tile size: {data.nbytes / (1024 ** 2)} MB")

            t0 = time.time()
            LOGGER.info("Starting write...")
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
                f"Done. Took {write_time}s. {_get_bytes_written(data) / write_time / (1024 ** 2)} MiB/s"
            )

        reader.close()

    client.shutdown()


def _get_bytes_written(data: Union[list, np.ndarray]):
    if isinstance(data, list):
        total_bytes = 0
        for arr in data:
            total_bytes += arr.nbytes
        return total_bytes

    return data.nbytes


if __name__ == "__main__":
    main()
