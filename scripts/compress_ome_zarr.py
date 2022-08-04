import argparse
import logging
import os
import time
from pathlib import Path

import numpy as np
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


def get_client(deployment="slurm"):
    base_config = load_jobqueue_config()
    if deployment == "slurm":
        config = base_config["jobqueue"]["slurm"]
        # cluster config is automatically populated from
        # ~/.config/dask/jobqueue.yaml
        cluster = SLURMCluster()
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
        default=r"Y:\mnt\vast\aind\mesospim_ANM457202_2022_07_11",
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
    args = parser.parse_args()
    return args


def main():
    args = parse_args()

    LOGGER.setLevel(args.log_level)

    validate_output_path(args.output)

    client, _ = get_client(args.deployment)

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

        t0 = time.time()
        writer.write_image(
            image_data=data,  # : types.ArrayLike,  # must be 5D TCZYX
            image_name=tile_name,  #: str,
            physical_pixel_sizes=None,
            channel_names=None,
            channel_colors=None,
            scale_num_levels=args.n_levels,  # : int = 1,
            scale_factor=args.scale_factor,  # : float = 2.0,
            target_chunk_size=args.chunk_size,  # MB
            storage_options=opts,
        )
        write_time = time.time() - t0
        LOGGER.info(
            f"Done. Took {write_time}s. {data.nbytes / write_time / (1024 ** 2)} MiB/s"
        )

        reader.close()


if __name__ == "__main__":
    main()
