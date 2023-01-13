import argparse
import logging

import google.cloud.exceptions
import pandas as pd
from botocore.session import get_session
from numcodecs import blosc

from aind_data_transfer.gcs import create_client
from aind_data_transfer.transcode.ome_zarr import write_files
from aind_data_transfer.util.file_utils import *
from aind_data_transfer.util.dask_utils import get_client
from aind_data_transfer.util.env_utils import find_hdf5plugin_path

# Importing this alone doesn't work on HPC
# Must manually override HDF5_PLUGIN_PATH environment variable
# in each Dask worker
import hdf5plugin



blosc.use_threads = False

logging.basicConfig(format="%(asctime)s %(message)s", datefmt="%Y-%m-%d %H:%M")
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)


def get_blosc_codec(cname, clevel):
    opts = {
        "compressor": blosc.Blosc(
            cname=cname, clevel=clevel, shuffle=blosc.SHUFFLE
        ),
        "params": {
            "shuffle": blosc.SHUFFLE,
            "level": clevel,
            "name": f"blosc-{cname}",
        },
    }
    return opts


def output_valid(output: Union[str, os.PathLike]) -> bool:
    """
    Check if the output path is valid. If the path is a cloud storage location,
    check if the bucket exists and is accessible (but not necessarily writable) by the current user.

    Args:
        output: the output path or url

    Returns:
        True if the bucket exists and is accessible
    """
    if output.startswith("gs://"):
        _, bucket_name, __ = parse_cloud_url(output)
        client = create_client()
        try:
            _ = client.get_bucket(bucket_name)
            return True
        except google.cloud.exceptions.NotFound:
            LOGGER.error(f"Bucket not found: {bucket_name}")
            return False
        except Exception as e:
            LOGGER.error(f"Error retrieving bucket {bucket_name}: {e}")
            return False
    elif output.startswith("s3://"):
        _, bucket_name, __ = parse_cloud_url(output)
        session = get_session()
        client = session.create_client("s3")
        try:
            response = client.head_bucket(Bucket=bucket_name)
        except Exception as e:
            LOGGER.error(f"Error connecting to bucket {bucket_name}: {e}")
            return False
        status = response["ResponseMetadata"]["HTTPStatusCode"]
        if status == 200:
            return True
        else:
            LOGGER.error(
                f"Error accessing bucket: {bucket_name}"
                f"\nHTTP status code: {status}"
            )
            return False
    else:
        os.makedirs(output, exist_ok=True)
        return True


def ensure_metrics_file(metrics_file):
    if not metrics_file.endswith(".csv"):
        raise ValueError("metrics_file must be .csv")
    metrics_dir = os.path.dirname(os.path.abspath(metrics_file))
    if not os.path.isdir(metrics_dir):
        os.makedirs(metrics_dir, exist_ok=True)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input",
        type=str,
        help="directory of images to transcode to OME-Zarr. Each image"
        " is written to a separate group in the top-level zarr folder.",
    )
    parser.add_argument(
        "--output",
        type=str,
        help="output location of the OME-Zarr dataset, either a path or url (e.g., s3://my-bucket/my-zarr)",
    )
    parser.add_argument("--codec", type=str, default="zstd")
    parser.add_argument("--clevel", type=int, default=1)
    parser.add_argument(
        "--chunk_size", type=float, default=64, help="chunk size in MB"
    )
    parser.add_argument(
        "--chunk_shape",
        type=int,
        nargs="+",
        default=None,
        help="5D sequence of chunk dimensions, in TCZYX order",
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
        "--metrics_file",
        type=str,
        default="tile-metrics.csv",
        help="output tile metrics csv file",
    )
    parser.add_argument(
        "--resume",
        default=False,
        action="store_true",
        help="resume processing",
    )
    parser.add_argument(
        "--exclude",
        default=[],
        type=str,
        nargs="+",
        help='filename patterns to exclude from transcoding, e.g., "*.tif", "*.memento", etc',
    )
    parser.add_argument(
        "--voxsize",
        type=str,
        default=None,
        help='Voxel size of the dataset as a string of floats in XYZ order, e.g. "0.3,0.3,1.0"',
    )
    args = parser.parse_args()
    return args


def parse_voxel_size(voxsize_str):
    vsstr = voxsize_str.split(",")
    vs = []
    if len(vsstr) > 0:
        vs.append(float(vsstr[2]))
    else:
        vs.append(1.0)
    if len(vsstr) > 1:
        vs.append(float(vsstr[1]))
    else:
        vs.append(1.0)
    if len(vsstr) > 2:
        vs.append(float(vsstr[0]))
    else:
        vs.append(1.0)
    return vs


def main():
    args = parse_args()

    LOGGER.setLevel(args.log_level)

    if not output_valid(args.output):
        LOGGER.error("Output path not valid, aborting.")
        return

    ensure_metrics_file(args.metrics_file)

    image_dir = Path(args.input)

    images = set(get_images(image_dir, exclude=args.exclude))
    LOGGER.info(f"Found {len(images)} images in {image_dir}")

    if not images:
        LOGGER.info(f"No images found, exiting.")
        return

    worker_options = {
        "env": {
            "HDF5_PLUGIN_PATH": find_hdf5plugin_path(),
            "HDF5_USE_FILE_LOCKING": "FALSE"
        }
    }
    client, _ = get_client(args.deployment, worker_options=worker_options)

    LOGGER.info(f"Writing {len(images)} images to OME-Zarr")
    LOGGER.info(f"Writing OME-Zarr to {args.output}")

    # If voxsize is None, we will
    # attempt to parse it from the image metadata
    voxsize = None
    if args.voxsize is not None:
        voxsize = parse_voxel_size(args.voxsize)

    overwrite = not args.resume

    opts = get_blosc_codec(args.codec, args.clevel)

    all_metrics = write_files(
        images,
        args.output,
        args.n_levels,
        args.scale_factor,
        overwrite,
        args.chunk_size,
        args.chunk_shape,
        voxsize,
        opts,
    )

    df = pd.DataFrame.from_records(all_metrics)
    df.to_csv(args.metrics_file, index_label="test_number")


if __name__ == "__main__":
    main()
