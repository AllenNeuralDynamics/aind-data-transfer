import argparse
import fnmatch
import logging
import os
import shutil
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Union

import google.cloud.exceptions
import pandas as pd
from botocore.session import get_session
from dask_jobqueue import SLURMCluster
from distributed import Client, LocalCluster
from numcodecs import blosc
from transfer.gcs import GCSUploader, create_client
from transfer.s3 import S3Uploader
from transfer.transcode.ome_zarr import write_files_to_zarr
from transfer.util.file_utils import (
    collect_filepaths,
    make_cloud_paths,
    parse_cloud_url,
    is_cloud_url,
)
from transfer.util.io_utils import DataReaderFactory

from cluster.config import load_jobqueue_config

# Importing this alone doesn't work on HPC
# Must manually override HDF5_PLUGIN_PATH environment variable
# in each Dask worker
import hdf5plugin

blosc.use_threads = False

logging.basicConfig(format="%(asctime)s %(message)s", datefmt="%Y-%m-%d %H:%M")
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)


def set_hdf5_env_vars(hdf5_plugin_path=None):
    if hdf5_plugin_path is not None:
        os.environ["HDF5_PLUGIN_PATH"] = hdf5_plugin_path
    os.environ["HDF5_USE_FILE_LOCKING"] = "FALSE"


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
        status = response['ResponseMetadata']['HTTPStatusCode']
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
        help="top-level directory to upload to --output."
             "If a micr/ directory is present, only images within that directory are transcoded, "
             "the rest are transferred as-is."
    )
    parser.add_argument(
        "--output",
        type=str,
        help="where to store the data",
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
        "--hdf5_plugin_path",
        type=str,
        default=None,
        help="path to HDF5 filter plugins. Specifying this is necessary if transcoding HDF5 or IMS files on HPC.",
    )
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
        help='Voxel size of the dataset as a string of floats in XYZ order, e.g. "0.3,0.3,1.0"'
    )
    args = parser.parse_args()
    return args


def get_images(image_folder, exclude=None):
    if exclude is None:
        exclude = []
    image_paths = collect_filepaths(
        image_folder,
        recursive=False,
        include_exts=DataReaderFactory().VALID_EXTENSIONS,
    )

    exclude_paths = set()
    for path in image_paths:
        if any(fnmatch.fnmatch(path, pattern) for pattern in exclude):
            exclude_paths.add(path)

    image_paths = [p for p in image_paths if p not in exclude_paths]

    return image_paths


def copy_files(filepaths, output, root_folder, n_workers=4):
    if not filepaths:
        LOGGER.info("No files to upload. Returning.")
        return
    dst_list = []
    for f in filepaths:
        dst = os.path.join(output, os.path.relpath(f, root_folder))
        dst_list.append(dst)
        Path(dst).parent.mkdir(parents=True, exist_ok=True)
    t0 = time.time()
    with ThreadPoolExecutor(n_workers) as executor:
        executor.map(shutil.copyfile, filepaths, dst_list)
    LOGGER.info(f"Copy took {time.time() - t0}s")


def copy_files_to_cloud(filepaths, provider, bucket, cloud_dst, root_folder):
    if not filepaths:
        LOGGER.info("No files to upload. Returning.")
        return
    if provider == "gs://":
        LOGGER.info("Uploading files to GCS")
        uploader = GCSUploader(bucket)
        uploader.upload_files(filepaths, cloud_dst, root_folder)
    elif provider == "s3://":
        LOGGER.info("Uploading files to s3")
        uploader = S3Uploader(
            target_throughput=1000 * (1024**2) ** 2,
            part_size=128 * (1024**2),
        )
        uploader.upload_files(filepaths, bucket, cloud_dst, root_folder)
    else:
        raise ValueError("Invalid cloud storage provider: {provider}")


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


def find_image_dir(directory):
    RAW_IMAGE_DIR = "micr"
    for root, dirs, files in os.walk(directory):
        if RAW_IMAGE_DIR in dirs:
            return os.path.join(root, RAW_IMAGE_DIR)


def main():
    args = parse_args()

    LOGGER.setLevel(args.log_level)

    if not output_valid(args.output):
        LOGGER.error("Output path not valid, aborting.")
        return

    ensure_metrics_file(args.metrics_file)

    set_hdf5_env_vars(args.hdf5_plugin_path)

    my_dask_kwargs = get_dask_kwargs(args.hdf5_plugin_path)

    client, _ = get_client(args.deployment, **my_dask_kwargs)

    opts = get_blosc_codec(args.codec, args.clevel)

    input_dir = Path(args.input)
    image_dir = Path(find_image_dir(input_dir))

    if not image_dir.is_dir():
        LOGGER.warning(f'"micr/" not found, converting all images in {input_dir}')
        image_dir = input_dir
    images = set(get_images(image_dir, exclude=args.exclude))
    LOGGER.info(f"Found {len(images)} images in {image_dir}")

    filepaths = collect_filepaths(input_dir, recursive=True)
    # Filter out the images we're transcoding, we won't upload the raw data
    filepaths = [p for p in filepaths if p not in images]

    # Upload all the files that we're not converting to Zarr
    if is_cloud_url(args.output):
        provider, bucket, cloud_dst = parse_cloud_url(args.output)
        copy_files_to_cloud(
            filepaths, provider, bucket, cloud_dst, input_dir
        )

        # We will place the Zarr in the cloud folder corresponding to image_dir
        zarr_dst = make_cloud_paths([image_dir], cloud_dst, input_dir)[0]
        zarr_dst = f"{provider}{bucket}/{zarr_dst}"
    else:
        LOGGER.info("Uploading to filesystem")
        copy_files(filepaths, args.output, input_dir)
        # We will place the Zarr in the output folder corresponding to image_dir
        zarr_dst = os.path.join(
            args.output, os.path.relpath(image_dir, input_dir)
        )
        Path(zarr_dst).mkdir(parents=True, exist_ok=True)

    if not images:
        LOGGER.info(f"No images found, exiting.")
        return

    LOGGER.info(f"Writing {len(images)} images to OME-Zarr")
    LOGGER.info(f"Writing OME-Zarr to {zarr_dst}")

    # If voxsize is None, we will
    # attempt to parse it from the image metadata
    voxsize = None
    if args.voxsize is not None:
        voxsize = parse_voxel_size(args.voxsize)

    overwrite = not args.resume

    all_metrics = write_files_to_zarr(
        images,
        zarr_dst,
        args.n_levels,
        args.scale_factor,
        overwrite,
        args.chunk_size,
        args.chunk_shape,
        voxsize,
        opts,
    )

    client.shutdown()

    df = pd.DataFrame.from_records(all_metrics)
    df.to_csv(args.metrics_file, index_label="test_number")


if __name__ == "__main__":
    main()
