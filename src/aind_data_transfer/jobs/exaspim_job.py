import logging
import os.path
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from shutil import copytree, ignore_patterns

from aind_data_transfer.configuration_loader import ImagingJobConfigurationLoader
from aind_data_transfer.readers import ImagingReaders
from aind_data_transfer.util.file_utils import is_cloud_url, parse_cloud_url

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)


def _find_scripts_dir():
    scripts_dir = Path(os.path.abspath(__file__)).parents[3] / "scripts"
    if not scripts_dir.is_dir():
        raise Exception(f"scripts directory not found: {scripts_dir}")
    return scripts_dir


_SCRIPTS_DIR = _find_scripts_dir()

_S3_SCRIPT = _SCRIPTS_DIR / "s3_upload.py"
if not _S3_SCRIPT.is_file():
    raise Exception(f"script not found: {_S3_SCRIPT}")

_GCS_SCRIPT = _SCRIPTS_DIR / "gcs_upload.py"
if not _GCS_SCRIPT.is_file():
    raise Exception(f"script not found: {_GCS_SCRIPT}")

_OME_ZARR_SCRIPT = _SCRIPTS_DIR / "write_ome_zarr.py"
if not _OME_ZARR_SCRIPT.is_file():
    raise Exception(f"script not found: {_OME_ZARR_SCRIPT}")


def _build_s3_cmd(data_src_dir, bucket, prefix, raw_image_dir_name, n_threads=4):
    cmd = f"python {_S3_SCRIPT} " \
          f"--input={data_src_dir} " \
          f"--bucket={bucket} " \
          f"--s3_path={prefix} " \
          f"--nthreads={n_threads} " \
          f"--recursive " \
          f"--exclude_dirs={raw_image_dir_name}"
    return cmd


def _build_gcs_cmd(data_src_dir, bucket, prefix, raw_image_dir_name, n_threads=4):
    cmd = f"python {_GCS_SCRIPT} " \
          f"--input={data_src_dir} " \
          f"--bucket={bucket} " \
          f"--gcs_path={prefix} " \
          f"--nthreads={n_threads} " \
          f"--recursive " \
          f"--method=python " \
          f"--exclude_dirs={raw_image_dir_name} "
    return cmd


def _build_ome_zar_cmd():
    # TODO
    pass


def main():
    job_start_time = datetime.now(timezone.utc)
    job_configs = ImagingJobConfigurationLoader().load_configs(sys.argv[1:])

    # Extract raw data name, (e.g., openephys) and raw data path
    data_name = job_configs["data"]["name"]
    data_src_dir = Path(job_configs["endpoints"]["raw_data_dir"])
    dest_data_dir = job_configs["endpoints"]["dest_data_dir"]

    raw_image_dir = ImagingReaders.get_raw_data_dir(
        ImagingReaders.get_reader_name(data_src_dir),
        data_src_dir
    )

    LOGGER.info(f"Transferring data to {dest_data_dir}")

    raw_image_dir_name = Path(raw_image_dir).name

    if job_configs["jobs"]["upload_aux_files"]:
        LOGGER.info("Uploading auxiliary data")
        if is_cloud_url(dest_data_dir):
            provider, bucket, prefix = parse_cloud_url(dest_data_dir)
            if provider == "s3://":
                cmd = _build_s3_cmd(data_src_dir, bucket, prefix, raw_image_dir_name)
            elif provider == "gs://":
                cmd = _build_gcs_cmd(data_src_dir, bucket, prefix, raw_image_dir_name)
            else:
                raise Exception(f"Unsupported cloud storage: {provider}")
            ret = subprocess.run(cmd, shell=True)
        else:
            copytree(data_src_dir, dest_data_dir, ignore=ignore_patterns(raw_image_dir_name))


if __name__ == "__main__":
    main()
