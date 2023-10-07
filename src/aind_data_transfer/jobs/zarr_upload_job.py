import logging
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from time import sleep
from typing import Any, Optional, Tuple, List

from aind_data_transfer.util import setup_logging
from aind_data_transfer.config_loader.base_config import (
    ConfigError,
    BasicUploadJobConfigs,
)
from aind_data_transfer.jobs.basic_job import BasicJob
from aind_data_transfer.transformations.converters import log_to_acq_json, acq_json_to_xml
from aind_data_transfer.transformations.ome_zarr import write_files
from aind_data_transfer.util.dask_utils import get_client, get_deployment
from aind_data_transfer.util.env_utils import find_hdf5plugin_path
from aind_data_transfer.util.file_utils import get_images
from aind_data_transfer.util.s3_utils import upload_to_s3
from aind_data_transfer.transformations.file_io import read_toml, read_imaging_log, write_acq_json, write_xml
from aind_data_transfer.transformations.ng_link_creation import write_json_from_zarr

import yaml
from numcodecs import blosc
from pydantic import Field, BaseSettings
from aind_data_schema.data_description import Modality, Platform


_CLIENT_CLOSE_TIMEOUT = 300  # seconds
_CLIENT_SHUTDOWN_SLEEP_TIME = 30  # seconds


class ZarrConversionConfigs(BaseSettings):
    n_levels: Optional[int] = Field(
        1, description="Number of levels to use for the pyramid. Default is 1."
    )
    scale_factor: Optional[int] = Field(
        2, description="Scale factor to use for the pyramid. Default is 2."
    )
    chunk_shape: Optional[Tuple[int, int, int, int, int]] = Field(
        (1, 1, 256, 256, 256),
        description="5D Chunk shape to use for the zarr Array. Default is (1, 1, 256, 256, 256).",
    )
    voxel_size: Optional[Tuple[float, float, float]] = Field(
        None,
        description="Voxel size to use for the zarr Array. if None, will attempt to parse from the image metadata. Default is None.",
    )
    codec: Optional[str] = Field(
        "zstd",
        description="Blosc codec to use for compression. Default is zstd.",
    )
    clevel: Optional[int] = Field(
        1, description="Blosc compression level to use. Default is 1."
    )
    do_bkg_subtraction: Optional[bool] = Field(
        False,
        description="Whether to subtract the background image from the raw data. Default is False.",
    )
    exclude_patterns: Optional[List[str]] = Field(
        None,
        description="List of patterns to exclude from the zarr conversion. Default is None.",
    )
    create_ng_link: Optional[bool] = Field(
        False,
        description="Whether to create a neuroglancer link. Default is False.",
    )
    ng_vmin: Optional[float] = Field(
        0,
        description="Default minimum of the neuroglancer display range. Default is 0."
    )
    ng_vmax: Optional[float] = Field(
        200.0,
        description="Default maximum of the neuroglancer display range. Default is 200."
    )

    @classmethod
    def from_yaml(cls, yaml_path: Path):
        with open(yaml_path, 'r') as f:
            yaml_dict = yaml.safe_load(f)
        return cls(**yaml_dict)


class ZarrUploadJob(BasicJob):

    def __init__(self, job_configs: BasicUploadJobConfigs):
        super().__init__(job_configs=job_configs)

        if len(self.job_configs.modalities) != 1:
            raise ConfigError("ZarrUploadJob only supports one modality at a time.")

        self._instance_logger.info(f"Using job configs: {self.job_configs}")

        self._modality_config = self.job_configs.modalities[0]
        if self._modality_config.modality != Modality.SPIM:
            raise ConfigError(
                "ZarrUploadJob only supports SPIM modality."
            )

        self._data_src_dir = self._modality_config.source
        if not self._data_src_dir.exists():
            raise FileNotFoundError(
                f"data source directory {self._data_src_dir} does not exist"
            )
        self._instance_logger.info(f"Using data source directory: {self._data_src_dir}")

        if self.job_configs.platform == Platform.HCR:
            self._raw_image_dir = self._data_src_dir / "diSPIM"
        elif self.job_configs.platform == Platform.EXASPIM:
            self._raw_image_dir = self._data_src_dir / "exaSPIM"
        else:
            raise ConfigError(
                "ZarrUploadJob only supports HCR and EXASPIM platforms."
            )
        if not self._raw_image_dir.exists():
            self._instance_logger.warning(
                f"raw image directory {self._raw_image_dir} does not exist. Trying micr/"
            )
            self._raw_image_dir = self._data_src_dir / "micr"
            if not self._raw_image_dir.exists():
                raise FileNotFoundError(
                    f"raw image directory {self._raw_image_dir} does not exist"
                )
        self._instance_logger.info(f"Using raw image directory: {self._raw_image_dir}")

        self._derivatives_dir = self._data_src_dir / "derivatives"
        if not self._derivatives_dir.exists():
            raise FileNotFoundError(
                f"derivatives directory {self._derivatives_dir} does not exist"
            )
        self._instance_logger.info(f"Using derivatives directory: {self._derivatives_dir}")

        self._zarr_path = f"s3://{self.job_configs.s3_bucket}/{self.job_configs.s3_prefix}/{self._modality_config.modality.value.abbreviation}.ome.zarr"
        self._instance_logger.info(f"Output zarr path: {self._zarr_path}")

        self._resolve_zarr_configs()
        self._instance_logger.info(f"Using zarr configs: {self._zarr_configs}")

    def _resolve_zarr_configs(self) -> None:
        config_path = self._modality_config.extra_configs
        if config_path is not None:
            if not config_path.exists():
                raise FileNotFoundError(f"Extra config file {config_path} does not exist.")
            self._zarr_configs = ZarrConversionConfigs.from_yaml(config_path)
        else:
            # use defaults
            self._zarr_configs = ZarrConversionConfigs()

    def _compress_raw_data(self, temp_dir: Path) -> None:
        """Not implemented for ZarrUploadJob."""
        pass

    def _encrypt_behavior_dir(self, temp_dir: Path) -> None:
        """Not implemented for ZarrUploadJob."""
        pass

    # Override to exclude raw data directory
    def _upload_to_s3(self, dir: Path, excluded: Any = None) -> None:
        """Upload the data to s3."""
        upload_to_s3(
            directory_to_upload=dir,
            s3_bucket=self.job_configs.s3_bucket,
            s3_prefix=self.job_configs.s3_prefix,
            dryrun=self.job_configs.dry_run,
            excluded=excluded,
        )

    def _upload_zarr(self) -> None:
        images = set(
            get_images(
                self._raw_image_dir, exclude=self._zarr_configs.exclude_patterns
            )
        )
        self._instance_logger.info(
            f"Found {len(images)} images in {self._raw_image_dir}"
        )

        if not images:
            self._instance_logger.warning(f"No images found, exiting.")
            return

        self._instance_logger.info(f"Writing {len(images)} images to OME-Zarr")
        self._instance_logger.info(f"Writing OME-Zarr to {self._zarr_path}")

        bkg_img_dir = None
        if self._zarr_configs.do_bkg_subtraction:
            bkg_img_dir = str(self._derivatives_dir)

        write_files(
            images,
            self._zarr_path,
            self._zarr_configs.n_levels,
            self._zarr_configs.scale_factor,
            True,
            None,
            self._zarr_configs.chunk_shape,
            self._zarr_configs.voxel_size,
            compressor=blosc.Blosc(self._zarr_configs.codec, self._zarr_configs.clevel, shuffle=blosc.SHUFFLE),
            bkg_img_dir=bkg_img_dir,
        )

    def _create_dispim_metadata(self) -> None:
        self._instance_logger.info("Creating xml files for diSPIM data")
        # convert imaging log to acq json
        log_file = self._data_src_dir.joinpath('imaging_log.log')
        toml_dict = read_toml(self._data_src_dir.joinpath('config.toml'))

        # read log file into dict
        log_dict = read_imaging_log(log_file)
        log_dict['data_src_dir'] = (self._data_src_dir.as_posix())
        log_dict['config_toml'] = toml_dict
        # convert to acq json
        acq_json = log_to_acq_json(log_dict)
        acq_json_path = self._data_src_dir.joinpath('acquisition.json')

        try:
            write_acq_json(acq_json, acq_json_path)
            self._instance_logger.info('Finished writing acq json')
        except Exception as e:
            self._instance_logger.error(f"Failed to write acquisition.json: {e}")

        # convert acq json to xml
        is_zarr = True
        condition = "channel=='405'"
        acq_xml = acq_json_to_xml(acq_json, log_dict, self.job_configs.s3_prefix + f'/{self._modality_config.modality.value.abbreviation}.ome.zarr', is_zarr, condition)  # needs relative path to zarr file (as seen by code ocean)

        # write xml to file
        xml_file_path = self._data_src_dir.joinpath('Camera_405.xml')  #
        write_xml(acq_xml, xml_file_path)

    def _create_neuroglancer_link(self) -> None:
        write_json_from_zarr(
            input_zarr=self._zarr_path,
            out_json_dir=str(self._data_src_dir),
            vmin=self._zarr_configs.ng_vmin,
            vmax=self._zarr_configs.ng_vmax
        )

    def run_job(self):
        """Runs the job. Creates a temp directory to compile the files before
        uploading."""
        process_start_time = datetime.now(timezone.utc)

        self._check_if_s3_location_exists()
        with tempfile.TemporaryDirectory(
            dir=self.job_configs.temp_directory
        ) as td:
            self._instance_logger.info("Checking write credentials...")
            self._test_upload(temp_dir=Path(td))

        self._instance_logger.info("Compiling metadata...")
        try:
            self._compile_metadata(
                temp_dir=self._data_src_dir, process_start_time=process_start_time
            )
        except Exception as e:
            self._instance_logger.error(f"Failed to compile metadata: {e}")

        if self.job_configs.platform == Platform.HCR:
            try:
                self._create_dispim_metadata()
            except Exception as e:
                self._instance_logger.error(f"Failed to create diSPIM metadata: {e}")

        self._instance_logger.info("Starting zarr upload...")
        self._upload_zarr()

        if self._zarr_configs.create_ng_link:
            self._instance_logger.info("Creating neuroglancer link...")
            self._create_neuroglancer_link()

        self._instance_logger.info("Starting s3 upload...")
        # Exclude raw image directory, this is uploaded separately
        self._upload_to_s3(
            dir=self._data_src_dir,
            excluded=os.path.join(self._raw_image_dir, "*"),
        )


if __name__ == "__main__":
    sys_args = sys.argv[1:]
    if "--json-args" in sys_args:
        job_configs_from_main = BasicUploadJobConfigs.from_json_args(sys_args)
    else:
        job_configs_from_main = BasicUploadJobConfigs.from_args(sys_args)

    worker_options = {
        "env": {
            "HDF5_PLUGIN_PATH": find_hdf5plugin_path(),
            "HDF5_USE_FILE_LOCKING": "FALSE",
        }
    }

    CLIENT, _ = get_client(get_deployment(), worker_options=worker_options)

    try:
        job = ZarrUploadJob(job_configs=job_configs_from_main)
        job.run_job()
    except Exception as e:
        # Catching the exception is necessary to ensure that the Dask client
        # is properly closed and shut down.
        logging.exception("ZarrUploadJob failed.")
    finally:
        CLIENT.shutdown()
        sleep(_CLIENT_SHUTDOWN_SLEEP_TIME)
        CLIENT.close(timeout=_CLIENT_CLOSE_TIMEOUT)
