import argparse
import json
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from aind_data_schema.data_description import Modality, ExperimentType
from aind_data_transfer.config_loader.base_config import (
    ConfigError,
    BasicUploadJobConfigs,
    ModalityConfigs,
    BasicJobEndpoints,
)
from aind_data_transfer.jobs.basic_job import BasicJob
from aind_data_transfer.transformations.converters import log_to_acq_json, acq_json_to_xml
from aind_data_transfer.transformations.ome_zarr import write_files
from aind_data_transfer.util.dask_utils import get_client
from aind_data_transfer.util.env_utils import find_hdf5plugin_path
from aind_data_transfer.util.file_utils import get_images
from aind_data_transfer.util.s3_utils import upload_to_s3
from aind_data_transfer.transformations.file_io import read_toml, read_imaging_log, write_acq_json, write_xml

from numcodecs import blosc
from pydantic import Field


_CLIENT_CLOSE_TIMEOUT = 300  # seconds


class ZarrUploadJobConfigs(BasicUploadJobConfigs):
    """Configurations for ZarrUploadJob."""

    n_levels: Optional[int] = Field(
        1, description="Number of levels to use for the pyramid. Default is 1."
    )
    scale_factor: Optional[int] = Field(
        2, description="Scale factor to use for the pyramid. Default is 2."
    )
    chunk_shape: Optional[tuple] = Field(
        (1, 1, 256, 256, 256),
        description="5D Chunk shape to use for the zarr Array. Default is (1, 1, 256, 256, 256).",
    )
    voxel_size: Optional[tuple] = Field(
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
    exclude_patterns: Optional[list] = Field(
        None,
        description="List of patterns to exclude from the zarr conversion. Default is None.",
    )

    @classmethod
    def from_args(cls, args: list):
        """Adds ability to construct settings from a list of arguments."""

        def _help_message(key: str) -> str:
            """Construct help message from field description"""
            return ZarrUploadJobConfigs.schema()["properties"][key][
                "description"
            ]

        parser = argparse.ArgumentParser()
        # Required
        parser.add_argument(
            "-a",
            "--acq-date",
            required=True,
            type=str,
            help="Date data was acquired, yyyy-MM-dd or dd/MM/yyyy",
        )
        parser.add_argument(
            "-b",
            "--s3-bucket",
            required=True,
            type=str,
            help=_help_message("s3_bucket"),
        )
        parser.add_argument(
            "-e",
            "--experiment-type",
            required=True,
            type=str,
            help=_help_message("experiment_type"),
        )
        parser.add_argument(
            "-m",
            "--modalities",
            required=True,
            type=str,
            help=(
                f"String that can be parsed as json list where each entry "
                f"has fields: {ModalityConfigs.__fields__}"
            ),
        )
        parser.add_argument(
            "-p",
            "--endpoints-parameters",
            required=True,
            type=str,
            help=(
                "Either a string that can be parsed as a json object or a name"
                " that points to an aws parameter store location"
            ),
        )
        parser.add_argument(
            "-s",
            "--subject-id",
            required=True,
            type=str,
            help=_help_message("subject_id"),
        )
        parser.add_argument(
            "-t",
            "--acq-time",
            required=True,
            type=str,
            help="Time data was acquired, HH-mm-ss or HH:mm:ss",
        )
        # Optional
        parser.add_argument(
            "-l",
            "--log-level",
            required=False,
            type=str,
            help=_help_message("log_level"),
        )
        parser.add_argument(
            "-n",
            "--temp-directory",
            required=False,
            type=str,
            help=_help_message("temp_directory"),
        )
        parser.add_argument(
            "-v",
            "--behavior-dir",
            required=False,
            type=str,
            help=_help_message("behavior_dir"),
        )
        parser.add_argument(
            "-x",
            "--metadata-dir",
            required=False,
            type=str,
            help=_help_message("metadata_dir"),
        )
        parser.add_argument(
            "--dry-run", action="store_true", help=_help_message("dry_run")
        )
        parser.add_argument(
            "--metadata-dir-force",
            action="store_true",
            help=_help_message("metadata_dir_force"),
        )
        parser.add_argument(
            "--force-cloud-sync",
            action="store_true",
            help=_help_message("force_cloud_sync"),
        )
        parser.add_argument(
            "-i",
            "--codeocean-process-capsule-id",
            required=False,
            type=str,
            help=_help_message("codeocean_process_capsule_id"),
        )
        # Now add arguments for ZarrUploadJobConfigs
        parser.add_argument(
            "--n-levels",
            required=False,
            type=int,
            help=_help_message("n_levels"),
        )
        parser.add_argument(
            "--scale-factor",
            required=False,
            type=int,
            help=_help_message("scale_factor"),
        )
        parser.add_argument(
            "--chunk-shape",
            required=False,
            type=str,
            help=_help_message("chunk_shape"),
        )
        parser.add_argument(
            "--voxel-size",
            required=False,
            type=str,
            help=_help_message("voxel_size"),
        )
        parser.add_argument(
            "--codec",
            required=False,
            type=str,
            help=_help_message("codec"),
        )
        parser.add_argument(
            "--clevel",
            required=False,
            type=int,
            help=_help_message("clevel"),
        )
        parser.add_argument(
            "--do-bkg-subtraction",
            action="store_true",
            help=_help_message("do_bkg_subtraction"),
        )
        parser.add_argument(
            "--exclude-patterns",
            required=False,
            type=str,
            help=_help_message("exclude_patterns"),
        )
        parser.set_defaults(dry_run=False)
        parser.set_defaults(metadata_dir_force=False)
        parser.set_defaults(force_cloud_sync=False)
        job_args = parser.parse_args(args)
        acq_date = job_args.acq_date
        acq_time = job_args.acq_time
        behavior_dir = (
            None
            if job_args.behavior_dir is None
            else Path(os.path.abspath(job_args.behavior_dir))
        )
        metadata_dir = (
            None
            if job_args.metadata_dir is None
            else Path(os.path.abspath(job_args.metadata_dir))
        )
        temp_directory = (
            None
            if job_args.temp_directory is None
            else Path(os.path.abspath(job_args.temp_directory))
        )
        log_level = (
            BasicUploadJobConfigs.__fields__["log_level"].default
            if job_args.log_level is None
            else job_args.log_level
        )
        # The user can define the endpoints explicitly as an object that can be
        # parsed with json.loads()
        try:
            params_from_json = BasicJobEndpoints.parse_obj(
                json.loads(job_args.endpoints_parameters)
            )
            endpoints_param_dict = params_from_json.dict()
        # If the endpoints are not defined explicitly, then we can check if
        # the input defines an aws parameter store name
        except json.decoder.JSONDecodeError:
            endpoints_param_dict = {
                "aws_param_store_name": job_args.endpoints_parameters
            }
        if job_args.codeocean_process_capsule_id is not None:
            endpoints_param_dict[
                "codeocean_process_capsule_id"
            ] = job_args.codeocean_process_capsule_id
        modalities_json = json.loads(job_args.modalities)
        modalities = [ModalityConfigs.parse_obj(m) for m in modalities_json]
        return cls(
            s3_bucket=job_args.s3_bucket,
            subject_id=job_args.subject_id,
            experiment_type=ExperimentType(job_args.experiment_type),
            modalities=modalities,
            acq_date=acq_date,
            acq_time=acq_time,
            behavior_dir=behavior_dir,
            temp_directory=temp_directory,
            metadata_dir=metadata_dir,
            dry_run=job_args.dry_run,
            metadata_dir_force=job_args.metadata_dir_force,
            force_cloud_sync=job_args.force_cloud_sync,
            log_level=log_level,
            n_levels=job_args.n_levels,
            scale_factor=job_args.scale_factor,
            chunk_shape=job_args.chunk_shape,
            voxel_size=job_args.voxel_size,
            codec=job_args.codec,
            clevel=job_args.clevel,
            do_bkg_subtraction=job_args.do_bkg_subtraction,
            exclude_patterns=job_args.exclude_patterns,
            **endpoints_param_dict,
        )


class ZarrUploadJob(BasicJob):
    job_configs: ZarrUploadJobConfigs

    def __init__(self, job_configs: ZarrUploadJobConfigs):
        super().__init__(job_configs=job_configs)

        if len(self.job_configs.modalities) != 1:
            raise ConfigError("ZarrUploadJob only supports one modality at a time.")

        self._modality_config = self.job_configs.modalities[0]
        if self._modality_config.modality not in (Modality.EXASPIM, Modality.DISPIM):
            raise ConfigError(
                "ZarrUploadJob only supports EXASPIM and DISPIM modalities."
            )

        self._data_src_dir = self._modality_config.source
        if not self._data_src_dir.exists():
            raise FileNotFoundError(
                f"data source directory {self._data_src_dir} does not exist"
            )
        self._raw_image_dir = (
            self._data_src_dir / self._modality_config.modality.value.abbreviation
        )
        if not self._raw_image_dir.exists():
            self._raw_image_dir = self._data_src_dir / "micr"
            if not self._raw_image_dir.exists():
                raise FileNotFoundError(
                    f"raw image directory {self._raw_image_dir} does not exist"
                )
        self._derivatives_dir = self._data_src_dir / "derivatives"
        if not self._derivatives_dir.exists():
            raise FileNotFoundError(
                f"derivatives directory {self._derivatives_dir} does not exist"
            )
        self._zarr_path = f"s3://{self.job_configs.s3_bucket}/{self.job_configs.s3_prefix}/{self._modality_config.modality.value.abbreviation}.zarr"

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
                self._raw_image_dir, exclude=self.job_configs.exclude_patterns
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

        compressor = blosc.Blosc(
            self.job_configs.codec,
            self.job_configs.clevel,
            shuffle=blosc.SHUFFLE,
        )

        bkg_img_dir = None
        if self.job_configs.do_bkg_subtraction:
            bkg_img_dir = str(self._derivatives_dir)

        write_files(
            images,
            self._zarr_path,
            self.job_configs.n_levels,
            self.job_configs.scale_factor,
            True,
            None,
            self.job_configs.chunk_shape,
            self.job_configs.voxel_size,
            compressor,
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
        acq_xml = acq_json_to_xml(acq_json, log_dict, self._data_src_dir.stem + '/diSPIM.zarr', is_zarr, condition)  # needs relative path to zarr file (as seen by code ocean)

        # write xml to file
        xml_file_path = self._data_src_dir.joinpath('Camera_405.xml')  #
        write_xml(acq_xml, xml_file_path)

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
        self._compile_metadata(
            temp_dir=self._data_src_dir, process_start_time=process_start_time
        )
        if self._modality_config.modality == Modality.DISPIM:
            self._create_dispim_metadata()

        self._instance_logger.info("Starting s3 upload...")
        # Exclude raw image directory, this is uploaded separately
        self._upload_to_s3(
            dir=self._data_src_dir,
            excluded=os.path.join(self._raw_image_dir, "*"),
        )
        self._instance_logger.info("Starting zarr upload...")
        self._upload_zarr()


if __name__ == "__main__":
    sys_args = sys.argv[1:]
    if "--json-args" in sys_args:
        job_configs_from_main = ZarrUploadJobConfigs.from_json_args(sys_args)
    else:
        job_configs_from_main = ZarrUploadJobConfigs.from_args(sys_args)
    worker_options = {
        "env": {
            "HDF5_PLUGIN_PATH": find_hdf5plugin_path(),
            "HDF5_USE_FILE_LOCKING": "FALSE",
        }
    }
    if os.getenv("SLURM_JOBID") is None:
        deployment = "local"
    else:
        # we're running on the Allen HPC
        deployment = "slurm"
    CLIENT, _ = get_client(deployment, worker_options=worker_options)

    try:
        job = ZarrUploadJob(job_configs=job_configs_from_main)
        job.run_job()
    finally:
        CLIENT.shutdown()
        CLIENT.close(timeout=_CLIENT_CLOSE_TIMEOUT)
