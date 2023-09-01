import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from aind_data_schema.data_description import Modality
from aind_data_transfer.config_loader.base_config import ConfigError, BasicUploadJobConfigs
from aind_data_transfer.jobs.basic_job import BasicJob
from aind_data_transfer.util.s3_utils import upload_to_s3


class ZarrUploadJob(BasicJob):

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
            excluded=excluded
        )

    def run_job(self):
        """Runs the job. Creates a temp directory to compile the files before
        uploading."""
        process_start_time = datetime.now(timezone.utc)
        if len(self.job_configs.modalities) != 1:
            raise ConfigError("ZarrUploadJob only supports one modality.")
        modality_config = self.job_configs.modalities[0]
        if modality_config.modality not in (Modality.EXASPIM, Modality.DISPIM):
            raise ConfigError("ZarrUploadJob only supports EXASPIM and DISPIM modalities.")
        data_src_dir = modality_config.source

        self._check_if_s3_location_exists()
        with tempfile.TemporaryDirectory(
            dir=self.job_configs.temp_directory
        ) as td:
            self._instance_logger.info("Checking write credentials...")
            self._test_upload(temp_dir=Path(td))
        self._instance_logger.info("Compiling metadata...")
        self._compile_metadata(
            temp_dir=data_src_dir, process_start_time=process_start_time
        )
        self._instance_logger.info("Starting s3 upload...")
        # Exclude raw image directory, this is uploaded separately
        self._upload_to_s3(dir=data_src_dir, excluded=data_src_dir / modality_config.modality.value.abbreviation)
        # TODO: zarr conversion
        self._instance_logger.info("Initiating code ocean pipeline...")
        self._trigger_codeocean_pipeline()


if __name__ == "__main__":
    sys_args = sys.argv[1:]
    if "--json-args" in sys_args:
        job_configs_from_main = BasicUploadJobConfigs.from_json_args(sys_args)
    else:
        job_configs_from_main = BasicUploadJobConfigs.from_args(sys_args)
    job = BasicJob(job_configs=job_configs_from_main)
    job.run_job()
