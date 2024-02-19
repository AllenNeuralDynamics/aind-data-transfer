import json
import logging
import os
import re
import sys
import time
from datetime import datetime
from importlib.resources import files
from pathlib import Path
from typing import List, Union, Optional

import requests
import yaml
from aind_data_transfer.jobs.zarr_upload_job import ZarrConversionConfigs
from aind_data_transfer.readers.imaging_readers import ImagingReaders
from aind_data_transfer.util import file_utils
from aind_data_transfer.util.s3_utils import get_secret
from pydantic import BaseModel

PathLike = Union[str, Path]


class ExASPIMCronJobConfig(BaseModel):
    root_folder: str
    sif_path: str
    s3_bucket: str
    transfer_service_domain: str
    codeocean_domain: str
    metadata_service_domain: str
    aind_data_transfer_repo_location: str
    zarr_config: ZarrConversionConfigs
    hpc_settings: Optional[dict] = None
    manifest_filename: str = "processing_manifest.json"
    force_cloud_sync: bool = False
    log_level: str = "INFO"

    @classmethod
    def from_config(cls, config_path: str) -> 'ExASPIMCronJobConfig':
        with open(config_path, 'r') as file:
            config_data = yaml.safe_load(file)
        return cls(**config_data)


def _configure_logger(level: int = logging.INFO) -> logging.Logger:
    """
    Configures and returns a logger with a specific format and level.

    Parameters
    ----------
    level : int
        Logging level.

    Returns
    -------
    logging.Logger
        Configured logger with stream handler at INFO level.
    """
    logger = logging.getLogger(__name__)
    logger.setLevel(level)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    return logger


def _get_script_template() -> str:
    """
    Fetches the SLURM job script template.

    Returns
    -------
    str
        The content of the SLURM template script.
    """
    template_path = files(
        'aind_data_transfer.jobs.cronjobs.templates'
    ).joinpath('zarr_upload_job_template.sh')
    return template_path.read_text("utf-8")


def _get_folders_with_file(
    root_folder: PathLike, search_file: PathLike
) -> List[Path]:
    """
    Scans a root folder for subdirectories containing a specific file,
    categorizing them as accepted or rejected.

    Parameters
    ----------
    root_folder : PathLike
        The root directory to scan.
    search_file : PathLike
        The file to search for in each subdirectory.

    Returns
    -------
    List[Path]
        A list of accepted subdirectories.
    """
    root_folder = Path(root_folder)
    raw_datasets = ImagingReaders.read_exaspim_folders(root_folder)
    raw_datasets_accepted = []

    for raw_dataset in raw_datasets:
        file_path = root_folder / raw_dataset / search_file
        if file_path.exists():
            raw_datasets_accepted.append(file_path.parent)

    return raw_datasets_accepted


def _get_sbatch_script(json_dict: dict, sif_path: str) -> str:
    """
    Generates an SBATCH script for a given dataset by reading from a
    template file.

    Parameters
    ----------
    json_dict : dict
        The dictionary containing the dataset information.
    sif_path : str
        The path to the singularity image file.

    Returns
    -------
    str
        The generated SBATCH script content.
    """
    template = _get_script_template()

    script = template.format(
        sif_path=sif_path, json_args=json.dumps(json_dict)
    )
    return script


def _build_jobs_request(
    hpc_settings: dict, slurm_script: str, job_config: dict
):
    """
    Builds a request to submit a set of jobs to the HPC cluster.

    Parameters
    ----------
    hpc_settings : dict
        The HPC settings for the job.
    slurm_script : str
        The SLURM script to execute.
    job_config : dict
        The configuration for the job.

    Returns
    -------
    dict
        The request containing the job information.
    """

    request = {
        "jobs": [{
            "hpc_settings": json.dumps(hpc_settings),
            "script": slurm_script,
            "upload_job_settings": json.dumps(job_config)
        }]
    }
    return request


def _submit_jobs_request(
    domain: str, request_json: dict
) -> requests.Response:
    """
    Submits a set of jobs to the HPC cluster.

    Parameters
    ----------
    request_json : dict
        The request containing the job information.

    Returns
    -------
    dict
        The response from the HPC cluster.
    """
    response = requests.post(
        url=f"{domain}/api/submit_hpc_jobs", json=request_json
    )
    return response


def _get_secret_json() -> dict:
    """
    Retrieves the secret from AWS Secrets Manager.

    Returns
    -------
    dict
        The secret as a dictionary.
    """
    try:
        secret_name = os.environ['SECRET_NAME']
    except KeyError:
        raise ValueError("secret_name environment variable not set")

    try:
        secret = get_secret(secret_name, "us-west-2")
    except Exception as e:
        raise ValueError(f"Error retrieving secret: {e}")

    try:
        secret_json = json.loads(secret)
    except json.JSONDecodeError as e:
        raise ValueError(f"Error decoding secret: {e}")

    return secret_json


class ExASPIMCronJob:
    """
    A class to manage cron job operations for ExASPIM data transfers.

    Attributes
    ----------
    config : ExASPIMCronJobConfig
        The configuration for the cron job.

    Methods
    -------
    run():
        Executes the cron job, processing pending datasets.
    get_pending_datasets(dataset_folder, manifest_path):
        Identifies and returns a list of datasets pending for upload.
    """

    def __init__(
        self, config: ExASPIMCronJobConfig
    ):
        """
         Parameters
         ----------
            config : ExASPIMCronJobConfig
                The configuration for the cron job.
         """
        self._logger = _configure_logger()
        self._logger.info("ExASPIMCronJob: __init__")

        self.config = config
        self.config.hpc_settings = self._resolve_hpc_settings(
            config.hpc_settings
        )

    def _resolve_hpc_settings(self, hpc_settings: dict) -> dict:
        """
        Resolves the HPC settings for the cron job.

        Parameters
        ----------
        hpc_settings : dict
            The HPC settings to resolve.

        Returns
        -------
        dict
            The resolved HPC settings.
        """
        if hpc_settings is None:
            hpc_settings = {}
        secret = _get_secret_json()
        try:
            hpc_settings['environment'] = {
                "HPC_TOKEN": secret['token'],
                "HPC_USERNAME": secret['user_name'],
                "HPC_PASSWORD": secret['password'],
                "HPC_HOST": os.environ['HPC_HOST'],
                "HPC_API_ENDPOINT": os.environ['HPC_API_ENDPOINT'],
                "HPC_PORT": os.environ['HPC_PORT']
            }
        except KeyError as ke:
            self._logger.error(
                f"Error reading secret: {ke}"
            )
            raise ke
        return hpc_settings

    def run(self) -> None:
        """
        Executes the cron job, processing pending datasets.
        """
        self._logger.info("ExASPIMCronJob: run")

        pending_datasets = self.get_pending_datasets(
            self.config.root_folder, self.config.manifest_filename
        )

        for ds in pending_datasets:
            self._logger.info(f"Processing dataset: {ds}")

            self.config.hpc_settings['name'] = ds.name

            zarr_config_path = ds / "zarr_config.yml"
            try:
                d = self.config.zarr_config.dict()
                if 'chunk_shape' in d and d['chunk_shape'] is not None:
                    d['chunk_shape'] = list(d['chunk_shape'])
                if 'voxel_size' in d and d['voxel_size'] is not None:
                    d['voxel_size'] = list(d['voxel_size'])
                file_utils.write_dict_to_yaml(
                    d, zarr_config_path
                )
            except Exception as e:
                self._logger.error(f"Error writing zarr config: {e}")
                continue

            job_config = self._build_job_config(
                self.config,
                ds,
                zarr_config_path
            )

            slurm_script = _get_sbatch_script(
                job_config, self.config.sif_path
            )
            self._logger.info(slurm_script)

            submit_jobs_request = _build_jobs_request(
                self.config.hpc_settings, slurm_script, job_config
            )
            submit_job_response = _submit_jobs_request(
                self.config.transfer_service_domain, submit_jobs_request
            )
            response_json = submit_job_response.json()
            self._logger.info(
                f"Job submission response: {response_json}"
            )
            if submit_job_response.status_code == 200:
                self._logger.info(
                    f"Job submitted successfully: {response_json['data']['responses'][0]['job_id']}"
                )
                status = "uploading"

            else:
                self._logger.error(
                    f"Job submission failed: {response_json['message']}"
                )
                status = "failed"

            self._update_dataset_status(
                ds, self.config.manifest_filename, status
            )

            time.sleep(1)

    def get_pending_datasets(
        self, dataset_folder: PathLike, manifest_path: PathLike
    ) -> List[Path]:
        """
        Identifies datasets within a specified folder that are marked as
        pending in their processing manifest.

        Parameters
        ----------
        dataset_folder : Union[str, Path]
            The folder to search for datasets.
        manifest_path : Union[str, Path]
            The relative path to the manifest file from each dataset directory.

        Returns
        -------
        List[Path]
            A list of paths to datasets marked as pending.
        """
        pending_datasets = []
        dataset_folder = Path(dataset_folder)
        exaspim_datasets = _get_folders_with_file(
            dataset_folder, manifest_path
        )

        for dataset_path in exaspim_datasets:
            with open(os.path.join(dataset_path, manifest_path), "r") as f:
                processing_manifest = json.load(f)
            try:
                dataset_status = processing_manifest["dataset_status"][
                    "status"].lower()
            except KeyError as ke:
                self._logger.error(
                    f"Error reading {manifest_path} for {dataset_path}: {ke}"
                )
                continue

            if dataset_status == "pending":
                pending_datasets.append(dataset_path)

        return pending_datasets

    def _build_job_config(
        self,
        config: ExASPIMCronJobConfig,
        dataset_path: PathLike,
        zarr_config_path: PathLike
    ) -> dict:
        """
        Builds the job configuration for a given dataset.

        Parameters
        ----------
        config : ExASPIMCronJobConfig
            The configuration for the cron job.
        dataset_path : PathLike
            The path to the dataset.
        zarr_config_path : PathLike
            The path to the zarr configuration file.

        Returns
        -------
        dict
            A dictionary containing configuration for the job.
        """
        dataset_path = str(dataset_path)

        m = re.search(
            ImagingReaders.SourceRegexPatterns.exaspim_acquisition.value,
            dataset_path
        )
        subject_id = m.group(1)

        acq_datetime = f"{m.group(2)}-{m.group(3)}-{m.group(4)}"
        # Note the space
        acq_datetime += f" {m.group(5)}:{m.group(6)}:{m.group(7)}"

        job_config = {
            "s3_bucket": config.s3_bucket,
            "platform": "exaSPIM",
            "modalities": [{
                "modality": "SPIM",
                "source": str(dataset_path),
                "extra_configs": str(zarr_config_path)
            }],
            "subject_id": subject_id,
            "acq_datetime": acq_datetime,
            "force_cloud_sync": config.force_cloud_sync,
            "codeocean_domain": config.codeocean_domain,
            "metadata_service_domain": config.metadata_service_domain,
            "aind_data_transfer_repo_location": config.aind_data_transfer_repo_location,
            "log_level": config.log_level
        }

        return job_config

    def _update_dataset_status(
        self, dataset_path: PathLike, manifest_filename: str, new_status: str
    ) -> None:
        """
        Updates the processing manifest of a dataset to indicate it is
        currently being uploaded.

        Parameters
        ----------
        dataset_path : PathLike
            The path to the dataset whose manifest needs updating.
        manifest_filename : str
            The name of the manifest file to update.
        """

        dataset_config_path = Path(dataset_path).joinpath(
            manifest_filename
        )

        status_date = datetime.now().strftime('%Y-%m-%d')
        status_time = datetime.now().strftime('%H-%M-%S')
        msg = {
            "status": new_status,
            "status_date": status_date,
            "status_time": status_time
        }

        file_utils.update_json_key(
            json_path=dataset_config_path, key="dataset_status", new_value=msg
        )
        self._logger.info(f"Updating state to 'uploading'")


if __name__ == "__main__":
    # get config path from sys args and validate
    if len(sys.argv) != 2:
        raise ValueError("Usage: python exaspim_cron_job.py <config_path>")

    config_path = sys.argv[1]
    if not os.path.isfile(config_path):
        raise FileNotFoundError(f"File not found: {config_path}")

    cron_job_config = ExASPIMCronJobConfig.from_config(config_path)

    cron_job = ExASPIMCronJob(cron_job_config)
    cron_job.run()
