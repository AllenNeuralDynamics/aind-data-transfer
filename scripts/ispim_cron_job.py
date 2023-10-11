"""
Script that automatically uploads new iSPIM data from specified directories to the specified s3 cloud bucket. 
This script is intended to be run as a cron job on a server that has access to the raw data directories.




"""

import logging
import os.path
import subprocess
import sys
import time
from pathlib import Path
PathLike = Union[str, Path]
from shutil import copytree, ignore_patterns
from typing import Union, Optional
import datetime
from argschema import ArgSchema, ArgSchemaParser
import yaml
import toml
import json
import pandas as pd

from numcodecs import Blosc

from aind_data_transfer.config_loader.imaging_configuration_loader import (
    ImagingJobConfigurationLoader,
)
from aind_data_schema.data_description import Modality
from aind_data_transfer.readers.imaging_readers import ImagingReaders
from aind_data_transfer.util.file_utils import is_cloud_url, parse_cloud_url
import aind_data_transfer.util.file_utils as file_utils
from aind_data_transfer.transformations.metadata_creation import (
    SubjectMetadata,
    ProceduresMetadata,
    RawDataDescriptionMetadata,
)
from aind_data_transfer.transformations.file_io import read_toml
from smartspim_cron_job import _find_scripts_dir, ConfigFile, CopyDatasets, organize_datasets, pre_upload_smartspim, provide_folder_permissions, get_default_config
# from aind_data_transfer.config_loader.imaging_configuration_loader import ImagingJobConfigurationLoader


from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)




def get_ispim_folders_with_file(
    root_folder: PathLike, search_file: str
) -> Tuple[List]:
    """
    Get the ispim folder that
    contain the search filename in
    the top level directory.

    Parameters
    ----------
    root_folder: PathLike
        Path to the root folder where
        the smartspim datasets are located

    search_file: str
        Filename to search

    Returns
    ----------
    Tuple[List[str]]
        Returns a tuple with two
        positions. The first one corresponds
        to the smartspim datasets that contain
        the provided filename, while the second
        one provides the rejected datasets
    """

    root_folder = Path(root_folder)
    raw_datasets = ImagingReaders.get_raw_data_dir(
        "diSPIM", 
        root_folder
    )

    raw_datasets_accepted = []
    raw_datasets_rejected = []

    for raw_dataset in raw_datasets:
        file_path = root_folder.joinpath(raw_dataset, search_file)

        if os.path.exists(file_path):
            raw_datasets_accepted.append(raw_dataset)

        else:
            raw_datasets_rejected.append(raw_dataset)

    return raw_datasets_accepted, raw_datasets_rejected

def get_ispim_default_processing_config() -> dict:
    """Returns the default config for the processing_manifest.json
    for the ispim pipeline """
    return {
        "stitching": {
            "co_folder": "scratch",
            "channel": "405",
        },
        'segmentation': {
            'channels': ['405'],
            'input_scale': '2',
            'chunksize': '200',
        },
    }

def build_pipeline_config(provided_config: dict, default_config: dict) -> dict:
    """
    Converts user input to pipeline input

    Parameters
    ----------
    provided_config: dict
        Dictionary provided from the processing_manifest.json

    default_config: dict
        Default configuration for each of the steps

    Returns
    ----------
    dict
        Pipeline configuration
    """

    new_config = default_config.copy()

    stitching_config = file_utils.helper_validate_key_dict(
        provided_config, "stitching"
    )

    cell_segmentation_channels = file_utils.helper_validate_key_dict(
        provided_config, "cell_segmentation_channels"
    )

    # Setting stitching channel
    if stitching_config is not None:
        new_config["stitching"]["channel"] = stitching_config["channel"]
    
    #TODO if we need resolution here, add a bit of logic to get it from config.toml
    else:
        default_ch = new_config["stitching"]["channel"]
  
        logger.info(
            f"Using default stitching channel {default_ch}"
        )

    # Setting cell segmentation channels
    if cell_segmentation_channels is not None:
        new_config["segmentation"] = {"input_scale": "0", "chunksize": "500"}
        new_config["segmentation"]["channels"] = cell_segmentation_channels

    else:
        logger.info(f"No segmentation!")

    return new_config

def get_upload_datasets(
    dataset_folder: PathLike,
    config_path: PathLike,
    info_manager_path: Optional[PathLike] = None,
) -> List:
    """
    This function gets the datasets and
    classifies them in pending, uploaded
    and warning.

    - Pending: Datasets that need to be uploaded
    - Uploading: Datasets that are currently being uploaded
    - Uploaded: Datasets that are in the cloud
    - Warning: Datasets that are not ready to be
    uploaded

    Parameters
    ------------------------
    dataset_folder: PathLike
        Path where the datasets are located.

    config_path: PathLike
        Path inside each dataset where the processing_manifest.json
        is located

    Returns
    ------------------------
    List[dict]
        List with the pending datasets.
        This list contains the path and
        smartspim pipeline configuration
        for the datasets
    """

    # Lists for classifying datasets
    pending_datasets = []
    uploading_datasets = []
    uploaded_datasets = []
    warning_datasets = []

    # List of pending datasets with pipeline configuration
    # built just like code ocean accepts it
    pending_datasets_config = []
    default_pipeline_config = get_ispim_default_processing_config()

    # Get smartspim datasets that match data conventions
    dataset_folder = Path(dataset_folder)
    dispim_datasets = ImagingReaders.read_dispim_folders(dataset_folder)

    # Checking status
    for dataset in dispim_datasets:
        dataset_path = dataset_folder.joinpath(dataset)

        # Reading config json
        dataset_config = file_utils.read_json_as_dict(
            dataset_path.joinpath(config_path)
        )

        dataset_path = str(dataset_path)
        dataset_status = file_utils.helper_validate_key_dict(
            dataset_config, "dataset_status"
        )

        if dataset_status is None:
            warning_datasets.append(dataset_path)
            continue

        # Validating status
        dataset_status = dataset_status.casefold()

        # Getting pipeline configuration to dataset
        pipeline_processing = file_utils.helper_validate_key_dict(
            dataset_config, "pipeline_processing"
        )

        if dataset_status == "pending" and pipeline_processing is not None:
            # Datasets to upload
            pending_datasets.append(dataset_path)
            pending_datasets_config.append(
                {
                    "path": dataset_path,
                    "pipeline_processing": build_pipeline_config(
                        pipeline_processing, default_pipeline_config
                    ),
                }
            )

        elif dataset_status == "uploading":
            # Datasets that are currently being uploaded
            uploading_datasets.append(dataset_path)

        # Using in instead of == since I add the upload time and s3 path
        elif "uploaded" in dataset_status:
            # Uploaded datasets
            uploaded_datasets.append(dataset_path)

        else:
            # Datasets that have issues
            warning_datasets.append(dataset_path)

    info_manager = {
        "generated_date": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
        "uploaded": uploaded_datasets,
        "uploading": uploading_datasets,
        "pending": pending_datasets,
        "warning": warning_datasets,
    }

    info_manager_path = (
        info_manager_path
        if info_manager_path is not None
        else dataset_folder.joinpath("status_dispim_datasets.yaml")
    )
    file_utils.write_dict_to_yaml(info_manager, info_manager_path)

    return pending_datasets_config

def get_voxel_size_from_config_toml(config_path: PathLike) -> list:
    """Reads voxel size from config_toml file in dataset folder and returns it as a list
    Parameters
    ----------
    config_path: PathLike
        Path to config.toml file in dataset folder
    
    Returns
    ----------
    List
        List with voxel size in order [X, Y, Z]
    """
    config = read_toml(config_path)

    # Getting voxel size
    Z_voxel_size = config["imaging_specs"]["z_step_size_um"]
    Y_voxel_size = config["tile_specs"]["y_field_of_view_um"]/config["tile_specs"]["column_count_pixels"]
    X_voxel_size = config["tile_specs"]["x_field_of_view_um"]/config["tile_specs"]["row_count_pixels"]

    return [X_voxel_size, Y_voxel_size, Z_voxel_size]

def get_subject_id_from_config_toml(config_path: PathLike) -> str:
    """Reads subject id from config_toml file in dataset folder and returns it as a string
    Parameters
    ----------
    config_path: PathLike
        Path to config.toml file in dataset folder
    
    Returns
    ----------
    str
        Subject id
    """
    config = read_toml(config_path)

    # Getting subject id
    subject_id = config["imaging_specs"]["subject_id"]

    return subject_id

def get_date_time_from_schema_log(schema_log_path: PathLike) -> tuple:

    """Reads session_start_time from schema_log.log file, which is in json format
    and returns date and time as strings
    Parameters
    ----------
    schema_log_path: PathLike
        Path to schema_log.log file in dataset folder
    
    Returns
    ----------
    Date
        The date when the dataset was acquired
        format: YYYY-MM-DD
    Time
        The time when the dataset was acquired
        format: HH-MM-SS

    """

    with open(schema_log_path) as f:
        schema_log = json.load(f)

    # Getting date and time
    date_time = schema_log["session_start_time"]
    date = date_time.split("T")[0]
    time = date_time.split("T")[1].split(".")[0]

    return date, time

def get_acq_datetime_from_schema_log(schema_log_path: PathLike) -> datetime:
    with open(schema_log_path) as f:
        schema_log = json.load(f)

    # Getting date and time
    date_time = schema_log["session_start_time"]
    date_time = date_time.replace("T", " ")

    return date_time

def update_transcode_job_config(config_path: PathLike, dataset_path: PathLike, new_config_path: PathLike = None):
    """
    This function copies the transcode_job_config.yml into a temporary upload data folder
    It updates the dataset path (raw_data_dir), subject_id, and voxel_size. 


    Parameters
    ------------------------
    config_path: PathLike
        Path to the transcode_job_config.yml

    dataset_path: PathLike
        Path to the dataset

    Returns
    ------------------------
    new_config_path: PathLike
        Path to the new transcode_job_config.yml with the updated fields
    """

    # Reading config yaml as dict

    with open(config_path) as f:
        yml_config = yaml.load(f, Loader=yaml.SafeLoader)
    

    # Getting voxel size
    voxel_size = get_voxel_size_from_config_toml(dataset_path.joinpath("config.toml"))


    # Getting dataset id
    subject_id = get_subject_id_from_config_toml(dataset_path.joinpath('config.toml'))

                                                 
    if subject_id is None:
        logger.error("Dataset id not found in config")
        raise ValueError("Dataset id not found in config")
    
    

    # Updating transcode_job_configs yml dict
    yml_config["endpoints"]["raw_data_dir"] = str(dataset_path)
    #convert list to string
    yml_config["transcode_job"]["voxsize"] = " ".join(map(str, voxel_size))
    yml_config["data"]["subject_id"] = subject_id

    #TODO inquire what the best strategy for this is
    # Creating new config path
    if new_config_path is None:
        new_config_path = config_path.parent.joinpath(f"{subject_id}_transcode_job_config.yml")
    # Writing config
    file_utils.write_dict_to_yaml(yml_config, new_config_path)

    return new_config_path

# temporary function to write the zarr upload config 
def write_zarr_upload_sbatch(dataset_path: PathLike, sbatch_path_to_write: PathLike) -> str:
    
    subject_id = get_subject_id_from_config_toml(dataset_path.joinpath('config.toml'))
    

    # acq_date, acq_time = get_date_time_from_schema_log(dataset_path.joinpath("schema_log.log"))
    acq_datetime = get_acq_datetime_from_schema_log(dataset_path.joinpath("schema_log.log"))


    sbatch_script = f"""
#!/bin/bash

#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=8000
#SBATCH --exclude=n69,n74
#SBATCH --tmp=64MB
#SBATCH --time=30:00:00
#SBATCH --partition=aind
#SBATCH --output=/allen/programs/mindscope/workgroups/omfish/carsonb/hpc_outputs/%j_zarr_upload.log
#SBATCH --mail-type=ALL
#SBATCH --mail-user=carson.berry@alleninstitute.org
#SBATCH --ntasks=64
#SBATCH --nodelist=n111

set -e

pwd; date
[[ -f "/allen/programs/mindscope/workgroups/omfish/carsonb/miniconda/bin/activate" ]] && source "/allen/programs/mindscope/workgroups/omfish/carsonb/miniconda/bin/activate" adt-upload-clone

module purge
module load mpi/mpich-3.2-x86_64

# Add 2 processes more than we have tasks, so that rank 0 (coordinator) and 1 (serial process)
# are not sitting idle while the workers (rank 2...N) work
# See https://edbennett.github.io/high-performance-python/11-dask/ for details.
mpiexec -np $(( SLURM_NTASKS + 2 )) python -m aind_data_transfer.jobs.zarr_upload_job --json-args '{"s3_bucket": "aind-open-data","platform": "HCR", "modalities":[{"modality": "SPIM","source":"{dataset_path}", "extra_configs": "/allen/programs/mindscope/workgroups/omfish/mfish/temp_raw/zarr_config.yml"}], "subject_id": "{subject_id}", "acq_datetime": "{acq_datetime}", "force_cloud_sync": "true", "codeocean_domain": "https://codeocean.allenneuraldynamics.org", "metadata_service_domain": "http://aind-metadata-service", "aind_data_transfer_repo_location": "https://github.com/AllenNeuralDynamics/aind-data-transfer", "log_level": "INFO"}'

echo "Done"

date
    """
    #write sbatch script
    assert Path(sbatch_path_to_write).parent.exists(), f"Parent directory of {sbatch_path_to_write} does not exist"
    with open(sbatch_path_to_write, "w") as f:
        f.write(sbatch_script)


    return sbatch_script

def write_csv_from_dataset(dataset_loc: PathLike, csv_path: PathLike):
    """Writes a csv file to be submitted to the metadata service from a dataset that needs uploading
    
    Parameters
    ------------------------
    dataset_loc: PathLike
        Path to the dataset
    
    csv_path: PathLike
        Path to the csv file to write
    
    Returns
    ------------------------
    csv_path: PathLike
        Path to the csv file that was written"""
    
    #get the subject id
    subject_id = get_subject_id_from_config_toml(dataset_loc.joinpath('config.toml'))

    #get the acquisition date and time
    # acq_date, acq_time = get_date_time_from_schema_log(dataset_loc.joinpath("schema_log.log"))
    acq_datetime = get_acq_datetime_from_schema_log(dataset_loc.joinpath("schema_log.log"))

    dict_to_write = {
        "s3_bucket": "aind-open-data",
        "platform": "HCR",
        "modality0": "SPIM",
        "modality0.source": str(dataset_loc),
        "modality0.extra_configs": "/allen/programs/mindscope/workgroups/omfish/mfish/temp_raw/zarr_config.yml",
        "subject_id": subject_id,
        "acq_datetime": acq_datetime,
        "force_cloud_sync": "true",
        "codeocean_domain": "https://codeocean.allenneuraldynamics.org",
        "metadata_service_domain": "http://aind-metadata-service",
        "aind_data_transfer_repo_location": "https://github.com/AllenNeuralDynamics/aind-data-transfer", 
        "log_level": "INFO"
    }

    #write the csv
    pd.DataFrame(dict_to_write, index=[0]).to_csv(csv_path, index=False)

    return csv_path


def main():
    """main to execute the diSPIM job"""
    config_param = ArgSchemaParser(schema_type=ConfigFile)


    #scripts to run during upload
    SUBMIT_TRANSCODE_JOB =  Path(__file__).parent.parent.joinpath('src/aind_data_transfer/jobs/transcode_job.py')
    SUBMIT_ZARR_JOB =       Path(__file__).parent.parent.joinpath('src/aind_data_transfer/jobs/zarr_upload_job.py')

    config_file_path = config_param.args["config_file"]
    config = get_default_config(config_file_path)

    root_folder = Path(config["root_folder"])

    (
        raw_datasets_ready,
        raw_datasets_rejected,
    ) = get_ispim_folders_with_file(
        root_folder=root_folder, search_file="processing_manifest.json"
    )

    provide_folder_permissions(
        root_folder=root_folder, paths=raw_datasets_ready, permissions="755"
    )

    logger.warning(f"Raw datasets rejected: {raw_datasets_rejected}")

    new_dataset_paths, ready_datasets = organize_datasets(
        root_folder,
        raw_datasets_ready,
        config["metadata_service_domain"])


    processing_manifest_path = "derivatives/processing_manifest.json"

    pending_datasets_config = get_upload_datasets(
        dataset_folder=root_folder,
        config_path=processing_manifest_path,  # Pointing to this folder due to data conventions
        info_manager_path=config["info_manager_path"],
    )

    mod = ArgSchemaParser(input_data=config, schema_type=CopyDatasets)
    args = mod.args

    logger.info(f"Uploading {pending_datasets_config}")

    sbatch_file_path = Path(__file__).parent.parent.joinpath('bin/zarr_upload_sbatch.sh')

    #processing all the datasets that are pending
    for dataset in pending_datasets_config:

        dataset_path = dataset["path"]
        dataset_name = Path(dataset_path).stem
        # new_config_path = update_transcode_job_config(config_file_path, dataset_path)

        #temporarily we will rewrite a file to be run with sbatch. It will have the HPC configs 
        #and the args for zarr upload job in a json-like format. 
        zarr_sbatch_cmd = write_zarr_upload_sbatch(dataset_path, sbatch_file_path)

        #the longer term solution TODO, is to write a csv file with the HPC configs and the args for zarr upload job
        #and then read that csv file with the new upload service job. 
    


        if os.path.isdir(dataset_path):
            # dataset_dest_path = dest_data_dir.joinpath(dataset_name)

            if config["transfer_type"]["type"] == "HPC":
                # dataset_dumped = json.dumps(dataset).replace('"', "[token]")
                # cmd = f"""python {SUBMIT_ZARR_JOB} -c {new_config_path}"""
                cmd = f"""sbatch {sbatch_file_path.as_posix()}"""

                # Setting dataset_status as 'uploading'
                pre_upload_smartspim(dataset_path, processing_manifest_path)

                # HPC run
                logger.info(f"Uploading dataset: {dataset_name}")
                for out in file_utils.execute_command(cmd):
                    logger.info(out)

                # Error with slurm logs directory
                time.sleep(30)
            else:
                # Local
                raise NotImplementedError
                # sys.argv = [
                #     "",
                #     f"--input={dataset_path}",
                #     f"--bucket={s3_bucket}",
                #     f"--s3_path={dataset_dest_path}",
                #     f"--nthreads={nthreads}",
                #     "--trigger_code_ocean",
                #     f"--pipeline_config={dataset}",
                #     "--recursive",
                # ]

                # # Local
                # s3_upload.main()

                # now_datetime = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                # dataset_config_path = Path(dataset_path).joinpath(
                #     processing_manifest_path
                # )
                # msg = f"uploaded - Upload time: {now_datetime} - Bucket: {s3_bucket}"

                # file_utils.update_json_key(
                #     json_path=dataset_config_path,
                #     key="dataset_status",
                #     new_value=msg,
                # )

        else:
            logger.warning(f"Path {dataset_path} does not exist. Ignoring...")

    pending_datasets = get_upload_datasets(
        dataset_folder=root_folder,
        config_path=processing_manifest_path,  # Pointing to this folder due to data conventions
        info_manager_path=config["info_manager_path"],
    )

if __name__ == "__main__":
    main()

