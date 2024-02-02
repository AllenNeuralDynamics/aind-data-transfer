"""
Script that automatically uploads new iSPIM data from specified directories to the specified s3 cloud bucket. 
This script is intended to be run as a cron job on a server that has access to the raw data directories.


"""

import logging
import os.path
import subprocess
import sys
import time
from typing import Optional, Tuple, Union
from pathlib import Path
PathLike = Union[str, Path]
from shutil import copytree, ignore_patterns
from typing import Union, Optional
from datetime import datetime
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
from smartspim_cron_job import ConfigFile, CopyDatasets, organize_datasets, pre_upload_smartspim, provide_folder_permissions
# from aind_data_transfer.config_loader.imaging_configuration_loader import ImagingJobConfigurationLoader

#for plotting tile metrics

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def get_default_config(filename: str) -> dict:
    """
    Get default configuration from a YAML file.

    Parameters
    ------------------------
    filename: str
        String where the YAML file is located.

    Returns
    ------------------------
    Dict
        Dictionary with the configuration
    """

    # filename = Path(os.path.dirname(__file__)).joinpath(filename)
    filename = Path(filename)


    config = None
    try:
        with open(filename, "r") as stream:
            config = yaml.safe_load(stream)
    except Exception as error:
        raise error

    return config


def get_ispim_folders_with_file(
    root_folder: PathLike, search_file: str
) -> Tuple[list]:
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
    raw_datasets = ImagingReaders.read_dispim_folders(
         #TODO change back to diSPIM when running from VAST
        root_folder
    )
    # raw_datasets = [Path(raw_dataset).name for raw_dataset in raw_datasets]
    raw_datasets_accepted = []
    raw_datasets_rejected = []

    print(f'raw_datasets: {raw_datasets}')
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
) -> list:
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

    print(f'dispim_datasets: {dispim_datasets}')
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


        # print(f'dataset_status: {dataset_status}')
        # print(f'is datasets_status a dict? {isinstance(dataset_status, dict)}')
        #if dataset_status is a dict, get just the status
        if isinstance(dataset_status, dict):
            dataset_status = dataset_status['status']

        # print(f'dataset_status: {dataset_status}')

        if dataset_status is None:
            warning_datasets.append(dataset_path)
            continue

        # Validating status
        dataset_status = dataset_status.casefold()

        # Getting pipeline configuration to dataset
        pipeline_processing = file_utils.helper_validate_key_dict(
            dataset_config, "pipeline_processing"
        )

        # print(f'pipeline_processing: {pipeline_processing}')

        #TODO add pipeline processing to the processing_manifest.json
        if dataset_status == "pending":
            # Datasets to upload
            pending_datasets.append(dataset_path)
            pending_datasets_config.append({"path": dataset_path})



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
        
        if pipeline_processing is not None:
            pending_datasets_config.append(
                {
                    "path": dataset_path,
                    "pipeline_processing": build_pipeline_config(
                        pipeline_processing, default_pipeline_config
                    ),
                }
            )

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

    return [Z_voxel_size, Y_voxel_size, X_voxel_size]

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
        # f = f.read()
        f = f.replace("'", '"')
        schema_log = json.load(f)

    # Getting date and time
    date_time = schema_log["session_start_time"]
    date = date_time.split("T")[0]
    time = date_time.split("T")[1].split(".")[0]

    return date, time

def get_acq_datetime_from_schema_log(schema_log_path: PathLike) -> datetime:
    with open(schema_log_path) as f:
        lines = f.readlines()
        # f = f.replace("'", '"')

    for line in lines:
        line = line.replace("\'", "\"")
        #remove windows path   
        line = line.replace('WindowsPath(','').replace(')', '')
        f = line.replace("'", '"')
        schema_log = json.loads(f)

        if "session_start_time" in schema_log:
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

def update_zarr_job_config(dataset_path: PathLike, new_config_path: PathLike = None, n_levels: int = 5, chunk_shape: list = [1,1,128,128,128]):
    """This file writes a very simple config file for the zarr upload job.
    Typically this file contains only the n_levels, chunk_shape, and voxel_size.
    
    Parameters
    ------------------------
    dataset_path: PathLike
        Path to the dataset
    new_config_path: PathLike
        Path to the new zarr_job_config.yml with the updated fields
    Returns
    ------------------------
    new_config_path: PathLike
        Path to the new zarr_job_config.yml with the updated fields

    """
    voxel_size = get_voxel_size_from_config_toml(Path(dataset_path).joinpath("config.toml"))

    yml_dict = {"n_levels": n_levels, "chunk_shape": chunk_shape, "voxel_size": voxel_size, "do_bkg_subtraction": False}
    file_utils.write_dict_to_yaml(yml_dict, new_config_path)

    return new_config_path

    

# temporary function to write the zarr upload config 
def write_zarr_upload_sbatch(dataset_path: PathLike, sbatch_path_to_write: PathLike) -> str:
    
    subject_id = get_subject_id_from_config_toml(Path(dataset_path).joinpath('config.toml'))
    
    #clean up subject_id so that it is just the subject id (6 digits)
    subject_id = subject_id.split("-")[0]

    dataset_path = str(dataset_path)

    # acq_date, acq_time = get_date_time_from_schema_log(dataset_path.joinpath("schema_log.log"))
    acq_datetime = get_acq_datetime_from_schema_log(dataset_path + "/schema_log.log")

    config_file_loc = '/allen/aind/scratch/diSPIM/zarr_config_auto.yml'
    update_zarr_job_config(dataset_path, config_file_loc)

    #TODO update s3 bucket to be configurable
    my_json_dict = {"s3_bucket": "aind-open-data","platform": "HCR", "modalities":[{"modality": "SPIM","source": dataset_path, "extra_configs": config_file_loc}], "subject_id": subject_id, "acq_datetime": acq_datetime, "force_cloud_sync": "true", "codeocean_domain": "https://codeocean.allenneuraldynamics.org", "metadata_service_domain": "http://aind-metadata-service", "aind_data_transfer_repo_location": "https://github.com/AllenNeuralDynamics/aind-data-transfer", "log_level": "INFO"}

    #convert dict to json
    my_json_string = json.dumps(my_json_dict)


    sbatch_script = f"""#!/bin/bash
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=8000
#SBATCH --exclude=n69,n74
#SBATCH --tmp=64MB
#SBATCH --time=30:00:00
#SBATCH --partition=aind
#SBATCH --output=/allen/aind/scratch/carson.berry/hpc_outputs/%j_zarr_upload.log
#SBATCH --mail-type=ALL
#SBATCH --mail-user=carson.berry@alleninstitute.org
#SBATCH --ntasks=64

set -e

pwd; date
[[ -f "/allen/programs/mindscope/workgroups/omfish/carsonb/miniconda/bin/activate" ]] && source "/allen/programs/mindscope/workgroups/omfish/carsonb/miniconda/bin/activate" adt-upload-clone

module purge
module load mpi/mpich-3.2-x86_64

# Add 2 processes more than we have tasks, so that rank 0 (coordinator) and 1 (serial process)
# are not sitting idle while the workers (rank 2...N) work
# See https://edbennett.github.io/high-performance-python/11-dask/ for details.
mpiexec -np $(( SLURM_NTASKS + 2 )) python -m aind_data_transfer.jobs.zarr_upload_job --json-args '{my_json_string}'

echo "Done"
date
    """
    #write sbatch script
    assert Path(sbatch_path_to_write).parent.exists(), f"Parent directory of {sbatch_path_to_write} does not exist"
    with open(sbatch_path_to_write, "w") as f:
        f.write(sbatch_script)

    return sbatch_script

def write_processing_manifest(dataset_path: PathLike):
    """Writes a processing manifest to the derivatives directory of a dataset
    Always writes it as pending (upload) status

    Parameters
    ------------------------
    dataset_path: PathLike
        Path to the dataset
    
    Returns
    ------------------------
    file_loc: PathLike
        Path to the processing manifest
    

    """
    file_loc = Path(dataset_path).joinpath("derivatives", "processing_manifest.json")

    #get the subject id
    subject_id = get_subject_id_from_config_toml(Path(dataset_path).joinpath('config.toml'))

    #get the time now
    now = datetime.now()
    date = now.strftime("%Y-%m-%d")
    time = now.strftime("%H:%M:%S")

    #write the manifest dict
    manifest_dict = {"specimen_id": subject_id, 
                    "dataset_status":{
                        "status": "pending", 
                        "status_date": date, 
                        "status_time": time}
                    }

    #write the json manifest to file
    try: 
        with open(file_loc, "w") as f:
            json.dump(manifest_dict, f, indent=4)
    except: 
        logger.error(f"Could not write processing manifest to {file_loc}")
        raise ValueError(f"Could not write processing manifest to {file_loc}")

    return file_loc

def check_if_processing_manifest_exists(dataset_path: PathLike):
    """Checks if a processing manifest exists for a dataset"""
    file_loc = Path(dataset_path).joinpath("derivatives", "processing_manifest.json")
    return file_loc.exists()

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


# write an sbatch script to submit the tile qc job
def write_tile_qc_sbatch_script(dataset_path: PathLike, sbatch_path_to_write: PathLike):
    """Writes an sbatch script to submit the tile qc job for a dataset
    
    Parameters
    ------------------------
    dataset_path: PathLike
        Path to the dataset
    
    sbatch_path_to_write: PathLike
        Path to write the sbatch script
    
    Returns
    ------------------------
    sbatch_script: str
        The sbatch script that was written"""
    
    #dataset_path = Path(dataset_path).stem
    print(f'Dataset path being passed to write_tile_qc_sbatch_script: {dataset_path}')

    #write the sbatch script
    sbatch_script = f"""#!/bin/bash
#SBATCH --cpus-per-task=8
#SBATCH --mem-per-cpu=8000
#SBATCH --exclude=n69,n74
#SBATCH --tmp=64MB
#SBATCH --time=2:00:00
#SBATCH --partition=aind
#SBATCH --output=/allen/aind/scratch/carson.berry/hpc_outputs/%j_plot_tile_qc.log
#SBATCH --mail-type=ALL
#SBATCH --mail-user=carson.berry@alleninstitute.org
#SBATCH --ntasks=1

set -e

pwd; date
[[ -f "/allen/programs/mindscope/workgroups/omfish/carsonb/miniconda/bin/activate" ]] && source "/allen/programs/mindscope/workgroups/omfish/carsonb/miniconda/bin/activate" adt-upload-clone

# Add 2 processes more than we have tasks, so that rank 0 (coordinator) and 1 (serial process)
# are not sitting idle while the workers (rank 2...N) work
# See https://edbennett.github.io/high-performance-python/11-dask/ for details.

python -m /allen/aind/scratch/diSPIM_QC/hcr_review_tiles/src/hcr_review_tiles/plot_lightsheet_data_for_tile_from_tiff.py {dataset_path}

echo "Done"
date
"""
    assert Path(sbatch_path_to_write).parent.exists(), f"Parent directory of {sbatch_path_to_write} does not exist"
    with open(sbatch_path_to_write, "w") as f:
        f.write(sbatch_script)
    
    return


###################################################################################################################

def main():
    """main to execute the diSPIM job"""
    config_param = ArgSchemaParser(schema_type=ConfigFile)


    #scripts to run during upload
    SUBMIT_TRANSCODE_JOB =  Path(__file__).parent.parent.joinpath('src/aind_data_transfer/jobs/transcode_job.py')
    SUBMIT_ZARR_JOB =       Path(__file__).parent.parent.joinpath('src/aind_data_transfer/jobs/zarr_upload_job.py')

    config_file_path = config_param.args["config_file"]
    config = get_default_config(config_file_path)

    # print(f'config: {config}')
    root_folder = Path(config["root_folder"])
    print(f'root_folder: {root_folder}')

    (
        raw_datasets_ready,
        raw_datasets_rejected,
    ) = get_ispim_folders_with_file(
        root_folder=root_folder, search_file="derivatives/processing_manifest.json"
    )

    provide_folder_permissions(
        root_folder=root_folder, paths=raw_datasets_ready, permissions="755"
    )

    logger.warning(f"Raw datasets rejected: {raw_datasets_rejected}")

    #make processing manifest for rejected datasets if they don't already exist: 
    for rejected_dataset in raw_datasets_rejected:
        if not check_if_processing_manifest_exists(root_folder.joinpath(rejected_dataset)):
            write_processing_manifest(root_folder.joinpath(rejected_dataset))
            logger.info(f"Processing manifest written for rejected dataset: {rejected_dataset}")
    

    # new_dataset_paths, ready_datasets = organize_datasets(
    #     root_folder,
    #     raw_datasets_ready,
    #     config["metadata_service_domain"])



    processing_manifest_path = "derivatives/processing_manifest.json"

    pending_datasets_config = get_upload_datasets(
        dataset_folder=root_folder,
        config_path=processing_manifest_path,  # Pointing to this folder due to data conventions
        info_manager_path=config["info_manager_path"],
    )

    logger.info(f"Uploading {pending_datasets_config}")

    sbatch_file_path = Path(__file__).parent.joinpath('bin/zarr_upload_sbatch.sh')

    tile_qc_sbatch_filepath = Path('/allen/aind/scratch/diSPIM_QC/hcr_review_tiles/bin/plot_tile_qc_sbatch.sh')


    print(f'pending_datasets_config: {pending_datasets_config}')
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

        write_tile_qc_sbatch_script(dataset_name, tile_qc_sbatch_filepath)

        if os.path.isdir(dataset_path):
            # dataset_dest_path = dest_data_dir.joinpath(dataset_name)

            if config["transfer_type"]["type"] == "HPC":
                # dataset_dumped = json.dumps(dataset).replace('"', "[token]")
                print(f'Running sbatch_file_path: {sbatch_file_path}')
                cmd = f"""sbatch {sbatch_file_path.as_posix()}"""


                # Setting dataset_status as 'uploading'
                pre_upload_smartspim(dataset_path, processing_manifest_path)

                # HPC run
                logger.info(f"Uploading dataset: {dataset_name}")
                for out in file_utils.execute_command(cmd):
                    logger.info(out) 

                # Wait for 5 minutes before running tile qc (so metadata can be generated)
                time.sleep(60*5)

                cmd_tile_qc = f"""sbatch {tile_qc_sbatch_filepath.as_posix()}"""
                for out in file_utils.execute_command(cmd_tile_qc):
                    logger.info(out)

                time.sleep(10)

            else:
                # Local
                raise NotImplementedError
        else:
            logger.warning(f"Path {dataset_path} does not exist. Ignoring...")

    pending_datasets = get_upload_datasets(
        dataset_folder=root_folder,
        config_path=processing_manifest_path,  # Pointing to this folder due to data conventions
        info_manager_path=config["info_manager_path"],
    )

if __name__ == "__main__":
    main()

