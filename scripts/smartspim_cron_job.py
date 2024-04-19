"""
Script that uploads smartspim datasets. It is designed
to be automatic by using a processing_manifest.json
inside of each dataset and a YAML configuration file
located in the conf/ folder.
"""
import glob
import json
import logging
import os
import sys
import time
import warnings
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple, Union

import s3_upload
import yaml
from argschema import ArgSchema, ArgSchemaParser
from argschema.fields import Dict, InputFile, Int, List, Str
from validate_datasets import validate_dataset

from aind_data_transfer.readers.imaging_readers import SmartSPIMReader
from aind_data_transfer.util import file_utils
from aind_data_transfer.writers.imaging_writers import SmartSPIMWriter

warnings.filterwarnings("ignore")

CURR_DATE = datetime.now().strftime("%Y-%m-%d")

# Printing logs in .cronjob folder in my root folder structure
file_path = Path(
    os.path.dirname(os.path.realpath(__file__))
).parent.parent.absolute()

LOGS_FOLDER = f"{file_path}/.cronjob/logs_{CURR_DATE}"
CURR_DATE_TIME = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

# Creates logs folder, if it does not exist
file_utils.create_folder(LOGS_FOLDER)

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s : %(message)s",
    datefmt="%Y-%m-%d %H:%M",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            f"{LOGS_FOLDER}/smartspim_logs_{CURR_DATE_TIME}.log", "a"
        ),
    ],
    force=True,
)
logging.disable("DEBUG")
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

PathLike = Union[str, Path]


class ConfigFile(ArgSchema):
    """YAML configuration file"""

    config_file = InputFile(
        required=True,
        metadata={"description": "Path to the YAML config file."},
    )


class CopyDatasets(ArgSchema):
    """
    Argschema parameters to copy smartspim
    datasets
    """

    transfer_type = Dict(
        required=True,
        metadata={"description": "Configuration for the transfer type"},
    )

    metadata_service_domain = Str(
        required=True,
        metadata={"description": "Metadata service domain"},
        dump_default="",
    )

    codeocean_credentials_path = Str(
        required=True,
        metadata={
            "description": "Path where the code ocean credentials are saved"
        },
        dump_default="",
    )

    co_capsule_id = Str(
        required=True,
        metadata={"description": "Capsule id of smartspim pipeline"},
    )

    root_folder = Str(
        required=True,
        metadata={
            "description": "Folder where smartspim datasets are located"
        },
    )

    dest_data_dir = Str(
        required=False,
        metadata={
            "description": "Relative path to bucket where the data will be stored"
        },
        dump_default="",
    )

    s3_bucket = Str(
        required=True,
        metadata={"description": "S3 bucket where the data will be stored"},
    )

    nthreads = Int(
        required=True,
        metadata={"description": "Number of threads to upload data"},
        dump_default=1,
    )

    exiftool_path = Str(
        required=True,
        metadata={"description": "Path to exiftool"},
    )

    info_manager_path = Str(
        required=False,
        metadata={
            "description": "Path to output status of smartspim datasets"
        },
        dump_default=None,
    )


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

    filename = Path(os.path.dirname(__file__)).joinpath(filename)

    config = None
    try:
        with open(filename, "r") as stream:
            config = yaml.safe_load(stream)
    except Exception as error:
        raise error

    return config


def get_smartspim_folders_with_file(
    root_folder: PathLike, search_file: str
) -> Tuple[List]:
    """
    Get the smartspim folder that
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
    raw_smartspim_datasets = SmartSPIMReader.read_raw_smartspim_folders(
        root_folder
    )

    raw_datasets_accepted = []
    raw_datasets_rejected = []

    for raw_smartspim_dataset in raw_smartspim_datasets:
        file_path = root_folder.joinpath(raw_smartspim_dataset, search_file)

        if os.path.exists(file_path):
            raw_datasets_accepted.append(raw_smartspim_dataset)

        else:
            raw_datasets_rejected.append(raw_smartspim_dataset)

    return raw_datasets_accepted, raw_datasets_rejected


def organize_datasets(
    root_folder: PathLike,
    datasets: list,
    metadata_service: str,
    config_filename: str = "processing_manifest.json",
) -> Tuple[list, list]:
    """
    This function organizes the smartspim
    datasets

    Parameters
    ------------------------
    root_folder: PathLike
        Root folder where these datasets
        are stored

    datasets: List
        List with the smartspim dataset
        paths.

    Returns
    --------------
    Tuple[List[str], List[dict]]
        Tuple that contains in the first position
        a list with the new paths for the smartspim
        datasets, and in the second position a list
        with the pipeline configuration of those
        datasets
    """
    new_dataset_paths = []
    ready_datasets = []

    if isinstance(datasets, list):
        for dataset in datasets:
            dataset_path = Path(root_folder).joinpath(dataset)

            if not os.path.isdir(dataset_path):
                logger.info(f"Dataset {dataset_path} does not exist.")
                continue

            json_path_config = dataset_path.joinpath(config_filename)
            logger.info(f"Reading {json_path_config}")

            # Reading `processing_manifest.json`
            dataset_config = file_utils.read_json_as_dict(json_path_config)

            if "dataset_status" not in dataset_config:
                logger.error(
                    f"Ignoring dataset {dataset_path}, it does not have the dataset status attribute or json does not exist."
                )

            elif dataset_config["dataset_status"].casefold() == "pending":
                logger.info(f"Validating dataset {dataset_path}")
                if validate_dataset(dataset_path):
                    dataset_config["path"] = dataset_path
                    ready_datasets.append(dataset_config)

                else:
                    logger.info(
                        f"Dataset {dataset_path} has problems in tiles!\n"
                    )
            else:
                logger.error(
                    f"Ignoring dataset {dataset_path}, it does not have the PENDING tag in dataset_status."
                )

        # Organizing smartspim folders
        smartSPIM_writer = SmartSPIMWriter(
            dataset_paths=ready_datasets,
            metadata_domain=metadata_service,
        )

        (
            new_dataset_paths,
            ignored_datasets,
        ) = smartSPIM_writer.prepare_datasets(mode="move")

    return new_dataset_paths, ready_datasets


def get_smartspim_default_config() -> dict:
    """
    Returns the default config
    for the smartspim pipeline
    """
    return {
        "stitching": {
            "co_folder": "scratch",
            "channel": "Ex_488_Em_525",
            "resolution": {"x": "1.8", "y": "1.8", "z": "2.0"},
        },
        "registration": {"channels": ["Ex_488_Em_525"], "input_scale": "3"},
        # 'segmentation': {
        #     'channels': ['Ex_488_Em_525'],
        #     'input_scale': '0',
        #     'chunksize': '500',
        # },
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

    ccf_registration_channels = file_utils.helper_validate_key_dict(
        provided_config, "ccf_registration_channels"
    )

    # Setting stitching channel
    if stitching_config is not None:
        new_config["stitching"]["channel"] = stitching_config["channel"]
        new_config["stitching"]["resolution"]["x"] = str(
            stitching_config["resolution"]["x"]
        )
        new_config["stitching"]["resolution"]["y"] = str(
            stitching_config["resolution"]["y"]
        )
        new_config["stitching"]["resolution"]["z"] = str(
            stitching_config["resolution"]["z"]
        )

    else:
        default_ch = new_config["stitching"]["channel"]
        x_res = new_config["stitching"]["resolution"]["x"]
        y_res = new_config["stitching"]["resolution"]["y"]
        z_res = new_config["stitching"]["resolution"]["z"]

        res = f"x={x_res} um, y={y_res} um and z={z_res} um"
        logger.info(
            f"Using default stitching channel {default_ch} and resolution {res}"
        )

    # Setting CCF registration
    if ccf_registration_channels is not None:
        new_config["registration"]["channels"] = ccf_registration_channels

    else:
        default_ch = new_config["registration"]["channels"]
        logger.info(f"Using default registration channel {default_ch}")

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
        Path where the smartspim datasets are located.

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
    default_pipeline_config = get_smartspim_default_config()

    # Get smartspim datasets that match data conventions
    dataset_folder = Path(dataset_folder)
    smartspim_datasets = SmartSPIMReader.read_smartspim_folders(dataset_folder)

    # Checking status
    for dataset in smartspim_datasets:
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
        else dataset_folder.joinpath("status_smartspim_datasets.yaml")
    )
    file_utils.write_dict_to_yaml(info_manager, info_manager_path)

    return pending_datasets_config


def pre_upload_smartspim(
    dataset_path: PathLike, processing_manifest_path: PathLike
):
    """
    Steps that need to be performed
    before uploading data to the cloud

    Parameters
    ----------
    dataset_path: PathLike
        Path where the dataset is located

    processing_manifest_path: PathLike
        Path where the manifest json
        is stored in the data conventions
    """

    dataset_config_path = Path(dataset_path).joinpath(processing_manifest_path)
    msg = f"uploading"

    file_utils.update_json_key(
        json_path=dataset_config_path, key="dataset_status", new_value=msg
    )
    logger.info(f"Updating state to 'uploading'")


def provide_folder_permissions(
    root_folder: PathLike, paths: list, permissions: Optional[str] = "755"
):
    """
    Provides 755 permission in the folder.
    This fixes a known bug in the VAST system
    where folder are created with 000 permissions.

    Parameters
    -----------
    root_folder: PathLike
        Root folder where the datasets are located

    paths: List[PathLike]
        List with the paths to the folder that
        the permissions will be updated

    permissions: Optional[str]
        Permission that will be given via chmod

    """

    oct_permissions = int(permissions, 8)

    def helper_update_permissions(list_folders: list):
        """
        Helper function to update permissions
        of a list of folders

        Parameters
        -----------
        list_folders: list
            List of strings with the paths to
            the folders to update
        """
        for list_folder in list_folders:
            os.chmod(list_folder, oct_permissions)

    for path in paths:
        dataset_path = f"{root_folder}/{path}"
        curr_permissions = oct(os.stat(dataset_path).st_mode)[-3:]

        if curr_permissions != permissions:
            logger.info(
                f"Updating permissions for {dataset_path} from {curr_permissions} to {permissions}"
            )
            os.chmod(dataset_path, oct_permissions)

            # Updating first level
            first_level_folders = glob.glob(f"{dataset_path}/*/")
            second_level_folders = glob.glob(f"{dataset_path}/*/*/")
            third_level_folders = glob.glob(f"{dataset_path}/*/*/*/")

            helper_update_permissions(first_level_folders)
            helper_update_permissions(second_level_folders)
            helper_update_permissions(third_level_folders)


def main():
    """
    Main to execute the smartspim job
    """
    config_param = ArgSchemaParser(schema_type=ConfigFile)

    SUBMIT_HPC_PATH = (
        Path(os.path.dirname(os.path.realpath(__file__))) / "cluster/submit.py"
    )
    S3_UPLOAD_PATH = (
        Path(os.path.dirname(os.path.realpath(__file__))) / "s3_upload.py"
    )

    # Getting config file
    config_file_path = config_param.args["config_file"]
    config = get_default_config(config_file_path)

    # Code ocean credentials
    os.environ["CODEOCEAN_CREDENTIALS_PATH"] = config[
        "codeocean_credentials_path"
    ]

    # Exiftool path for image validation
    os.environ["PATH"] = f"{config['exiftool_path']}:{os.environ['PATH']}"

    # Get raw smartspim folders with processing_manifest.json
    root_folder = Path(config["root_folder"])

    (
        raw_datasets_ready,
        raw_datasets_rejected,
    ) = get_smartspim_folders_with_file(
        root_folder=root_folder, search_file="processing_manifest.json"
    )

    provide_folder_permissions(
        root_folder=root_folder, paths=raw_datasets_ready, permissions="755"
    )

    logger.warning(f"Raw datasets rejected: {raw_datasets_rejected}")

    new_dataset_paths, ready_datasets = organize_datasets(
        root_folder,
        raw_datasets_ready,
        config["metadata_service_domain"],
    )

    processing_manifest_path = "derivatives/processing_manifest.json"
    # List of datasets with its pipeline configuration
    pending_datasets_config = get_upload_datasets(
        dataset_folder=root_folder,
        config_path=processing_manifest_path,  # Pointing to this folder due to data conventions
        info_manager_path=config["info_manager_path"],
    )

    # Uploading datasets using the HPC
    sys.argv = [""]

    if config["dest_data_dir"] == None:
        config["dest_data_dir"] = ""

    mod = ArgSchemaParser(input_data=config, schema_type=CopyDatasets)
    args = mod.args

    # Getting params
    s3_bucket = args["s3_bucket"]
    nthreads = args["nthreads"]
    co_capsule_id = args["co_capsule_id"]
    dest_data_dir = Path(args["dest_data_dir"])

    logger.info(f"Uploading {pending_datasets_config}")

    for dataset in pending_datasets_config:
        # Adding the CO capsule to the dataset config
        dataset["co_capsule_id"] = co_capsule_id

        dataset_path = dataset["path"]
        dataset_name = Path(dataset_path).stem

        if os.path.isdir(dataset_path):
            dataset_dest_path = dest_data_dir.joinpath(dataset_name)

            if config["transfer_type"]["type"] == "HPC":
                dataset_dumped = json.dumps(dataset).replace('"', "[token]")
                cmd = f"""python {SUBMIT_HPC_PATH} generate-and-launch-run --job_cmd='python {S3_UPLOAD_PATH} \
                --input={dataset_path} --bucket={s3_bucket} --s3_path={dataset_name} --recursive --cluster \
                --trigger_code_ocean --pipeline_config="{dataset_dumped}" --type_spim="smartspim"' \
                --run_parent_dir='{config['transfer_type']['logs_folder']}' \
                --conda_activate='/home/{config['transfer_type']['hpc_account']}/anaconda3/bin/activate' \
                --conda_env='{config['transfer_type']['conda_env']}' --queue='{config['transfer_type']['hpc_queue']}' \
                --ntasks_per_node={config['transfer_type']['tasks_per_node']} \
                --nodes={config['transfer_type']['nodes']}  --cpus_per_task={config['transfer_type']['cpus_per_task']} \
                --mem_per_cpu={config['transfer_type']['mem_per_cpu']} --walltime='{config['transfer_type']['walltime']}' \
                --mail_user='{config['transfer_type']['mail_user']}' """

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

                sys.argv = [
                    "",
                    f"--input={dataset_path}",
                    f"--bucket={s3_bucket}",
                    f"--s3_path={dataset_dest_path}",
                    f"--nthreads={nthreads}",
                    "--trigger_code_ocean",
                    f"--pipeline_config={dataset}",
                    "--recursive",
                ]

                # Local
                s3_upload.main()

                now_datetime = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                dataset_config_path = Path(dataset_path).joinpath(
                    processing_manifest_path
                )
                msg = f"uploaded - Upload time: {now_datetime} - Bucket: {s3_bucket}"

                file_utils.update_json_key(
                    json_path=dataset_config_path,
                    key="dataset_status",
                    new_value=msg,
                )

        else:
            logger.warning(f"Path {dataset_path} does not exist. Ignoring...")

    pending_datasets = get_upload_datasets(
        dataset_folder=root_folder,
        config_path=processing_manifest_path,  # Pointing to this folder due to data conventions
        info_manager_path=config["info_manager_path"],
    )


if __name__ == "__main__":
    main()