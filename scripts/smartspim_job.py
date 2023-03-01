import json
import logging
import os
import sys
import time
import warnings
from datetime import datetime
from pathlib import Path
from typing import Tuple, Union

import s3_upload
import yaml
from argschema import ArgSchema, ArgSchemaParser
from argschema.fields import Dict, InputFile, Int, List, Str
from validate_datasets import validate_dataset

from aind_data_transfer.readers.imaging_readers import SmartSPIMReader
from aind_data_transfer.util import file_utils
from aind_data_transfer.writers.imaging_writers import SmartSPIMWriter

warnings.filterwarnings("ignore")

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s : %(message)s",
    datefmt="%Y-%m-%d %H:%M",
    handlers=[
        logging.StreamHandler(),
        # logging.FileHandler("test.log", "a"),
    ],
)
logging.disable("DEBUG")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

PathLike = Union[str, Path]


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

    organize_smartspim_datasets = List(
        Dict(),
        required=True,
        metadata={
            "description": "Datasets that will be organized and uploaded"
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


class ConfigFile(ArgSchema):
    """YAML configuration file"""

    config_file = InputFile(
        required=True,
        metadata={"description": "Path to the YAML config file."},
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


def save_string_to_txt(txt: str, filepath: PathLike, mode="w") -> None:
    """
    Saves a text in a file in the given mode.

    Parameters
    ------------------------
    txt: str
        String to be saved.

    filepath: PathLike
        Path where the file is located or will be saved.

    mode: str
        File open mode.

    """

    with open(filepath, mode) as file:
        file.write(txt + "\n")


def write_dict_to_yaml(dictionary: dict, filename: PathLike) -> None:
    """
    Writes a dictionary to a YAML file.

    Parameters
    ------------------------
    dictionary: dict
        Dictionary with the data to be written.

    filename: PathLike
        Path where the YAML will be saved
    """

    with open(filename, "w") as outfile:
        yaml.dump(dictionary, outfile, default_flow_style=False)


# Check dataset status
STATUS = ["PENDING", "UPLOADED"]
STATUS_FILENAME = "DATASET_STATUS.txt"


def get_upload_datasets(dataset_folder: PathLike) -> list:
    """
    This function gets the datasets and
    classifies them in pending, uploaded
    and warning.

    - Pending: Datasets that need to be uploaded
    - Uploaded: Datasets that are in the cloud
    - Warning: Datasets that are not ready to be
    uploaded

    Parameters
    ------------------------
    dataset_folder: PathLike
        Path where the smartspim datasets are located.

    Returns
    ------------------------
    List
        List with the pending datasets
    """

    pending_datasets = []
    uploaded_datasets = []
    warning_datasets = []

    dataset_folder = Path(dataset_folder)
    # Get smartspim datasets
    smartspim_datasets = SmartSPIMReader.read_smartspim_folders(dataset_folder)

    # Checking status
    for dataset in smartspim_datasets:
        dataset_path = dataset_folder.joinpath(dataset)

        file_content = file_utils.get_status_filename_data(dataset_path)
        dataset_path = str(dataset_path)

        if not len(file_content):
            warning_datasets.append(dataset_path)
            continue

        if isinstance(file_content, list):
            if "PENDING" in file_content:
                # Upload dataset
                pending_datasets.append(dataset_path)

            elif "UPLOADED" in file_content:
                # Ignore dataset
                uploaded_datasets.append(dataset_path)
            else:
                # Dataset does not have a tag
                warning_datasets.append(dataset_path)

    info_manager = {
        "generated_date": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
        "uploaded": uploaded_datasets,
        "pending": pending_datasets,
        "warning": warning_datasets,
    }

    info_manager_path = dataset_folder.joinpath("info_manager.yaml")
    write_dict_to_yaml(info_manager, info_manager_path)

    return pending_datasets


def organize_datasets(
    root_folder: PathLike, datasets: list, metadata_service: str
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

    if datasets:
        # Looking for DATASET_STATUS.txt
        ready_datasets = []
        for dataset in datasets:
            dataset_path = Path(root_folder).joinpath(dataset["path"])

            if not os.path.isdir(dataset_path):
                continue

            if validate_dataset(dataset_path):
                file_content = file_utils.get_status_filename_data(
                    dataset_path
                )

                if not len(file_content):
                    logging.error(
                        f"Ignoring dataset {dataset_path}, it's not ready"
                    )
                    dataset_config = dataset.copy()
                    dataset_config["path"] = dataset_path
                    ready_datasets.append(dataset_config)

            else:
                logger.info(
                    f"Dataset f{dataset['path']} has problems in tiles!"
                )

        # Organizing smartspim folders
        smartSPIM_writer = SmartSPIMWriter(
            dataset_paths=ready_datasets, metadata_domain=metadata_service,
        )

        (
            new_dataset_paths,
            ignored_datasets,
        ) = smartSPIM_writer.prepare_datasets(mode="move")

    return new_dataset_paths, ready_datasets


def get_pipeline_config(
    dataset_config: dict,
    pipeline_steps: list = ["stitching", "registration", "segmentation"],
) -> dict:
    """
    Returns the pipeline configuration

    Parameters
    ------------
    dataset_config: dict
        Configuration of a SmartSPIM dataset

    pipeline_steps: List[str]
        List with the steps that will be
        performed in the pipeline

    Returns
    ---------
    dict
        Dictionary with the configuration
        of that smartspim dataset to be
        executed
    """
    pipeline_config = {}

    for pipeline_step in pipeline_steps:
        if pipeline_step in dataset_config:
            pipeline_config[pipeline_step] = dataset_config[pipeline_step]

    return pipeline_config


def get_smartspim_default_config() -> dict:
    """
    Returns the default config
    for the smartspim pipeline
    """
    return {
        "stitching": {"co_folder": "scratch", "stitch_channel": "0"},
        "registration": {"channel": "Ex_488_Em_525.zarr", "input_scale": "3"},
        # 'segmentation': {
        #     'channel': 'Ex_488_Em_525',
        #     'input_scale': '0',
        #     'chunksize': '500',
        # },
    }


def set_dataset_config(
    pending_datasets: list,
    new_dataset_paths: list,
    old_dataset_paths: list,
    default_pipeline_config: dict,
) -> list:
    """
    Returns a dictionary with the
    pipeline steps per dataset

    Parameters
    ------------
    pending_datasets: List[str]
        List with the paths to the
        pending datasets

    new_dataset_paths: List[str]
        List with the new paths of
        the old_dataset_paths in the
        same order

    old_dataset_paths: List[dict]
        List with Dicitonaries with the original
        paths and configuration for the
        datasets

    default_pipeline_config: dict
        Default configuration for the
        smartspim datasets that are not
        found in the yaml file

    Returns
    -----------
    List[dict]
        List with dictionaries with the configuration
        for the smartspim datasets
    """

    smartspim_datasets_configs = []

    # Setting the configuration for the datasets
    # that are in the YAML
    for new_name_idx in range(len(new_dataset_paths)):
        config = {**old_dataset_paths[new_name_idx]}

        config["path"] = str(new_dataset_paths[new_name_idx])

        smartspim_datasets_configs.append(config)

    pending_datasets = set(pending_datasets)
    new_dataset_paths = set(new_dataset_paths)

    # Datasets with no YAML config
    no_yaml_datasets = pending_datasets.symmetric_difference(new_dataset_paths)

    for no_yaml_dataset in no_yaml_datasets:
        smartspim_datasets_configs.append(
            {"path": no_yaml_dataset, **default_pipeline_config}
        )

    return smartspim_datasets_configs


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

    os.environ["CODEOCEAN_CREDENTIALS_PATH"] = config[
        "codeocean_credentials_path"
    ]

    # Organizing folders
    root_folder = Path(config["root_folder"])
    new_dataset_paths, ready_datasets = organize_datasets(
        root_folder,
        config["organize_smartspim_datasets"],
        config["metadata_service_domain"],
    )

    # Getting all pending datasets in root folder (even old ones)
    pending_datasets = get_upload_datasets(root_folder)

    # Providing parameters to datasets
    default_pipeline_config = get_smartspim_default_config()

    pending_datasets = set_dataset_config(
        pending_datasets,
        new_dataset_paths,
        ready_datasets,
        default_pipeline_config,
    )

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

    logger.info(f"Uploading {pending_datasets}")

    for dataset in pending_datasets:
        dataset["co_capsule_id"] = co_capsule_id
        dataset_path = Path(root_folder).joinpath(dataset["path"])
        dataset_name = dataset_path.stem

        if os.path.isdir(dataset_path):
            dataset_dest_path = dest_data_dir.joinpath(dataset_name)

            if config["transfer_type"]["type"] == "HPC":
                dataset_dumped = json.dumps(dataset).replace('"', "[token]")
                cmd = f"""python {SUBMIT_HPC_PATH} generate-and-launch-run --job_cmd='python {S3_UPLOAD_PATH} \
                --input={dataset_path} --bucket={s3_bucket} --s3_path={dataset_name} --recursive --cluster \
                --trigger_code_ocean --pipeline_config="{dataset_dumped}" ' \
                --run_parent_dir='{config['transfer_type']['logs_folder']}' \
                --conda_activate='/home/{config['transfer_type']['hpc_account']}/anaconda3/bin/activate' \
                --conda_env='{config['transfer_type']['conda_env']}' --queue='{config['transfer_type']['hpc_queue']}' \
                --ntasks_per_node={config['transfer_type']['tasks_per_node']} \
                --nodes={config['transfer_type']['nodes']}  --cpus_per_task={config['transfer_type']['cpus_per_task']} \
                --mem_per_cpu={config['transfer_type']['mem_per_cpu']} --walltime='{config['transfer_type']['walltime']}' \
                --mail_user='{config['transfer_type']['mail_user']}' """

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
                file_utils.write_list_to_txt(
                    str(dataset_path.joinpath(STATUS_FILENAME)),
                    [
                        "UPLOADED",
                        f"Upload time: {now_datetime}",
                        f"Bucket: {s3_bucket}",
                    ],
                )

        else:
            logger.warning(f"Path {dataset_path} does not exist. Ignoring...")

    pending_datasets = get_upload_datasets(root_folder)


if __name__ == "__main__":
    main()
