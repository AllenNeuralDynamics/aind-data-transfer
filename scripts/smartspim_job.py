import logging
import os
import sys
import time
import warnings
from datetime import datetime
from pathlib import Path
from typing import Union

import s3_upload
import yaml
from argschema import ArgSchema, ArgSchemaParser
from argschema.fields import Dict, InputFile, Int, List, Str

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

    transfer_type = Str(
        required=True,
        metadata={
            "description": 'Where the processes will be run. ["HPC", "LOCAL"]'
        },
        dump_default="LOCAL",
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


def organize_datasets(datasets: list) -> None:
    """
    This function organizes the smartspim
    datasets

    Parameters
    ------------------------
    datasets: List
        List with the smartspim dataset
        paths.

    """

    if datasets:
        # Organizing smartspim folders
        smartSPIM_writer = SmartSPIMWriter(dataset_paths=datasets)

        (
            new_dataset_paths,
            ignored_datasets,
        ) = smartSPIM_writer.prepare_datasets(mode="move")

    return new_dataset_paths


def main():
    """
    Main to execute the smartspim job
    """
    config_param = ArgSchemaParser(schema_type=ConfigFile)

    SUBMIT_HPC_PATH = "/home/camilo.laiton/repositories/aind-data-transfer/scripts/cluster/submit.py"
    S3_UPLOAD_PATH = "/home/camilo.laiton/repositories/aind-data-transfer/scripts/s3_upload.py"

    config_file_path = config_param.args["config_file"]
    config = get_default_config(config_file_path)

    new_dataset_paths = organize_datasets(
        config["organize_smartspim_datasets"]
    )

    pending_datasets = get_upload_datasets(Path(config["root_folder"]))

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

    for dataset_path in pending_datasets:

        dataset_path = Path(dataset_path)
        dataset_name = dataset_path.stem

        if os.path.isdir(dataset_path):

            dataset_dest_path = dest_data_dir.joinpath(dataset_name)

            if config["transfer_type"] == "HPC":

                cmd = f'python {SUBMIT_HPC_PATH} generate-and-launch-run --job_cmd="python {S3_UPLOAD_PATH} --input={dataset_path} --bucket={s3_bucket} --s3_path={dataset_name} --recursive --cluster --trigger_code_ocean --capsule_id={co_capsule_id}" --run_parent_dir="/home/camilo.laiton/.slurm" --conda_activate="/home/camilo.laiton/anaconda3/bin/activate" --conda_env="data_transfer" --queue="aind" --ntasks_per_node=4 --nodes=4 --cpus_per_task=2 --mem_per_cpu=4000 --walltime="05:00:00" --mail_user="camilo.laiton@alleninstitute.org"'
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

    pending_datasets = get_upload_datasets(Path(config["root_folder"]))


if __name__ == "__main__":
    main()
