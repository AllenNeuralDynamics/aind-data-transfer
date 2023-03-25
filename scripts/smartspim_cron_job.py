from argschema import ArgSchema, ArgSchemaParser
import os
import yaml
from pathlib import Path
import logging
import json
from typing import Tuple, Union, List

from aind_data_transfer.readers.imaging_readers import SmartSPIMReader
from aind_data_transfer.util import file_utils

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
    root_folder:PathLike,
    search_file:str
) -> Tuple[List[str]]:
    raw_smartspim_datasets = SmartSPIMReader.read_raw_smartspim_folders(
        dataset_folder
    )
    root_folder = Path(root_folder)

    raw_datasets_accepted = []
    raw_datasets_rejected = []

    for raw_smartspim_dataset in raw_smartspim_datasets:
        
        file_path = root_folder.joinpath(
            raw_smartspim_dataset,
            search_file
        )

        if os.path.exists(file_path):
            raw_datasets_accepted.append(raw_smartspim_dataset)
        
        else:
            raw_datasets_rejected.append(raw_smartspim_dataset)
        
    return raw_datasets_accepted, raw_datasets_rejected


def organize_datasets(
    root_folder: PathLike,
    datasets: list,
    metadata_service: str,
    config_filename:str="processing_manifest.json"
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
        for dataset in datasets:
            dataset_path = Path(root_folder).joinpath(dataset)

            if not os.path.isdir(dataset_path):
                continue

            logger.info(f"Validating dataset {dataset_path}")
            
            if validate_dataset(dataset_path):

                # Reading `processing_manifest.json`
                dataset_config = file_utils.read_json_as_dict(
                    dataset_path.joinpath(config_filename)
                )

                if "dataset_status" not in dataset_config:
                    logger.error(
                        f"Ignoring dataset {dataset_path}, it does not have the dataset status attribute."
                    )
                
                elif dataset_config["dataset_status"].casefold() == "pending":
                    dataset_config["path"] = dataset_path
                    ready_datasets.append(dataset_config)
                
                else:
                    logger.error(
                        f"Ignoring dataset {dataset_path}, it does not have the PENDING tag in dataset_status."
                    )

            else:
                logger.info(
                    f"Dataset f{dataset['path']} has problems in tiles!\n"
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

def get_smartspim_default_config() -> dict:
    """
    Returns the default config
    for the smartspim pipeline
    """
    return {
        "stitching": {"co_folder": "scratch", "channel": "0"},
        "registration": {"channels": ["Ex_488_Em_525"], "input_scale": "3"},
        # 'segmentation': {
        #     'channels': ['Ex_488_Em_525'],
        #     'input_scale': '0',
        #     'chunksize': '500',
        # },
    }

def build_pipeline_config(
    provided_config:dict,
    default_config:dict
) -> dict:
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

    stitching_channel = helper_validate_key_dict(
        provided_config,
        "stitching_channel"
    )

    cell_segmentation_channels = helper_validate_key_dict(
        provided_config,
        "cell_segmentation_channels"
    )

    ccf_registration_channels = helper_validate_key_dict(
        provided_config,
        "ccf_registration_channels"
    )

    # Setting stitching channel
    if stitching_channel is not None:
        new_config["stitching"]["channel"] = stitching_channel
    
    else:
        default_ch = new_config["stitching"]["channel"]
        logger.info(f"Using default stitching channel {default_ch}")

    # Setting cell segmentation channels
    if cell_segmentation_channels is not None:
        new_config["segmentation"]["channels"] = cell_segmentation_channels

    else:
        default_ch = new_config["segmentation"]["channels"]
        logger.info(f"Using default segmentation channel {default_ch}")
    
    # Setting CCF registration
    if ccf_registration_channels is not None:
        new_config["registration"]["channels"] = ccf_registration_channels

    else:
        default_ch = new_config["registration"]["channels"]
        logger.info(f"Using default registration channel {default_ch}")
    
    return new_config

def get_upload_datasets(
    dataset_folder: PathLike,
    config_path: PathLike
) -> List[dict]:
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

    pending_datasets = []
    uploading_datasets = []
    uploaded_datasets = []
    warning_datasets = []

    pending_datasets_config = []
    default_pipeline_config = get_smartspim_default_config()

    dataset_folder = Path(dataset_folder)
    # Get smartspim datasets
    smartspim_datasets = SmartSPIMReader.read_smartspim_folders(dataset_folder)

    # Checking status
    for dataset in smartspim_datasets:
        dataset_path = dataset_folder.joinpath(dataset)

        dataset_config = file_utils.read_json_as_dict(
            dataset_path.joinpath(config_path)
        )
        dataset_path = str(dataset_path)

        dataset_status = helper_validate_key_dict(dataset_config, "dataset_status")

        if dataset_status is None:
            warning_datasets.append(dataset_path)
            continue

        # Validating status
        dataset_status = dataset_status.casefold()

        # Getting pipeline configuration to dataset
        pipeline_processing = helper_validate_key_dict(dataset_config, "pipeline_processing")

        if dataset_status == "pending" and pipeline_processing is not None:
            pending_datasets.append(dataset_path)

            pending_datasets_config.append(
                {
                    "path": dataset_path,
                    "pipeline_processing": build_pipeline_config(
                        pipeline_processing,
                        default_pipeline_config
                    )
                }
            )
        
        elif dataset_status == "uploading":
            uploading_datasets.append(dataset_path)
            
        elif dataset_status == "uploaded":
            uploaded_datasets.append(dataset_path)

        else:
            warning_datasets.append(dataset_path)

    info_manager = {
        "generated_date": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
        "uploaded": uploaded_datasets,
        "uploading": uploading_datasets,
        "pending": pending_datasets,
        "warning": warning_datasets,
    }

    info_manager_path = dataset_folder.joinpath("info_manager.yaml")
    write_dict_to_yaml(info_manager, info_manager_path)

    return pending_datasets_config

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

    raw_datasets_ready, raw_datasets_rejected = get_smartspim_folders_with_file(
        root_folder=root_folder
        search_file="processing_manifest.json"
    )

    new_dataset_paths, ready_datasets = organize_datasets(
        root_folder,
        raw_datasets_ready,
        config["metadata_service_domain"],
    )

    # List of datasets with its pipeline configuration
    pending_datasets_config = get_upload_datasets(
        dataset_folder=root_folder,
        config_path="derivatives/processing_manifest.json" # Pointing to this folder due to data conventions
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

    logger.info(f"Uploading {pending_datasets}")

    for dataset in pending_datasets_config:
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