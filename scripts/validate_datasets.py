"""
Script to validate smartspim datasets
"""

import logging
import os
import re
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import List, Union

from tqdm import tqdm

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

PathLike = Union[Path, str]

POST_PATH_NEW_CONV = "SmartSPIM"
POST_PATH_OLD_CONV = ""


class SmartSPIMReader:
    """Reader for smartspim datasets"""

    class RegexPatterns(Enum):
        """Enum for regex patterns for the smartSPIM data"""

        # regex expressions for not structured smartspim datasets
        capture_date_regex = r"(20[0-9]{2}([0-9][0-9]{1})([0-9][0-9]{1}))"
        capture_time_regex = r"(_(\d{2})_(\d{2})_(\d{2})_)"
        capture_mouse_id = r"(_(\d+|[a-zA-Z]*\d+)$)"

        # Regular expression for smartspim datasets
        smartspim_regex = r"SmartSPIM_(\d+|[a-zA-Z]*\d+)_(20\d{2}-(\d\d{1})-(\d\d{1}))_((\d{2})-(\d{2})-(\d{2}))"
        smartspim_old_regex = r"(20\d{2}(\d\d{1})(\d\d{1}))_((\d{2}))_((\d{2}))_((\d{2}))_(\d+|[a-zA-Z]*\d+)"

        # Regex expressions for inner folders inside root
        regex_channels = r"Ex_(\d{3})_Em_(\d{3})$"
        regex_channels_MIP = r"Ex_(\d{3})_Em_(\d{3}_MIP)$"
        regex_files = r'[^"]*.(txt|ini)$'

    @staticmethod
    def read_smartspim_folders(path: PathLike) -> List[str]:
        """
        Reads smartspim datasets in a folder
        based on data conventions

        Parameters
        -----------------
        path: PathLike
            Path where the datasets are located

        Returns
        -----------------
        List[str]
            List with the found smartspim datasets
        """
        smartspim_datasets = []

        if os.path.isdir(path):
            datasets = os.listdir(path)

            for dataset in datasets:

                dataconvention_match = re.match(
                    SmartSPIMReader.RegexPatterns.smartspim_regex.value,
                    dataset,
                )

                oldconvention_match = re.match(
                    SmartSPIMReader.RegexPatterns.smartspim_old_regex.value,
                    dataset,
                )

                if dataconvention_match:
                    str_path = str(Path(dataset).joinpath(POST_PATH_NEW_CONV))
                    smartspim_datasets.append(str_path)

                if oldconvention_match:
                    str_path = str(Path(dataset).joinpath(POST_PATH_OLD_CONV))
                    smartspim_datasets.append(str_path)

        else:
            raise ValueError(f"Path {path} is not a folder.")

        return smartspim_datasets


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


def read_image_directory_structure(folder_dir) -> dict:
    """
    Creates a dictionary representation of all the images
    saved by folder/col_N/row_N/images_N.[file_extention]

    Parameters
    ------------------------
    folder_dir:PathLike
        Path to the folder where the images are stored

    Returns
    ------------------------
    dict:
        Dictionary with the image representation where:
        {channel_1: ... {channel_n: {col_1: ... col_n: {row_1: ... row_n: integer with n images} } } }
    """

    directory_structure = {}
    folder_dir = Path(folder_dir)

    channel_paths = [
        folder_dir.joinpath(folder)
        for folder in os.listdir(folder_dir)
        if os.path.isdir(folder_dir.joinpath(folder))
        and re.match(
            SmartSPIMReader.RegexPatterns.regex_channels.value, folder
        )
    ]

    for channel_idx in range(len(channel_paths)):
        directory_structure[channel_paths[channel_idx]] = {}
        cols = os.listdir(channel_paths[channel_idx])
        # logger.info(f"Validating channel {channel_paths[channel_idx]}")

        for col in tqdm(cols):
            possible_col = channel_paths[channel_idx].joinpath(col)

            if os.path.isdir(possible_col):
                directory_structure[channel_paths[channel_idx]][col] = {}
                rows = os.listdir(possible_col)

                for row in rows:
                    possible_row = (
                        channel_paths[channel_idx].joinpath(col).joinpath(row)
                    )

                    if os.path.isdir(possible_row):
                        col_row_images = len(os.listdir(possible_row))
                        directory_structure[channel_paths[channel_idx]][col][
                            row
                        ] = col_row_images

    return directory_structure


def get_images_channel(channel_dict: dict) -> int:
    """
    Gets the number of images
    in a channel

    Parameters
    ------------
    channel_dict: dict
        Dictionary with the folder structure

    Returns
    -----------
    Number of images in the channel
    """
    n_images = 0

    for col_name, rows in channel_dict.items():
        for row_name, images in rows.items():
            if images == 1:
                raise ValueError(
                    f"Possible error in pos {col_name}/{row_name}"
                )

            n_images += images

    return n_images


def validate_dataset(dataset_path: PathLike) -> bool:
    """
    Validates a dataset

    Parameters
    ------------
    dataset_path: PathLike
        Path where the dataset is stored

    Returns
    -----------
    True if the dataset is correct, False otherwise
    """
    dataset_structure = read_image_directory_structure(dataset_path)

    # logger.info(f"Time reading folder structure: {end_time - start_time}")

    images_per_channel = []

    for channel_name, image_paths in dataset_structure.items():

        n_images = get_images_channel(image_paths)
        images_per_channel.append(n_images)
        logger.info(f"Channel {channel_name} has {n_images} images")

    n_channels = len(images_per_channel)

    if not n_channels:
        return False

    channel_images = images_per_channel[0]

    for images_idx in range(1, n_channels):
        if channel_images != images_per_channel[images_idx]:
            return False

    return True


def main():
    """
    Nothing fancy, but a script that check the status
    of each dataset in terms of # of tiles.
    """

    # BASE_PATH = Path("/allen/aind/stage/SmartSPIM")
    BASE_PATH = Path("/net/aind.vast01/aind/SmartSPIM_Data/")
    error_dataset_paths = "/net/aind.vast01/aind/SmartSPIM_Data/dataset_errors_SmartSPIM_DATA_folder.txt"

    search_datasets = True
    dataset_paths = []

    if search_datasets:
        dataset_paths = SmartSPIMReader.read_smartspim_folders(BASE_PATH)

    else:
        dataset_paths = [
            # Path where the channels are
            # "SmartSPIM_634571_2022-08-24_19-14-17/SmartSPIM"
            "20230220_12_16_27_642480"
        ]

    datasets_with_problems = ["Datasets with errors:\n"]
    check_paths = ["\nCheck paths:\n"]

    for dataset_path in dataset_paths:
        val_path = Path(BASE_PATH.joinpath(dataset_path))

        logger.info(f"Validating dataset: {dataset_path}")

        try:
            tile_status = validate_dataset(val_path)

        except FileNotFoundError as e:
            logger.error(
                f"[!!] Check path {val_path}. This folder MUST have the channels!"
            )
            check_paths.append(str(val_path))

        if not tile_status:
            logger.error(f"\n[+] Dataset {dataset_path} has problems in tiles")
            datasets_with_problems.append(str(dataset_path))

        else:
            logger.info(f"\n[+] Dataset {dataset_path} does not have issues")

    # Saving datasets with errors
    join_lists = datasets_with_problems + check_paths
    txt = "\n".join(join_lists)
    save_string_to_txt(txt, error_dataset_paths)


if __name__ == "__main__":
    main()
