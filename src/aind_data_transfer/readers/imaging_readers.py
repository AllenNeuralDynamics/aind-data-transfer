"""This module contains the api to retrieve a reader for imaging data.
"""
import os
import re
from enum import Enum
from pathlib import Path
from typing import List, Union

PathLike = Union[str, Path]


class ImagingReaders:
    """This class contains the methods to retrieve a reader for aind imaging data."""

    class Readers(Enum):
        """Enum for readers."""

        dispim = "diSPIM"
        exaspim = "exaSPIM"
        mesospim = "mesoSPIM"
        generic = "micr"

    readers = [member.value for member in Readers]

    class SourceRegexPatterns(Enum):
        """Enum for regex patterns the source folder name should match"""

        exaspim_acquisition = r"exaSPIM_([-A-Z0-9.]+)_(\d{4})-(\d{2})-(\d{2})_(\d{2})-(\d{2})-(\d{2})"
        mesospim_acquisition = r"mesoSPIM_([-A-Z0-9.]+)_(\d{4})-(\d{2})-(\d{2})_(\d{2})-(\d{2})-(\d{2})"
        dispim_acquisition = r"HCR_([-A-Z0-9.]+)_(\d{4})-(\d{2})-(\d{2})_(\d{2})-(\d{2})-(\d{2})"

    @staticmethod
    def get_raw_data_dir(reader_name, input_dir):
        if reader_name not in ImagingReaders.readers:
            raise Exception(
                f"Unknown readers: {reader_name}. "
                f"Please select one of {ImagingReaders.readers}"
            )
        raw_data_dir = Path(input_dir) / reader_name
        if not raw_data_dir.is_dir():
            raise FileNotFoundError(
                f"Raw data directory not found: {raw_data_dir}"
            )
        return raw_data_dir

    @staticmethod
    def get_reader_name(input_dir):
        # re.search does not work with Path objects
        input_dir = str(input_dir)
        if (
            re.search(ImagingReaders.Readers.exaspim.value, input_dir)
            is not None
        ):
            return ImagingReaders.Readers.exaspim.value
        elif (
            re.search(ImagingReaders.Readers.mesospim.value, input_dir)
            is not None
        ):
            return ImagingReaders.Readers.mesospim.value
        elif (
            re.search(ImagingReaders.Readers.dispim.value, input_dir)
            is not None
        ):
            return ImagingReaders.Readers.dispim.value
        else:
            return ImagingReaders.Readers.generic.value


    @staticmethod
    def read_dispim_folders(path: PathLike) -> List[str]:
        """
        Reads diSPIM datasets in a folder
        based on data conventions

        Parameters
        -----------------
        path: PathLike
            Path where the datasets are located

        Returns
        -----------------
        List[str]
            List with the found diSPIM datasets
        """
        dispim_datasets = []

        if os.path.isdir(path):
            datasets = os.listdir(path)

            for dataset in datasets:
                if re.match(
                    ImagingReaders.SourceRegexPatterns.dispim_acquisition.value,
                    dataset,
                ):
                    dispim_datasets.append(dataset)

        else:
            raise ValueError(f"Path {path} is not a folder.")

        return dispim_datasets


    @staticmethod
    def read_exaspim_folders(path: PathLike) -> List[str]:
        """
        Reads exaSPIM datasets in a folder
        based on data conventions

        Parameters
        -----------------
        path: PathLike
            Path where the datasets are located

        Returns
        -----------------
        List[str]
            List with the found exaSPIM datasets
        """
        exaspim_datasets = []

        if os.path.isdir(path):
            datasets = os.listdir(path)

            for dataset in datasets:
                if re.match(
                    ImagingReaders.SourceRegexPatterns.exaspim_acquisition.value,
                    dataset,
                ):
                    exaspim_datasets.append(dataset)

        else:
            raise ValueError(f"Path {path} is not a folder.")

        return exaspim_datasets



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
        raw_smartspim_regex = r"((20[0-9]{2}[0-9][0-9]{1}[0-9][0-9]{1})_(\d{2}_\d{2}_\d{2})_(\d+|[a-zA-Z]*\d+)$)"

        # Regex expressions for inner folders inside root
        regex_channels = r"Ex_(\d{3})_Em_(\d{3})$"
        regex_channels_MIP = r"Ex_(\d{3})_Em_(\d{3}_MIP)$"
        regex_files = r'[^"]*.(txt|ini|json)$'

    @staticmethod
    def read_raw_smartspim_folders(path: PathLike) -> List[str]:
        """
        Reads raw smartspim datasets in a folder

        Parameters
        -----------------
        path: PathLike
            Path where the datasets are located

        Returns
        -----------------
        List[str]
            List with the found raw smartspim datasets
        """
        smartspim_datasets = []

        if os.path.isdir(path):
            datasets = os.listdir(path)

            for dataset in datasets:
                if re.match(
                    SmartSPIMReader.RegexPatterns.raw_smartspim_regex.value,
                    dataset,
                ):
                    smartspim_datasets.append(dataset)

        else:
            raise ValueError(f"Path {path} is not a folder.")

        return smartspim_datasets

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
                if re.match(
                    SmartSPIMReader.RegexPatterns.smartspim_regex.value,
                    dataset,
                ):
                    smartspim_datasets.append(dataset)

        else:
            raise ValueError(f"Path {path} is not a folder.")

        return smartspim_datasets
