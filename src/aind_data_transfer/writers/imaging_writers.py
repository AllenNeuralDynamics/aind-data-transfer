import logging
import os
import re
from datetime import date, datetime, time
from pathlib import Path
from typing import Tuple, Union

from aind_data_schema import Funding, Procedures, RawDataDescription, Subject
from aind_data_schema.procedures import (
    Anaesthetic,
    Craniotomy,
    InjectionMaterial,
    NanojectInjection,
)
from aind_metadata_service.client import AindMetadataServiceClient

from aind_data_transfer.readers.imaging_readers import SmartSPIMReader
from aind_data_transfer.util import file_utils

PathLike = Union[str, Path]

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


class SmartSPIMWriter:
    """This class contains the methods to write smartspim data."""

    def __init__(self, dataset_paths: dict, metadata_domain: str):
        """
        Class constructor.

        Parameters
        ------------------------
        dataset_paths: dict
            Dictionary with the dataset paths

        metadata_domain: str
            Metadata domain

        """
        self.__dataset_paths = dataset_paths
        self.__regex_expressions = SmartSPIMReader.RegexPatterns
        self.__metadata_domain = metadata_domain

    @property
    def dataset_paths(self) -> dict:
        """
        Getter of dataset paths.

        Returns
        ------------------------
        dict
            Dictionary with the dataset paths

        """
        return self.__dataset_paths

    @dataset_paths.setter
    def dataset_paths(self, new_dataset_paths: dict) -> None:
        """
        Setter of dataset paths.

        Parameters
        ------------------------
        new_dataset_paths: dict
            Dictionary with the dataset paths

        """
        self.__dataset_paths = new_dataset_paths

    def create_dataset_convention(self, dataset_path: PathLike) -> Tuple:
        """
        Creates the dataset name based on the data convention.

        Parameters
        ------------------------
        dataset_path: PathLike
            Path where the dataset is located

        Returns
        ------------------------
        Tuple
            Tuple with the new dataset path based on
            the data conventions and mouse id
        """

        dataset_path = Path(dataset_path)

        dataset_name = dataset_path.stem

        try:
            date_str = re.search(
                self.__regex_expressions.capture_date_regex.value, dataset_name
            ).group(1)
            time_str = re.search(
                self.__regex_expressions.capture_time_regex.value, dataset_name
            ).group(1)[1:-1]
            mouse_id_str = re.search(
                self.__regex_expressions.capture_mouse_id.value, dataset_name
            ).group(1)[1:]
        except ValueError as err:
            raise ValueError(
                "It was not possible to capture date, time or mouse_id.", err
            )

        date_time_obj = datetime.strptime(
            date_str + time_str, "%Y%m%d%H_%M_%S"
        )

        date_fmt = "%Y-%m-%d"
        date_str = date_time_obj.strftime(date_fmt)

        time_fmt = "%H-%M-%S"
        time_str = date_time_obj.strftime(time_fmt)

        new_dataset_path = dataset_path.parent.joinpath(
            f"SmartSPIM_{mouse_id_str}_{date_str}_{time_str}"
        )

        return new_dataset_path, mouse_id_str

    def __create_data_description(
        self, mouse_id: str, dataset_info: dict, output_path: PathLike
    ):
        """
        Creates the data description json.

        Parameters
        ------------------------
        mouse_id: str
            Mouse id for the dataset

        dataset_info: dict
            Information for the dataset

        output_path: PathLike
            Path where the dataset is located

        """

        today = datetime.today()

        # Creating data description
        data_description = RawDataDescription(
            modality="smartspim",
            subject_id=mouse_id,
            creation_date=date(today.year, today.month, today.day),
            creation_time=time(today.hour, today.minute, today.second),
            institution=dataset_info["institution"],
            group="MSMA",
            project_name=dataset_info["project"],
            project_id=dataset_info["project_id"],
            funding_source=[Funding(funder=dataset_info["institution"])],
        )

        data_description_path = str(
            output_path.joinpath("data_description.json")
        )

        with open(data_description_path, "w") as f:
            f.write(data_description.json(indent=3))

    def __create_subject(self, mouse_id: str, output_path: PathLike):
        """
        Creates the data description json.

        Parameters
        ------------------------
        mouse_id: str
            Mouse id for the dataset

        output_path: PathLike
            Path where the dataset is located
        """

        client = AindMetadataServiceClient(self.__metadata_domain)

        response = client.get_subject(mouse_id)

        if response.status_code == 200:
            data = response.json()["data"]

            model = Subject(
                species=data["species"],
                subject_id=data["subject_id"],
                sex=data["sex"],
                date_of_birth=data["date_of_birth"],
                genotype=data["genotype"],
                mgi_allele_ids=data["mgi_allele_ids"],
                background_strain=data["background_strain"],
                source=data["source"],
                rrid=data["rrid"],
                restrictions=data["restrictions"],
                breeding_group=data["breeding_group"],
                maternal_id=data["maternal_id"],
                maternal_genotype=data["maternal_genotype"],
                paternal_id=data["paternal_id"],
                paternal_genotype=data["paternal_genotype"],
                light_cycle=data["light_cycle"],
                home_cage_enrichment=data["home_cage_enrichment"],
                wellness_reports=data["wellness_reports"],
                notes=data["notes"],
            )

            subject_path = str(output_path.joinpath("subject.json"))

            with open(subject_path, "w") as f:
                f.write(subject.json(indent=3))

        else:
            logger.error(
                f"Mouse {mouse_id} does not have subject information - res status: {res.status_code}"
            )

    def __create_adquisition(self, mouse_id: str, output_path: PathLike):
        """
        Creates the data description json.

        Parameters
        ------------------------
        mouse_id: str
            Mouse id for the dataset

        output_path: PathLike
            Path where the dataset is located

        """
        pass

    def __create_smartspim_metadata(
        self, mouse_id: str, dataset_info: dict, output_path: PathLike
    ) -> None:
        """
        Creates the data description json.

        Parameters
        ------------------------
        mouse_id: str
            Mouse id for the dataset

        dataset_info: dict
            Information for the dataset

        dataset_path: PathLike
            Path where the dataset is located
        """

        output_path = Path(output_path)

        # Creates the data description json
        self.__create_data_description(mouse_id, dataset_info, output_path)

        # Creates the subject metadata json
        self.__create_subject(mouse_id, output_path)

        # Creates the procedures json
        self.__create_procedures(mouse_id, output_path)

    def prepare_datasets(
        self, mode: str = "move", delete_empty: bool = True
    ) -> Tuple:
        """
        Prepares the smartspim folder structure
        based on the data conventions.

        Parameters
        ------------------------
        mode: str
            Preparation mode. Move to move data
            and copy to copy it.

        delete_empty: bool
            Deletes the original folder of the data
            if it's empty.

        Returns
        ------------------------
        Tuple
            Tuple with the new dataset paths and
            the ignored datasets
        """

        new_dataset_paths = []
        ignored_datasets = []

        for dataset_info in self.__dataset_paths:
            dataset_path = dataset_info["path"]

            if os.path.isdir(dataset_path):
                logger.info(f"Processing: {dataset_path}\n")
                (
                    new_dataset_path,
                    mouse_id_str,
                ) = self.create_dataset_convention(dataset_path)
                derivatives_path = new_dataset_path.joinpath("derivatives")
                smartspim_channels_path = new_dataset_path.joinpath(
                    "SmartSPIM"
                )

                if not os.path.isdir(new_dataset_path):
                    file_utils.create_folder(derivatives_path, True)
                    file_utils.create_folder(smartspim_channels_path, True)

                    # Temporary while we are able to find a way to get all metadata from datasets

                    # Create smartspim metadata
                    self.__create_smartspim_metadata(
                        mouse_id=mouse_id_str,
                        dataset_info=dataset_info,
                        output_path=new_dataset_path,
                    )

                    # Moving channels
                    file_utils.move_folders_or_files(
                        dataset_path,
                        new_dataset_path.joinpath("SmartSPIM"),
                        self.__regex_expressions.regex_channels.value,
                        mode=mode,
                    )

                    # Moving maximal intensity projections per channel
                    file_utils.move_folders_or_files(
                        dataset_path,
                        new_dataset_path.joinpath("derivatives"),
                        self.__regex_expressions.regex_channels_MIP.value,
                        mode=mode,
                    )

                    # Moving metadata files
                    file_utils.move_folders_or_files(
                        dataset_path,
                        new_dataset_path.joinpath("derivatives"),
                        self.__regex_expressions.regex_files.value,
                        mode=mode,
                    )

                    file_utils.write_list_to_txt(
                        new_dataset_path.joinpath("DATASET_STATUS.txt"),
                        ["PENDING"],
                    )

                    new_dataset_paths.append(new_dataset_path)

                else:
                    ignored_datasets.append(new_dataset_path)
                    logger.warning(
                        f"[!] {new_dataset_path} already exists, please check it. Ignoring..."
                    )

                # We delete the folder if it does not have any files
                elements_in_dataset = os.listdir(dataset_path)

                if delete_empty and not len(elements_in_dataset):
                    # Delete only if folder is empty
                    file_utils.delete_folder(dataset_path)

            else:
                logger.warning(
                    f"[!] Path not found for dataset located in: {dataset_path}. Ignoring..."
                )

        return new_dataset_paths, ignored_datasets
