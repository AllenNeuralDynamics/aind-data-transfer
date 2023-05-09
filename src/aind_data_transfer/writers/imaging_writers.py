"""
Imaging writers for the different data modalities
"""
import logging
import os
import re
from datetime import date, datetime, time
from pathlib import Path
from typing import Any, List, Tuple, Union

import chardet
from aind_data_schema import Funding, RawDataDescription, Subject
from aind_data_schema.data_description import (
    ExperimentType,
    Group,
    Institution,
    Modality,
)
from aind_data_schema.imaging import acquisition, tile
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


def digest_asi_line(line: str) -> datetime:
    """
    Scrape a datetime from a non-empty line, otherwise return None

    Parameters
    -----------
    line: str
        Line from the ASI file

    Returns
    -----------
    datetime
        A date that could be parsed from a string
    """

    if line.isspace():
        return None
    else:
        mdy, hms, ampm = line.split()[0:3]

    mdy = [int(i) for i in mdy.split(b"/")]
    ymd = [mdy[i] for i in [2, 0, 1]]

    hms = [int(i) for i in hms.split(b":")]
    if ampm == b"PM":
        hms[0] += 12
        if hms[0] == 24:
            hms[0] = 0

    ymdhms = ymd + hms

    dtime = datetime(*ymdhms)
    ymd = date(*ymd)
    hms = time(*hms)
    return dtime


def get_session_end(asi_file) -> datetime:
    """
    Work backward from the last line until there is a timestamp

    Parameters
    ------------
    asi_file: PathLike
        Path where the ASI metadata file is
        located

    Returns
    ------------
    Date when the session ended
    """

    with open(asi_file, "rb") as file:
        asi_mdata = file.readlines()

    idx = -1
    result = None
    while result is None:
        result = digest_asi_line(asi_mdata[idx])
        idx -= 1

    return result


def get_line_indices_metadata_file(lines: bytes) -> dict:
    """
    Get the index location in the text file
    where each block of metadata information
    starts

    Parameters
    -----------
    lines: bytes
        Bytes with the metadata information
        taken from the text file

    Returns
    -----------
    dict
        Dictionary with the indices of
        each block of metadata
    """
    indices = {
        "info_start": None,
        "wavelength_start": None,
        "tile_acquisition_start": None,
    }

    total_lines = len(lines)

    for idx_line in range(total_lines):
        splited_line = lines[idx_line].rstrip().split("\t")

        if "Obj" in splited_line:
            indices["info_start"] = idx_line

        if "Wavelength" in splited_line:
            indices["wavelength_start"] = idx_line

        if "Exposure" in splited_line:
            indices["tile_acquisition_start"] = idx_line
            break

    return indices


def get_session_config(lines: List[str]) -> dict:
    """
    Gets the session config

    Parameters
    -----------
    lines: List[str]
        List with the lines of the
        metadata file

    Returns
    -----------
    Dictionary with the parsed data
    """
    # Get objective magnification
    objective_regex = r"\d+\.?\d*"  # "(\d+\.?\d*)(x|X)" to get with X

    data = {}
    lines_splited = lines[1].rstrip().split("\t")

    # Getting first line metadata
    obj = lines_splited[0]
    v_res = float(lines_splited[1])
    um_per_pix = float(lines_splited[2])
    z_step = float(lines_splited[3])
    scanning = lines_splited[4]
    sampling = lines_splited[5]
    destripe = lines_splited[6]
    z_block = int(lines_splited[7])

    data["obj_name"] = obj
    data["obj_magnification"] = re.findall(objective_regex, obj)[0]
    data["v_res"] = v_res
    data["um/pix"] = um_per_pix
    data["z_step_um"] = z_step
    data["scanning"] = scanning
    data["sampling"] = sampling
    data["destripe"] = destripe
    data["z_block"] = z_block

    return data


def get_wavelength_config(lines: List[str], start_index: int, end_index: int):
    """
    Gets the wavelength config

    Parameters
    -----------
    lines: List[str]
        List with the lines of the
        metadata file

    start_index: int
        Index with the line where
        we'll parse the data

    end_index: int
        Index with the line where
        we'll finish

    Returns
    -----------
    Dictionary with the parsed data
    """
    wavelengths = {}
    for line in lines[start_index:end_index]:
        line_splited = line.rstrip().split("\t")

        try:
            wavelength = int(line_splited[0])
            power_l = float(line_splited[1])
            power_r = float(line_splited[2])

            wavelengths[wavelength] = {}
            wavelengths[wavelength]["power_left"] = power_l
            wavelengths[wavelength]["power_right"] = power_r

        except ValueError as err:
            logger.error(
                f"An error ocurred while parsing {line_splited} Traceback: {err}"
            )
    return wavelengths


def get_tile_info(lines: List[str], start_index: int):
    """
    Gets the tile config

    Parameters
    -----------
    lines: List[str]
        List with the lines of the
        metadata file

    start_index: int
        Index with the line where
        we'll parse the data

    Returns
    -----------
    Dictionary with the parsed data
    """

    data = {}
    ite = 0

    for line in lines[start_index:]:
        splited_line = line.rstrip().split("\t")
        line_elements = len(splited_line)

        if not line_elements or (line_elements == 1 and splited_line[0] == ""):
            # End of file when reading from bucket
            break

        name = f"t_{ite}"

        x = int(splited_line[0])
        y = int(splited_line[1])
        z = int(splited_line[2])
        wavelength = int(splited_line[3])
        side = int(splited_line[4])
        exposure = int(splited_line[5])
        skip = int(splited_line[6])

        data[name] = {
            "x": x,
            "y": y,
            "z": z,
            "wavelength": wavelength,
            "side": side,
            "exposure": exposure,
            "skip": skip,
        }
        ite += 1

    return data


def make_acq_tiles(metadata_dict: dict, filter_mapping: dict):
    """
    Makes metadata for the acquired tiles of
    the dataset

    Parameters
    -----------
    metadata_dict: dict
        Dictionary with the acquisition metadata
        coming from the microscope

    filter_mapping: dict
        Dictionary with the channel names

    Returns
    -----------
    List[tile.Translation3dTransform]
        List with the metadata for the tiles
    """

    channels = {}

    # List where the metadata of the acquired
    # tiles is stored
    tile_acquisitions = []

    filter_wheel_idx = 0
    for wavelength, value in metadata_dict["wavelength_config"].items():
        channel = tile.Channel(
            channel_name=wavelength,
            laser_wavelength=wavelength,
            laser_power=value["power_left"],
            filter_wheel_index=filter_wheel_idx,
        )
        filter_wheel_idx += 1

        channels[wavelength] = channel

    # Scale metadata
    scale = tile.Scale3dTransform(
        scale=[
            metadata_dict["session_config"]["um/pix"],  # X res
            metadata_dict["session_config"]["um/pix"],  # Y res
            metadata_dict["session_config"]["z_step_um"],  # Z res
        ]
    )

    for tile_key, tile_info in metadata_dict["tile_config"].items():
        tile_transform = tile.Translation3dTransform(
            translation=[
                tile_info["x"] / 10,
                tile_info["y"] / 10,
                tile_info["z"] / 10,
            ]
        )

        channel = channels[tile_info["wavelength"]]

        tile_acquisition = tile.AcquisitionTile(
            channel=channel,
            notes=(
                "\nLaser power is in percentage of total, it needs calibration"
            ),
            coordinate_transformations=[tile_transform, scale],
            file_name=f"Ex_{tile_info['wavelength']}_Em_{filter_mapping[tile_info['wavelength']]}/{tile_info['x']}/{tile_info['x']}_{tile_info['y']}/",
        )

        tile_acquisitions.append(tile_acquisition)

    return tile_acquisitions


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

        parsed_data = {"mouse_id": mouse_id_str, "mouse_date": date_time_obj}

        return new_dataset_path, parsed_data

    def __create_data_description(
        self, parsed_data: dict, dataset_info: dict, output_path: PathLike
    ):
        """
        Creates the data description json.

        Parameters
        ------------------------
        parsed_data: dict
            Parsed data for the dataset. Contains
            mouse id and creation datetime

        dataset_info: dict
            Information for the dataset

        output_path: PathLike
            Path where the dataset is located

        """

        mouse_date = parsed_data["mouse_date"]

        # Validating data_description args
        institution = None

        # Getting the institution
        if "institution" not in dataset_info:
            raise ValueError("Please, provide the institution in the manifest")
        else:
            institution = dataset_info["institution"]

        funding_source = file_utils.helper_validate_key_dict(
            dictionary=dataset_info,
            key="funding_source",
            default_return=institution,
        )
        project_name = file_utils.helper_validate_key_dict(
            dictionary=dataset_info, key="project"
        )
        project_id = file_utils.helper_validate_key_dict(
            dictionary=dataset_info, key="project_id"
        )
        group = file_utils.helper_validate_key_dict(
            dictionary=dataset_info, key="group", default_return="MSMA"
        )

        # Creating data description
        data_description = RawDataDescription(
            modality=[Modality.SPIM],
            subject_id=parsed_data["mouse_id"],
            creation_date=date(
                mouse_date.year, mouse_date.month, mouse_date.day
            ),
            creation_time=time(
                mouse_date.hour, mouse_date.minute, mouse_date.second
            ),
            institution=institution,
            group=group,
            project_name=project_name,
            project_id=project_id,
            funding_source=[Funding(funder=funding_source)],
            experiment_type=ExperimentType.SMARTSPIM,
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

            subject = Subject(
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
                wellness_reports=data["wellness_reports"],
                notes=data["notes"],
            )

            subject_path = str(output_path.joinpath("subject.json"))

            with open(subject_path, "w") as f:
                f.write(subject.json(indent=3))

        else:
            logger.error(
                f"Mouse {mouse_id} does not have subject information - res status: {response.status_code}"
            )

    def __get_excitation_emission_waves(self, dataset_path: PathLike) -> dict:
        """
        Gets the excitation and emission waves for
        the existing channels within a dataset

        Parameters
        ------------
        dataset_path: PathLike
            Path where the channels of the dataset
            are stored

        Returns
        ------------
        dict
            Dictionary with the excitation
            and emission waves
        """
        regex_channels = self.__regex_expressions.regex_channels.value
        excitation_emission_channels = {}

        elements = [
            element
            for element in os.listdir(dataset_path)
            if re.match(regex_channels, element)
        ]

        for channel in elements:
            channel = channel.replace("Em_", "").replace("Ex_", "")
            splitted = channel.split("_")
            excitation_emission_channels[int(splitted[0])] = int(splitted[1])

        return excitation_emission_channels

    def __create_acquisition(
        self,
        parsed_data: dict,
        dataset_info: dict,
        original_dataset_path: PathLike,
        output_path: PathLike,
    ):
        """
        Creates the data description json.

        Parameters
        ------------------------
        parsed_data: dict
            Parsed data for the dataset. Contains
            mouse id and creation datetime

        dataset_info: dict
            Information for the dataset

        output_path: PathLike
            Path where the dataset is located

        """

        asi_file = original_dataset_path.joinpath("ASI_logging.txt")
        mdata_file = original_dataset_path.joinpath("metadata.txt")
        filter_mapping = self.__get_excitation_emission_waves(
            original_dataset_path
        )

        with open(mdata_file, "rb") as f:
            lc_mdata_result = chardet.detect(f.read())

        with open(mdata_file, "r", encoding=lc_mdata_result["encoding"]) as f:
            lc_mdata = f.readlines()

        # Get information where starts each metadata block
        # in the metadata file
        line_indices = get_line_indices_metadata_file(lc_mdata)

        # Parse first section
        session_config = get_session_config(lines=lc_mdata)

        # Getting wavelengths
        wavelength_config = get_wavelength_config(
            lines=lc_mdata,
            start_index=line_indices["wavelength_start"] + 1,
            end_index=line_indices["tile_acquisition_start"],
        )

        # Getting tile info
        tile_config = get_tile_info(
            lines=lc_mdata,
            start_index=line_indices["tile_acquisition_start"] + 1,
        )

        metadata_dict = {
            "session_config": session_config,
            "wavelength_config": wavelength_config,
            "tile_config": tile_config,
        }

        session_end_time = get_session_end(asi_file)

        # Validating data in config
        instrument_id = file_utils.helper_validate_key_dict(
            dictionary=dataset_info, key="instrument_id"
        )
        experimenter_full_name = file_utils.helper_validate_key_dict(
            dictionary=dataset_info, key="experimenter"
        )
        local_storage_directory = file_utils.helper_validate_key_dict(
            dictionary=dataset_info, key="local_storage"
        )
        chamber_immersion_medium = file_utils.helper_validate_key_dict(
            dictionary=dataset_info["chamber_immersion"], key="medium"
        )
        chamber_immersion_ri = file_utils.helper_validate_key_dict(
            dictionary=dataset_info["chamber_immersion"],
            key="refractive_index",
        )
        sample_immersion_medium = file_utils.helper_validate_key_dict(
            dictionary=dataset_info["sample_immersion"], key="medium"
        )
        sample_immersion_ri = file_utils.helper_validate_key_dict(
            dictionary=dataset_info["sample_immersion"], key="refractive_index"
        )

        # Giving the specific error of what is missing
        if instrument_id is None:
            raise ValueError("Instrument id not provided in manifest")

        if experimenter_full_name is None:
            raise ValueError("Experimenter full name not provided in manifest")

        if chamber_immersion_medium is None:
            raise ValueError(
                "Chamber immersion medium not provided in manifest"
            )

        if chamber_immersion_ri is None:
            raise ValueError("Chamber immersion ri not provided in manifest")

        acquisition_model = acquisition.Acquisition(
            specimen_id="",
            instrument_id=instrument_id,
            experimenter_full_name=experimenter_full_name,
            subject_id=parsed_data["mouse_id"],
            session_start_time=parsed_data["mouse_date"],
            session_end_time=session_end_time,
            local_storage_directory=local_storage_directory,
            external_storage_directory="",
            chamber_immersion=acquisition.Immersion(
                medium=chamber_immersion_medium,
                refractive_index=chamber_immersion_ri,
            ),
            sample_immersion=acquisition.Immersion(
                medium=sample_immersion_medium,
                refractive_index=sample_immersion_ri,
            ),
            axes=[
                acquisition.Axis(
                    name="X",
                    dimension=2,
                    direction="Left_to_right",
                ),
                acquisition.Axis(
                    name="Y", dimension=1, direction="Posterior_to_anterior"
                ),
                acquisition.Axis(
                    name="Z", dimension=0, direction="Superior_to_inferior"
                ),
            ],
            tiles=make_acq_tiles(
                metadata_dict=metadata_dict, filter_mapping=filter_mapping
            ),
        )

        acquisition_path = str(output_path.joinpath("acquisition.json"))

        with open(acquisition_path, "w") as f:
            f.write(acquisition_model.json(indent=3))

    def __create_smartspim_metadata(
        self,
        parsed_data: dict,
        dataset_info: dict,
        original_dataset_path: PathLike,
        output_path: PathLike,
    ) -> None:
        """
        Creates the data description json.

        Parameters
        ------------------------
        parsed_data: config
            Dictionary with the data from the dataset name.
            It includes mouse id, creation date and time for the dataset

        dataset_info: dict
            Information for the dataset

        dataset_path: PathLike
            Path where the dataset is located
        """

        output_path = Path(output_path)

        # Creates the data description json
        if "data_description" in dataset_info:
            self.__create_data_description(
                parsed_data, dataset_info["data_description"], output_path
            )
        else:
            logger.error(
                f"data_description.json was not created for {parsed_data['mouse_id']}. Add it to the YAML configuration."
            )

        # Creates the subject metadata json
        self.__create_subject(parsed_data["mouse_id"], output_path)

        # Creates the acquisition json
        if "acquisition" in dataset_info:
            try:
                self.__create_acquisition(
                    parsed_data,
                    dataset_info["acquisition"],
                    original_dataset_path,
                    output_path,
                )
            except ValueError:
                logger.error("Error creating acquisition schema")
        else:
            logger.error(
                f"acquisition.json was not created for {parsed_data['mouse_id']}. Add it to the YAML configuration."
            )

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
                logger.info(f"\nOrganizing: {dataset_path}")
                (
                    new_dataset_path,
                    parsed_data,
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
                        parsed_data=parsed_data,
                        dataset_info=dataset_info,
                        original_dataset_path=dataset_path,
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
