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
from aind_data_schema.core.subject import Subject
from aind_data_schema.core.data_description import (
    Funding,
    RawDataDescription,
)
from aind_data_schema.models.platforms import Platform
from aind_data_schema.models.modalities import Modality
from aind_data_schema.models.organizations import Organization

from aind_data_schema.models.coordinates import ImageAxis, Axis, AnatomicalDirection
from aind_data_schema.imaging import tile
from aind_data_schema.core import acquisition
from aind_data_schema.models.units import SizeUnit, PowerUnit

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
    data["µm/pix"] = um_per_pix
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
            channel_name=str(wavelength),
            light_source_name=str(wavelength),
            filter_names=[""], # We don't have filter names at the moment
            detector_name="", # We don't have detector names at the moment
            additional_device_names=[],
            # Excitation wavelenghts
            excitation_wavelength=int(wavelength),
            excitation_wavelength_unit=SizeUnit.NM,
            # Excitation power
            excitation_power=value["power_left"], # [value["power_left"], value["power_right"]]
            excitation_power_unit=PowerUnit.PERCENT,
            filter_wheel_index=filter_wheel_idx,
        )
        filter_wheel_idx += 1

        channels[wavelength] = channel

    # Scale metadata
    session_config = metadata_dict.get("session_config")

    x_res = y_res = session_config.get("um/pix")
    z_res = session_config.get("Z step (um)")
    
    # utf-8 error with micron symbol
    if x_res is None:
        x_res = y_res = session_config.get("m/pix")
        if x_res is None:
            raise KeyError("Failed getting the x and y resolution from metadata.json")
    
    if z_res is None:
        z_res = session_config.get("Z step (m)")

        if z_res is None:
            raise KeyError("Failed to get the Z step in microns")

        z_res = float(z_res)

    x_res = float(x_res)
    y_res = float(y_res)
    z_res = float(z_res)

    scale = tile.Scale3dTransform(
        scale=[
            x_res,  # X res
            y_res,  # Y res
            z_res,  # Z res
        ]
    )

    for tile_key, tile_info in metadata_dict["tile_config"].items():

        tile_info_x = tile_info.get("x")
        tile_info_y = tile_info.get("y")
        tile_info_z = tile_info.get("z")

        # For some reason, Jeff changed the lower case to upper case
        if tile_info_x is None:
            tile_info_x = tile_info.get("X")

        if tile_info_y is None:
            tile_info_y = tile_info.get("Y")

        if tile_info_z is None:
            tile_info_z = tile_info.get("Z")

        tile_info_x = float(tile_info_x)
        tile_info_y = float(tile_info_y)
        tile_info_z = float(tile_info_z)

        tile_transform = tile.Translation3dTransform(
            translation=[
                int(tile_info_x) / 10,
                int(tile_info_y) / 10,
                int(tile_info_z) / 10,
            ]
        )
        
        # print("Keys before breaking: ", tile_info.keys())
        channel = channels[tile_info["Laser"]]
        exaltation_wave = int(tile_info["Laser"])
        emission_wave = filter_mapping[exaltation_wave]

        tile_acquisition = tile.AcquisitionTile(
            channel=channel,
            notes=(
                "\nLaser power is in percentage of total, it needs calibration"
            ),
            coordinate_transformations=[tile_transform, scale],
            file_name=f"Ex_{exaltation_wave}_Em_{emission_wave}/{tile_info_x}/{tile_info_x}_{tile_info_y}/",
        )

        tile_acquisitions.append(tile_acquisition)

    return tile_acquisitions


def get_anatomical_direction(anatomical_direction: str) -> AnatomicalDirection:
    """
    This function returns the correct anatomical
    direction defined in the aind_data_schema.

    Parameters
    ----------
    anatomical_direction: str
        String defining the anatomical direction
        of the data

    Returns
    -------
    AnatomicalDirection: class::Enum
        Corresponding enum defined in the anatomical
        direction class
    """

    enum_anatomical_direction = AnatomicalDirection.OTHER
    anatomical_direction = anatomical_direction.lower()

    # Note: I could have done str.capitalize and then get
    # the anatomical direction directly from the class
    # but we have multiple versions now and I want to make this
    # robust
    if anatomical_direction == "left_to_right":
        anatomical_direction = AnatomicalDirection.LR
    
    elif anatomical_direction == "right_to_left":
        anatomical_direction = AnatomicalDirection.RL

    elif anatomical_direction == "anterior_to_posterior":
        anatomical_direction = AnatomicalDirection.AP
    
    elif anatomical_direction == "posterior_to_anterior":
        anatomical_direction = AnatomicalDirection.PA
    
    elif anatomical_direction == "inferior_to_superior":
        anatomical_direction = AnatomicalDirection.IS
    
    elif anatomical_direction == "superior_to_inferior":
        anatomical_direction = AnatomicalDirection.SI

    return anatomical_direction

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
            institution = Organization.OTHER
            if dataset_info["institution"]["abbreviation"] == "AIND" or dataset_info["institution"]["abbreviation"] == "NYU":
                institution = Organization.AIND
            
            elif dataset_info["institution"]["abbreviation"] == "AIBS":
                institution = Organization.AIBS

        funding_sources = file_utils.helper_validate_key_dict(
            dictionary=dataset_info,
            key="funding",
            default_return=[Funding(funder=Organization.AI)], # setting Allen Institute by default
        )
        funding_sources = [
            Funding.parse_obj(funding_source)
            for funding_source in funding_sources
        ]

        project_name = file_utils.helper_validate_key_dict(
            dictionary=dataset_info, key="project"
        )
        project_id = file_utils.helper_validate_key_dict(
            dictionary=dataset_info, key="project_id"
        )
        group = file_utils.helper_validate_key_dict(
            dictionary=dataset_info, key="group", default_return=None
        )

        # Creating data description
        data_description = RawDataDescription(
            modality=[Modality.SPIM],
            platform=Platform.SMARTSPIM,
            subject_id=parsed_data["mouse_id"],
            creation_time=datetime(mouse_date.year, mouse_date.month, mouse_date.day, mouse_date.hour, mouse_date.minute, mouse_date.second),
            institution=institution,
            group=group,
            project_name=project_name,
            # project_id=project_id,
            funding_source=funding_sources,
            investigators=[""],
        )

        data_description_path = str(
            output_path.joinpath("data_description.json")
        )
        
        with open(data_description_path, "w") as f:
            f.write(data_description.model_dump_json())

    def __create_subject(self, mouse_id: str, output_path: PathLike):
        """
        Creates the subject json.

        Parameters
        ------------------------
        mouse_id: str
            Mouse id for the dataset

        output_path: PathLike
            Path where the dataset is located
        """

        client = AindMetadataServiceClient(self.__metadata_domain)

        response = client.get_subject(mouse_id)

        if response.status_code == 200 or response.status_code == 406:
            data = response.json()["data"]

            # Setting breeding info to empty str since data schema does not allow
            # None values and metadata service retrieves None instead of empty
            breed_info = data.get('breeding_info')

            if breed_info:
                for key, val in breed_info.items():
                    if val is None:
                        data['breeding_info'][key] = ""
            else:
                data['breeding_info'] = {
                    'breeding_group': '',
                    'maternal_id': '',
                    'maternal_genotype': '',
                    'paternal_id': '',
                    'paternal_genotype': '',
                    
                }

            subject = Subject(
                subject_id=data["subject_id"],
                sex=data["sex"],
                date_of_birth=data["date_of_birth"],
                genotype=data["genotype"],
                species=data["species"],
                alleles=data["alleles"],
                background_strain=data["background_strain"],
                breeding_info=data["breeding_info"],
                source=data["source"],
                rrid=data["rrid"],
                restrictions=data["restrictions"],
                wellness_reports=data["wellness_reports"],
                housing=data["housing"],
                notes=data["notes"],
            )

            subject_path = str(output_path.joinpath("subject.json"))

            with open(subject_path, "w") as f:
                f.write(subject.model_dump_json())

        else:
            logger.error(
                f"Mouse {mouse_id} does not have subject information - res status: {response.status_code}"
            )

    def __create_procedures(self, mouse_id: str, output_path: PathLike):
        """
        Creates the procedures json.

        Parameters
        ------------------------
        mouse_id: str
            Mouse id for the dataset

        output_path: PathLike
            Path where the dataset is located
        """

        client = AindMetadataServiceClient(self.__metadata_domain)

        response = client.get_procedures(mouse_id)

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

    def __get_excitation_emission_waves(self, channels: List) -> dict:
        """
        Gets the excitation and emission waves for
        the existing channels within a dataset

        Parameters
        ------------
        channels: List[str]
            List with the channels.
            They must contain the emmision
            wavelenght in the name

        Returns
        ------------
        dict
            Dictionary with the excitation
            and emission waves
        """
        excitation_emission_channels = {}

        for channel in channels:
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
        channels: List
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

        channels: List
            List of channels in the dataset. These must contain
            the emmision wavelength in the name.
        """

        asi_file = original_dataset_path.joinpath("ASI_logging.txt")
        mdata_file = original_dataset_path.joinpath("metadata.txt")

        mdata_json_file = original_dataset_path.joinpath("metadata.json")

        if not os.path.exists(asi_file):
            raise FileNotFoundError(f"File {asi_file} does not exist")

        session_end_time = get_session_end(asi_file)
        filter_mapping = self.__get_excitation_emission_waves(
            channels
        )

        session_config = None
        wavelength_config = None
        tile_config = None

        # Checking if json file exists
        if os.path.exists(mdata_json_file):
            metadata_info = file_utils.read_json_as_dict(mdata_json_file)
            
            session_config = metadata_info["session_config"]
            wavelength_config = metadata_info["wavelength_config"]
            tile_config = metadata_info["tile_config"]

        elif os.path.exists(mdata_file):

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

        else:
            # No metadata found
            raise FileNotFoundError("No metadata file found")


        if None in [session_config, wavelength_config, tile_config]:
            raise ValueError("Not able to parse the metadata") 

        # Metadata dictionary
        metadata_dict = {
            "session_config": session_config,
            "wavelength_config": wavelength_config,
            "tile_config": tile_config,
        }

        # Validating data in config
        instrument_id = file_utils.helper_validate_key_dict(
            dictionary=dataset_info, key="instrument_id"
        )
        experimenter_full_name = file_utils.helper_validate_key_dict(
            dictionary=dataset_info, key="experimenter_full_name"
        )
        local_storage_directory = file_utils.helper_validate_key_dict(
            dictionary=dataset_info, key="local_storage_directory"
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

        axes = dataset_info.get("axes")

        if axes is None:
            raise ValueError("Please, check the axes orientation")
        
        axes = [
            ImageAxis(
                name=ax['name'],
                dimension=ax['dimension'],
                direction=get_anatomical_direction(ax['direction']),
                # unit=ax['unit']
            )
            for ax in axes
        ]
    
        notes = f"Chamber immersion: {chamber_immersion_medium} - Sample immersion: {sample_immersion_medium}"
        if "cargille" in chamber_immersion_medium.lower():
            chamber_immersion_medium = "oil"
        
        else:
            chamber_immersion_medium = "other"

        if "cargille" in sample_immersion_medium.lower():
            sample_immersion_medium = "oil"
        
        else:
            sample_immersion_medium = "other"

        acquisition_model = acquisition.Acquisition(
            experimenter_full_name=experimenter_full_name,
            specimen_id="",
            subject_id=parsed_data["mouse_id"],
            instrument_id=instrument_id,
            session_start_time=parsed_data["mouse_date"],
            session_end_time=session_end_time,
            tiles=make_acq_tiles(
                metadata_dict=metadata_dict, filter_mapping=filter_mapping
            ),
            axes=axes,
            chamber_immersion=acquisition.Immersion(
                medium=chamber_immersion_medium,
                refractive_index=chamber_immersion_ri,
            ),
            sample_immersion=acquisition.Immersion(
                medium=sample_immersion_medium,
                refractive_index=sample_immersion_ri,
            ),
            local_storage_directory=local_storage_directory,
            external_storage_directory="",
            # processing_steps=[],
            notes=notes
        )

        acquisition_path = str(output_path.joinpath("acquisition.json"))

        with open(acquisition_path, "w") as f:
            f.write(acquisition_model.model_dump_json())

    def __create_smartspim_metadata(
        self,
        parsed_data: dict,
        dataset_info: dict,
        original_dataset_path: PathLike,
        output_path: PathLike,
        channels: List
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
        
        channels: List
            List of channels in the dataset. These must contain
            the emmision wavelength in the name.
        """

        output_path = Path(output_path)

        # Creates the data description json
        self.__create_data_description(
            parsed_data, dataset_info["prelim_data_description"], output_path
        )

        # Creates the subject metadata json
        self.__create_subject(parsed_data["mouse_id"], output_path)

        # Creates the acquisition json
        self.__create_acquisition(
            parsed_data,
            dataset_info["prelim_acquisition"],
            original_dataset_path,
            output_path,
            channels
        )

        # moving instrument.json
        file_utils.move_folders_or_files(
            original_dataset_path,
            output_path,
            "instrument.json",
            mode="move",
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

                    # Dictionary with excitation and emission mappings
                    smartspim_channel_translation = dataset_info.get('channel_translation')

                    smartspim_excitation_wav = smartspim_channel_translation.get('excitation').values()
                    smartspim_emission_wav = smartspim_channel_translation.get('emission')

                    if smartspim_channel_translation is None:
                        raise ValueError("We need a channel translation for the LifeCanvas microscope!")
                    
                    # Getting raw channel names
                    channels = [
                        element
                        for element in os.listdir(dataset_path)
                        if re.match(self.__regex_expressions.regex_channels.value, element)
                    ]

                    smartspim_final_translation = {}
                    for channel_idx in range(len(channels)):
                        
                        filter_index = str(
                            channels[channel_idx].split('_')[-1].replace('Ch', '')
                        )
                        folder_map = channels[channel_idx].split('_')
                        folder_map[-1] = f"Em_{smartspim_emission_wav[filter_index]}"
                        smartspim_final_translation[channels[channel_idx]] = "_".join(folder_map)

                    check_map = min([
                        channel in smartspim_final_translation
                        for channel in channels
                    ])

                    if not check_map:
                        raise ValueError(f"Missing channels in the map, provided {channels} - Map: {smartspim_channel_translation.keys()}")

                    # Create smartspim metadata
                    self.__create_smartspim_metadata(
                        parsed_data=parsed_data,
                        dataset_info=dataset_info,
                        original_dataset_path=dataset_path,
                        output_path=new_dataset_path,
                        channels=list(smartspim_final_translation.values())
                    )

                    # Moving channels
                    # Using smartspim_channel_translation since the new
                    # LifeCanvas software modifies removed the Emission
                    # wavelength
                    file_utils.move_folders_or_files(
                        dataset_path,
                        new_dataset_path.joinpath("SmartSPIM"),
                        self.__regex_expressions.regex_channels.value,
                        mode=mode,
                        map_dictionary=smartspim_final_translation
                    )

                    modified_channel_translation = None
                    if smartspim_final_translation:
                        postfix = "_MIP"
                        modified_channel_translation = {}
                        for orig_ch, mod_ch in smartspim_final_translation.items():
                            modified_key = orig_ch + postfix
                            modified_value = mod_ch + postfix
                            modified_channel_translation[modified_key] = modified_value

                    # Moving maximum intensity projections per channel
                    file_utils.move_folders_or_files(
                        dataset_path,
                        new_dataset_path.joinpath("derivatives"),
                        self.__regex_expressions.regex_channels_MIP.value,
                        mode=mode,
                        map_dictionary=modified_channel_translation
                    )

                    # Moving metadata files
                    file_utils.move_folders_or_files(
                        dataset_path,
                        new_dataset_path.joinpath("derivatives"),
                        self.__regex_expressions.regex_files.value,
                        mode=mode,
                    )

                    # Moving acquisition qc
                    file_utils.move_folders_or_files(
                        dataset_path,
                        new_dataset_path.joinpath("derivatives"),
                        "acquisition_qc",
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
