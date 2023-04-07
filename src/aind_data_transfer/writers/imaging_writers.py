import logging
import os
import re
from datetime import date, datetime, time
from pathlib import Path
from typing import List, Tuple, Union

from aind_data_schema import Funding, RawDataDescription, Subject
from aind_data_schema.data_description import ExperimentType, Modality, Institution, Group
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


def get_scale(lc_mdata) -> tile.Scale3dTransform:
    """
    Get scales metadata

    Parameters
    -----------
    lc_mdata: List[bytes]
        List with bytes containing the
        scale information

    Returns
    -----------
    aind_data_schema.imaging.tile.Scale3dTransform
        Scales for the imaging data
    """

    line = lc_mdata[1]
    xy, z = line.split()[3:5]
    scale = tile.Scale3dTransform(scale=[xy, xy, z])

    return scale


def make_acq_tiles(lc_mdata: bytes, filter_mapping: dict):
    """
    Makes metadata for the acquired tiles of
    the dataset

    Parameters
    -----------
    lc_mdata: List[bytes]
        List with bytes containing the
        scale information

    Returns
    -----------
    List[tile.Translation3dTransform]
        List with the metadata for the tiles
    """

    channels = {}

    wavelength_line = None
    tiles_line = None

    for idx_line in range(len(lc_mdata)):

        if "Wavelength" in str(lc_mdata[idx_line]):
            wavelength_line = idx_line + 1

        elif "Skip" in str(lc_mdata[idx_line]):
            tiles_line = idx_line + 1

    tiles = []

    if wavelength_line and tiles_line:
        for idx, l in enumerate(lc_mdata[wavelength_line : tiles_line - 1]):
            wavelength, powerl, powerr = [float(j) for j in l.split()]

            channel = tile.Channel(
                channel_name=wavelength,
                laser_wavelength=wavelength,
                laser_power=powerl,
                filter_wheel_index=idx,
            )

            channels[wavelength] = channel

        scale = get_scale(lc_mdata)

        for line in lc_mdata[tiles_line:]:
            X, Y, Z, W, S, E, Sk = [int(float(i)) for i in line.split()]

            tform = tile.Translation3dTransform(
                translation=[int(float(i)) / 10 for i in line.split()[0:3]]
            )

            channel = channels[float(W)]
            t = tile.AcquisitionTile(
                channel=channel,
                notes=(
                    "\nLaser power is in percentage of total -- needs calibration"
                ),
                coordinate_transformations=[tform, scale],
                file_name=f"Ex_{W}_Em_{filter_mapping[W]}/{X}/{X}_{Y}/",
            )
            tiles.append(t)
    return tiles


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

        # Creating data description
        data_description = RawDataDescription(
            modality=[Modality.SPIM],
            experiment_type=ExperimentType.SMARTSPIM,
            subject_id=parsed_data["mouse_id"],
            creation_date=date(
                mouse_date.year, mouse_date.month, mouse_date.day
            ),
            creation_time=time(
                mouse_date.hour, mouse_date.minute, mouse_date.second
            ),
            # Parses the attribute name to an institution...
            institution=Institution(dataset_info["institution"]),
            group=Group.MSMA,
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

    def __create_adquisition(
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

        with open(mdata_file, "rb") as file:
            lc_mdata = file.readlines()

        session_end_time = get_session_end(asi_file)

        acquisition_model = acquisition.Acquisition(
            specimen_id="",
            instrument_id=dataset_info["instrument_id"],
            experimenter_full_name=dataset_info["experimenter"],
            subject_id=parsed_data["mouse_id"],
            session_start_time=parsed_data["mouse_date"],
            session_end_time=session_end_time,
            local_storage_directory=dataset_info["local_storage_directory"],
            external_storage_directory="",
            chamber_immersion=acquisition.Immersion(
                medium=dataset_info["immersion"]["medium"],
                refractive_index=dataset_info["immersion"]["refractive_index"],
            ),
            axes=[
                acquisition.Axis(
                    name="X", dimension=2, direction="Left_to_right",
                ),
                acquisition.Axis(
                    name="Y", dimension=1, direction="Posterior_to_anterior"
                ),
                acquisition.Axis(
                    name="Z", dimension=0, direction="Superior_to_inferior"
                ),
            ],
            tiles=make_acq_tiles(lc_mdata, filter_mapping),
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

        # Creates the adquisition json
        if "adquisition" in dataset_info:
            self.__create_adquisition(
                parsed_data,
                dataset_info["adquisition"],
                original_dataset_path,
                output_path,
            )
        else:
            logger.error(
                f"adquisition.json was not created for {parsed_data['mouse_id']}. Add it to the YAML configuration."
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
                logger.info(f"Processing: {dataset_path}\n")
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
