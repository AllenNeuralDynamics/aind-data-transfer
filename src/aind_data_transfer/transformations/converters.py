from datetime import datetime
import xml.etree.ElementTree as ET
from typing import List
import numpy as np

import pandas as pd

import aind_data_transfer.transformations.file_io as file_io

from aind_data_schema.core.acquisition import (
    AxisName,
    Direction,
    Axis,
    Immersion,
    Acquisition,
    AcquisitionTile,
)
from aind_data_schema.imaging.tile import (
    Channel,
    Scale3dTransform,
    Translation3dTransform,
)
from datetime import datetime
import tifffile
import re
import pathlib
from aind_data_transfer.transformations.deinterleave import (
    ChannelParser,
    Deinterleave,
)

MM_TO_UM = 1000

def read_dispim_aquisition(acq_path: str) -> Acquisition:
    """Read json formatted acquisition file, output by iSPIM rig
    Parameters
    ----------
    acq_path: str
        Path to acquisition.json file
    Returns
    -------
    Acquisition
        Acquisition instance
    """
    acq_path = pathlib.Path(acq_path)
    assert acq_path.exists(), f"acquisition.json file not found at {acq_path}"

    acq_json = file_io.read_json(acq_path)

    # get general metadata
    experimenter_full_name: List[str] = acq_json["experimenter_full_name"][0]
    specimen_id: str = acq_json["specimen_id"]
    subject_id: str = acq_json["subject_id"]
    instrument_id: str = acq_json["instrument_id"]
    session_start_time: datetime = acq_json["session_start_time"]
    session_end_time: datetime = acq_json["session_end_time"]
    tiles_list: List[AcquisitionTile] = acq_json["tiles"]
    axes: List[Axis] = acq_json["axes"]
    chamber_immersion: Immersion = acq_json["chamber_immersion"]
    local_storage_directory: str = acq_json["local_storage_directory"]
    external_storage_directory: str = acq_json["external_storage_directory"]

    tiles = []
    for tile_dict in tiles_list: 
        # rewrite the tranlsation transform to be in microns
        if tile_dict['tile_position_units'] == 'millimeters':
            translation_tfm = Translation3dTransform(translation=
                        [float(tile_dict['tile_x_position']) * MM_TO_UM , #TODO there's a bug where these should be 1000, but the mm is recorded wrong. 
                        float(tile_dict['tile_y_position']) * MM_TO_UM ,
                        float(tile_dict['tile_z_position']) * MM_TO_UM ])
        elif tile_dict['tile_position_units'] == 'microns':
            translation_tfm = Translation3dTransform(translation=
                            [float(tile_dict['tile_x_position']) , 
                            float(tile_dict['tile_y_position'])  ,
                            float(tile_dict['tile_z_position'])  ])
        
        scale_tfm = Scale3dTransform(scale=[float(tile_dict['x_voxel_size']), 
                                            float(tile_dict['y_voxel_size']), 
                                            float(tile_dict['z_voxel_size'])])
        ch = Channel(channel_name=tile_dict['channel_name'],
                light_source_name=tile_dict['laser_wavelength'],
                filter_names = ['0', '1', '2', '3', '4', '5'], 
                filter_wheel_index=tile_dict['filter_wheel_index'],
                detector_name = 'iSPIM', 
                excitation_wavelength=tile_dict['laser_wavelength'],
                excitation_power=tile_dict['laser_power'])

        if type(tile_dict['file_name']) == list:
            tile = AcquisitionTile(channel=ch, 
                                file_name=tile_dict['file_name'][0],
                                imaging_angle=tile_dict['lightsheet_angle'], 
                                coordinate_transformations=[scale_tfm, translation_tfm])
        elif type(tile_dict['file_name']) == str:
            tile = AcquisitionTile(channel=ch, 
                                file_name=tile_dict['file_name'],
                                imaging_angle=tile_dict['lightsheet_angle'], 
                                coordinate_transformations=[scale_tfm, translation_tfm])
    
        tiles.append(tile)

    return Acquisition(experimenter_full_name=[experimenter_full_name], 
                       specimen_id=specimen_id, 
                       subject_id=subject_id, 
                       instrument_id=instrument_id, 
                       session_start_time=session_start_time, 
                       session_end_time=session_end_time, 
                       tiles=tiles, 
                       axes=axes,
                       chamber_immersion=chamber_immersion, 
                       local_storage_directory=local_storage_directory,
                       external_storage_directory=external_storage_directory)




def log_to_acq_json(log_dict: dict) -> Acquisition:
    """
    Parameters
    ----------
    log_dict: dict
        Output of file_io.read_log_file.
        log_dict formats general metadata as flat dictionary, and tile metadata in a nested dict.
    Returns
    -------
    Acquisition
        Acquisition instance
    """

    experimenter_full_name: List[str] = ["ISpim Group"]
    specimen_id: str = log_dict["specimen_id"]
    subject_id: str = log_dict["subject_id"]
    instrument_id: str = log_dict["instrument_id"]
    session_start_time: datetime = log_dict["session_start_time"]
    session_end_time: datetime = log_dict["session_end_time"]

    # convert session start and end times to datetime objects
    session_start_time = datetime.strptime(
        session_start_time, "%Y-%m-%dT%H:%M:%S"
    )
    session_end_time = datetime.strptime(
        session_end_time, "%Y,%m,%d,%H,%M,%S"
    )  # the imaging_log file may change this format

    tiles: list[AcquisitionTile] = []
    for tile_dict in log_dict["tiles"].values():
        scale_tfm = Scale3dTransform(
            scale=[
                float(
                    log_dict["config_toml"]["tile_specs"]["x_field_of_view_um"]
                    / log_dict["config_toml"]["tile_specs"]["row_count_pixels"]
                ),
                float(
                    log_dict["config_toml"]["tile_specs"]["y_field_of_view_um"]
                    / log_dict["config_toml"]["tile_specs"][
                        "column_count_pixels"
                    ]
                ),
                float(log_dict["z_voxel_size"]),
            ]
        )
        translation_tfm = Translation3dTransform(
            translation=[
                float(tile_dict["tile_x_position"]),
                float(tile_dict["tile_y_position"]),
                float(tile_dict["tile_z_position"]),
            ]
        )

        if (
            log_dict["config_toml"]["imaging_specs"]["acquisition_style"]
            == "interleaved"
        ):
            for channel in tile_dict["channel"]:
                channel_dict = log_dict["channels"][channel]
                ch = Channel(channel_name=tile_dict['channel_name'],
                            light_source_name=tile_dict['laser_wavelength'],
                            filter_names = ['0', '1', '2', '3', '4', '5'], 
                            filter_wheel_index=tile_dict['filter_wheel_index'],
                            detector_name = 'iSPIM', 
                            excitation_wavelength=tile_dict['laser_wavelength'],
                            excitation_power=tile_dict['laser_power'])

                tile = AcquisitionTile(
                    channel=ch,
                    file_name=tile_dict["file_name"],
                    imaging_angle=log_dict["lightsheet_angle"],
                    coordinate_transformations=[scale_tfm, translation_tfm],
                )
                tiles.append(tile)
        elif (
            log_dict["config_toml"]["imaging_specs"]["acquisition_style"]
            == "sequential"
        ):
            channel = tile_dict["channel"]
            channel_dict = log_dict["channels"][channel]
            ch = Channel(channel_name=tile_dict['channel_name'],
                     light_source_name=tile_dict['laser_wavelength'],
                     filter_names = ['0', '1', '2', '3', '4', '5'], 
                     filter_wheel_index=tile_dict['filter_wheel_index'],
                     detector_name = 'iSPIM', 
                     excitation_wavelength=tile_dict['laser_wavelength'],
                     excitation_power=tile_dict['laser_power'])

            tile = AcquisitionTile(
                channel=ch,
                file_name=tile_dict["file_name"],
                imaging_angle=log_dict["lightsheet_angle"],
                coordinate_transformations=[scale_tfm, translation_tfm],
            )
            tiles.append(tile)

    # NOTE: These directions are in "camera coordinates" (ie. the data is stored in this order, and the cells/data have a 45 degree skew from the "optical coordinates")
    axes: list[Axis] = []
    axes.append(Axis(name=AxisName.X, dimension=2, direction=Direction.AP))
    axes.append(Axis(name=AxisName.Y, dimension=1, direction=Direction.LR))
    axes.append(Axis(name=AxisName.Z, dimension=0, direction=Direction.IS))

    chamber_immersion: Immersion = Immersion(
        medium=log_dict["chamber_immersion_medium"],
        refractive_index=log_dict["chamber_immersion_refractive_index"],
    )

    local_storage_directory: str = log_dict[
        "local_storage_directory"
    ]  # should come from config.yml (in case of files being moved before uploading to s3)
    # assert local_storage_directory is accessible and exists
    if not pathlib.Path(local_storage_directory).exists():
        local_storage_directory = log_dict['data_src_dir']
    
    external_storage_directory: str = log_dict['external_storage_directory']

    return Acquisition(experimenter_full_name=[experimenter_full_name], 
                       specimen_id=specimen_id, 
                       subject_id=subject_id, 
                       instrument_id=instrument_id, 
                       session_start_time=session_start_time, 
                       session_end_time=session_end_time, 
                       tiles=tiles, 
                       axes=axes,
                       chamber_immersion=chamber_immersion, 
                       local_storage_directory=local_storage_directory,
                       external_storage_directory=external_storage_directory)

def schema_log_to_acq_json(log_dict: dict) -> Acquisition:
    """
    Parameters
    ----------
    log_dict: dict
        Output of file_io.read_log_file. 
        log_dict formats general metadata as flat dictionary, and tile metadata in a nested dict. 
    Returns 
    -------
    Acquisition
        Acquisition instance
    """

    experimenter_full_name: str = 'ISpim Group'
    specimen_id: str = log_dict['specimen_id']
    subject_id: str = log_dict['subject_id']
    instrument_id: str = log_dict['instrument_id']
    session_start_time: datetime = log_dict['session_start_time']
    session_end_time: datetime = log_dict['session_end_time']

    tiles: list[AcquisitionTile] = []
    for tile_dict in log_dict['tiles']:
        # ch = Channel(channel_name=tile_dict['channel_name'], 
        #              laser_wavelength=int(tile_dict['laser_wavelength']), 
        #              laser_power=tile_dict['laser_power'],
        #              filter_wheel_index=tile_dict['filter_wheel_index'])
        
        ch = Channel(channel_name=tile_dict['channel_name'],
                     light_source_name=tile_dict['laser_wavelength'],
                     filter_names = ['0', '1', '2', '3', '4', '5'], 
                     filter_wheel_index=tile_dict['filter_wheel_index'],
                     detector_name = 'iSPIM', 
                     excitation_wavelength=tile_dict['laser_wavelength'],
                     excitation_power=tile_dict['laser_power'])
        scale_tfm = Scale3dTransform(scale=[float(tile_dict['x_voxel_size']), 
                                            float(tile_dict['y_voxel_size']), 
                                            float(tile_dict['z_voxel_size'])])
        #want this to be in pixels 
        if tile_dict['tile_position_units'] == 'millimeters':
            translation_tfm = Translation3dTransform(translation=
                        [float(tile_dict['tile_x_position']) * MM_TO_UM, 
                        float(tile_dict['tile_y_position']) * MM_TO_UM ,
                        float(tile_dict['tile_z_position']) * MM_TO_UM ])
        elif tile_dict['tile_position_units'] == 'microns':
            translation_tfm = Translation3dTransform(translation=
                            [float(tile_dict['tile_x_position']) , 
                            float(tile_dict['tile_y_position'])  ,
                            float(tile_dict['tile_z_position'])  ])
            
        if type(tile_dict['file_name']) == list:
            tile = AcquisitionTile(channel=ch, 
                                file_name=tile_dict['file_name'][0],
                                imaging_angle=tile_dict['lightsheet_angle'], 
                                coordinate_transformations=[scale_tfm, translation_tfm])
        elif type(tile_dict['file_name']) == str:
            tile = AcquisitionTile(channel=ch, 
                                file_name=tile_dict['file_name'],
                                imaging_angle=tile_dict['lightsheet_angle'], 
                                coordinate_transformations=[scale_tfm, translation_tfm])
        else: 
            raise TypeError(f"tile_dict['file_name'] is of type {type(tile_dict['file_name'])}, but should be of type list or str")
        tiles.append(tile)


    # NOTE: Made up directions
    axes: list[Axis] = []
    axes.append(Axis(name=AxisName.X, 
                     dimension=2, 
                     direction=Direction.LR))  
    axes.append(Axis(name=AxisName.Y, 
                     dimension=1, 
                     direction=Direction.AP))
    axes.append(Axis(name=AxisName.Z, 
                     dimension=0, 
                     direction=Direction.IS))
    if 'chamber_immersion_medium' in log_dict.keys():
        chamber_immersion: Immersion = Immersion(medium=log_dict['chamber_immersion_medium'], 
                                             refractive_index=log_dict['chamber_immersion_refractive_index'])
    elif 'chamber_immersion' in log_dict.keys():
        if type(log_dict['chamber_immersion']) == dict:
            chamber_immersion: Immersion = Immersion(medium=log_dict['chamber_immersion']['medium'], 
                                             refractive_index=log_dict['chamber_immersion']['refractive_index'])
        else:
            chamber_immersion: Immersion = Immersion(medium=log_dict['chamber_immersion'], 
                                             refractive_index=log_dict['chamber_immersion_refractive_index'])
    local_storage_directory: str = log_dict['local_storage_directory']
    external_storage_directory: str = log_dict['external_storage_directory']

    return Acquisition(experimenter_full_name=[experimenter_full_name], 
                       specimen_id=specimen_id, 
                       subject_id=subject_id, 
                       instrument_id=instrument_id, 
                       session_start_time=session_start_time, 
                       session_end_time=session_end_time, 
                       tiles=tiles, 
                       axes=axes,
                       chamber_immersion=chamber_immersion, 
                       local_storage_directory=local_storage_directory,
                       external_storage_directory=external_storage_directory)

def acq_json_to_xml(acq_obj: Acquisition, log_dict: dict, data_loc: str, zarr: bool = True, condition: str = "") -> ET.ElementTree:
    """
    Parameters
    ----------
    acq_obj: Acquisition
        Acquisition instance
    log_dict: dict
        Output of file_io.read_log_file (see there for more details, keys etc)
    data_loc: str
        Relative path to dataset in linked Code Ocean data/dir
    zarr: bool
        Boolean that describes the type of the dataset: zarr or n5.
        If n5, conditioning the dataset is turned off.
    condition: str
        Conditional statement for filtering the dataset on specific attributes.
        Can filter on following 5 fields: X, Y, Z, channel, camera, filename (not recommended)
        Ex: "channel == 405 and camera = 0"
        In practice this will generally be "channel==405" or "channel==488"

        If no condition is specified, the entire dataset is uploaded.
        If acquisition style is "interleaved", the condition will be conditionally overwritten.


    Returns
    -------
    ET.ElementTree:
        ET.ElementTree instance
    """

    # Retrieve map of tile names -> transforms
    def extract_tile_translation() -> dict[str, list[float]]:
        tile_transforms: dict[str, list[float]] = {}

        for tile in acq_obj.tiles:
            # zero the translations from the first tile
            if tile == acq_obj.tiles[0]:
                [offset_x, offset_y, offset_z] = [
                    i /j
                    for i, j in zip(
                        tile.coordinate_transformations[1].translation,
                        tile.coordinate_transformations[0].scale,
                    )
                ]
                offset = [offset_x, offset_y, offset_z] 


            translation: list[float] = [
                i /j
                for i, j in zip(
                    tile.coordinate_transformations[1].translation,
                    tile.coordinate_transformations[0].scale,
                )
            ]  # this needs to be in pixels for the xml. Not microns.

            filename: str = tile.file_name


            tile_transforms[filename] = [
                el2 - el1 for el1, el2 in zip(translation, offset)
            ]

            #TODO remove this -1 once MICAH confirms that the y-basis has been flipped on the rig
            tile_transforms[filename][1] = float(tile_transforms[filename][1]) * -1 * np.sqrt(2)

            

        return tile_transforms

    def filter_tiles_on_condition(
        condition: str, acquisition_style: str = "sequential"
    ) -> list[str]:
        """
        Parses tilenames into a pandas dataframe and applies boolean masking on given str condition.
        This includes some logic to handle different tilename formats.

        Returns list of filtered tile names.

        Parameters
        ----------

        Condition: str
            Conditional statement for filtering the dataset on specific attributes.
            Can filter on following 5 fields:
                X, Y, Z, channel, camera

            Ex: "channel==405 and camera==0"

            In general, the most-used format is anticipated to be "channel==405"



        acquisition_style: str
            Acquisition style of the dataset.

            Options: ['sequential', 'interleaved']

            If the acquisition style is 'interleaved', then the
            condition may be overwritten to filter on the Rn28s channel.

        Returns
        -------
        output_tilenames: list[str]
            List of filtered tile names that satisfy the condition. These are
            the tilenames before conversion to zarr.

        output_new_tilenames`: list[str]
            List of new tilenames after conversion to zarr.


        """

        # Construct Dataframe
        fields = {
            "filename": str(),
            "X": int(),
            "Y": int(),
            "Z": int(),
            "channel": int(),
            "camera": int(),
        }
        df = pd.DataFrame(fields, index=[0])

        for i, tile in enumerate(acq_obj.tiles):
            filename: str = tile.file_name
            # use regex anchors
            if acquisition_style == "sequential":
                file_parse_regex = (
                    r"X_(\d*)_Y_(\d*)_Z_(\d*)_ch_(\d*)[_cam_]*(\d)?"
                )
                vals = re.search(file_parse_regex, filename).groups()

                X = int(vals[0])
                Y = int(vals[1])
                Z = int(vals[2])
                channel = vals[
                    3
                ]  # need to ensure this is the channel number, not the wavelength

                if vals[4] != None:
                    cam = int(vals[4])
                else:
                    cam = 0

                # match the logic from ome_zarr.py that generates the new tilenames
                tile_prefix = ChannelParser.parse_tile_xyz_loc(filename)
                new_zarr_tile_name = tile_prefix + f"_ch_{channel}" + ".zarr"
                df = pd.concat(
                    [
                        df,
                        pd.DataFrame(
                            {
                                "filename": filename,
                                "new_filename": new_zarr_tile_name,
                                "X": X,
                                "Y": Y,
                                "Z": Z,
                                "channel": (channel),
                                "camera": cam,
                            },
                            index=[i],
                        ),
                    ]
                )

            elif acquisition_style == "interleaved":
                # TODO, we will eventually get this information from one of the metadata json files (the acquisition.json?)
                # for the interim we will parse the filename
                # We need to support the following filename conventions, which come from different operators:
                # Ex2_Rn28s-{channel0}_{gene1}-{channel1}_{gene2}-{channel2}_{gene3}-{channel3}_X_{x_pos}_Y_{y_pos}_Z_{z_pos}_ch_{channel0}_{channel1}_{channel2}_{channel3}.tiff
                # somtimes these have less than 4 channels, and sometimes there is a _cam_0 or _cam_1 at the end

                # The other convention is:
                # {random_stuff_like_round}_{mouse_id}_X_{x_pos}_Y_{y_pos}_Z_{z_pos}_ch_{channel0}_{channel1}_{channel2}_{channel3}.tiff

                file_parse_regex = r"X_(\d*)_Y_(\d*)_Z_(\d*)_"
                # first convention:

                ribo_regex = r"(?:Rn28s-(\d+)?)"
                channel_regex = r".*_ch_(\d+(?:_\d+)*)[_cam_]*(\d)?\."
                try:
                    ribo_channel = re.search(ribo_regex, filename).groups()[0]
                except:
                    #grab the first channel
                    ribo_channel = acq_obj.tiles[0].channel.channel_name
                if len(ribo_channel)>0:
                    #first convention, which means we need to only return the ribo channel 
                    condition = f"channel=='{ribo_channel}'"

                # second convention: assume 405 is the ribo channel

                vals = re.search(file_parse_regex, filename).groups()
                # convert tuple to list
                vals = list(vals)

                X = int(vals[0])
                Y = int(vals[1])
                Z = int(vals[2])

                # flatten vals
                channels = re.search(channel_regex, filename).groups()
                channel = []
                try:
                    other_channels = channels[0].split("_")
                    for oc in other_channels:
                        channel.append(oc)
                except Exception as e:
                    print(e)
                    print(
                        "Error parsing channel number from filename. Continuing with next tile"
                    )
                    continue

                # get cam number if it exists
                if channels[1] != None:
                    cam = int(channels[1])
                else:
                    cam = 0

                for ch in channel:
                    # match the logic from ome_zarr.py that generates the new tilenames
                    tile_prefix = ChannelParser.parse_tile_xyz_loc(filename)
                    new_zarr_tile_name = tile_prefix + f"_ch_{ch}" + ".zarr"
                    df = pd.concat(
                        [
                            df,
                            pd.DataFrame(
                                {
                                    "filename": filename,
                                    "new_filename": new_zarr_tile_name,
                                    "X": X,
                                    "Y": Y,
                                    "Z": Z,
                                    "channel": (ch),
                                    "camera": cam,
                                },
                                index=[i],
                            ),
                        ]
                    )
            else:
                raise ValueError(
                    "acquisition_style must be either 'sequential' or 'interleaved'"
                )

        # Filter Dataframe
        if condition != "":
            results = df.query(condition)
            results = results.drop_duplicates(
                ["filename"]
            )  # remove filename duplicates
        else:
            results = df
        output_tilenames = list(results["filename"])
        output_new_tilenames = list(results["new_filename"])

        return output_tilenames, output_new_tilenames

    # Define filepaths of tiles and associated attributes
    def add_sequence_description(
        parent: ET.Element, log_dict: dict, filtered_tiles: list[str]
    ) -> None:
        def add_image_loader(
            seq_desc: ET.Element, filtered_tiles: list[str]
        ) -> None:
            img_loader = ET.SubElement(seq_desc, "ImageLoader")
            if zarr:
                img_loader.attrib["format"] = "bdv.multimg.zarr"
                img_loader.attrib["version"] = "1.0"
                x = ET.SubElement(img_loader, "zarr")
                x.attrib["type"] = "absolute"

                # should be relative path on s3 (from code_ocean)
                x.text = "/data/" + data_loc + "/"

                zgs = ET.SubElement(img_loader, "zgroups")
                for i, tile in enumerate(filtered_tiles):
                    zg = ET.SubElement(zgs, "zgroup")
                    zg.attrib["setup"] = f"{i}"
                    zg.attrib["timepoint"] = "0"
                    x = ET.SubElement(zg, "path")
                    x.text = str(pathlib.Path(tile).stem) + ".zarr"

            else:  # n5
                img_loader.attrib["format"] = "bdv.n5"
                img_loader.attrib["version"] = "1.0"
                x = ET.SubElement(img_loader, "n5")
                x.attrib["type"] = "relative"
                x.text = "dataset.n5"

        def add_view_setups(
            seq_desc: ET.Element, log_dict, filtered_tiles: list[str]
        ) -> None:
            def add_attributes(view_setups: ET.Element, log_dict) -> None:
                # Add attributes
                x = ET.SubElement(view_setups, "Attributes")
                x.attrib["name"] = "illumination"
                x = ET.SubElement(x, "Illumination")
                y = ET.SubElement(x, "id")
                y.text = "0"
                y = ET.SubElement(x, "name")
                y.text = "0"

                x = ET.SubElement(view_setups, "Attributes")
                x.attrib["name"] = "channel"
                x = ET.SubElement(x, "Channel")
                y = ET.SubElement(x, "id")
                y.text = "0"
                y = ET.SubElement(x, "name")
                y.text = "0"

                tile_atts = ET.SubElement(view_setups, "Attributes")
                tile_atts.attrib["name"] = "tile"
                for i, tile in enumerate(filtered_tiles):
                    t_entry = ET.Element("Tile")
                    id_entry = ET.Element("id")
                    id_entry.text = f"{i}"
                    t_entry.append(id_entry)
                    name_entry = ET.Element("name")
                    name_entry.text = str(pathlib.Path(tile).stem)
                    t_entry.append(name_entry)
                    tile_atts.append(t_entry)

                x = ET.SubElement(view_setups, "Attributes")
                x.attrib["name"] = "angle"
                x = ET.SubElement(x, "Angle")
                y = ET.SubElement(x, "id")
                y.text = "0"
                y = ET.SubElement(x, "name")
                y.text = "0"

            view_setups = ET.SubElement(seq_desc, "ViewSetups")
            for i, tile in enumerate(filtered_tiles):
                vs = ET.SubElement(view_setups, "ViewSetup")

                x = ET.SubElement(vs, "id")
                x.text = f"{i}"
                x = ET.SubElement(vs, "name")
                x.text = str(pathlib.Path(tile).stem)
                x = ET.SubElement(vs, "size")

                # FIXME: Test later
                # this is going to run prior to uploading the data to s3, so we need to get the shape from the metadata file if available
                # metadata_path_res = s3_path[s3_path.index('//') + 2:] + '0/.zarray'

                def get_tiff_dimensions(tile_path: str) -> list[int]:
                    with tifffile.TiffFile(tile_path) as tif:
                        return tif.series[0].shape

                def get_tile_size_from_config_toml(log_dict) -> list[int]:
                    X_size = log_dict["config_toml"]["tile_specs"][
                        "row_count_pixels"
                    ]
                    Y_size = log_dict["config_toml"]["tile_specs"][
                        "column_count_pixels"
                    ]
                    Z_size = int(
                        log_dict["config_toml"]["imaging_specs"]["volume_z_um"]
                        / log_dict["config_toml"]["imaging_specs"][
                            "z_step_size_um"
                        ]
                    )
                    return [Z_size, Y_size, X_size]

                # could get shape by reading file directly

                # shape: list[int] =  file_io.read_json(metadata_path_res)["shape"]  # 5 ints: [t, c, z, y, x]

                shape: list[int] = [
                    1,
                    1,
                    get_tile_size_from_config_toml(log_dict),
                ]

                x.text = f"{shape[2][2]} {shape[2][1]} {shape[2][0]}"  # XYZ for BDV, ZYX for Zarr

                voxel_size = ET.SubElement(vs, "voxelSize")
                x = ET.SubElement(voxel_size, "unit")
                x.text = "µm"
                x = ET.SubElement(voxel_size, "size")

                scale_vector = (
                    acq_obj.tiles[0].coordinate_transformations[0].scale
                )
                x.text = (
                    f"{scale_vector[0]} {scale_vector[1]} {scale_vector[2]}"
                )

                attr = ET.SubElement(vs, "attributes")
                x = ET.SubElement(attr, "illumination")
                x.text = "0"
                x = ET.SubElement(attr, "channel")
                x.text = "0"
                x = ET.SubElement(attr, "tile")
                x.text = f"{i}"
                x = ET.SubElement(attr, "angle")
                x.text = str(acq_obj.tiles[0].imaging_angle)

            add_attributes(view_setups, log_dict)

        def add_time_points(seq_desc: ET.Element) -> None:
            x = ET.SubElement(seq_desc, "Timepoints")
            x.attrib["type"] = "pattern"
            y = ET.SubElement(x, "integerpattern")
            y.text = "0"
            ET.SubElement(seq_desc, "MissingViews")

        seq_desc = ET.SubElement(parent, "SequenceDescription")
        add_image_loader(seq_desc, filtered_tiles)
        add_view_setups(seq_desc, log_dict, filtered_tiles)
        add_time_points(seq_desc)

    # Define transformations applied to tiles
    def add_view_registrations(
        parent: ET.Element, filtered_translations: list[list[float]], log_dict : dict
    ) -> None:
        view_registrations = ET.SubElement(parent, "ViewRegistrations")
        for i, tr in enumerate(filtered_translations):
            vr = ET.SubElement(view_registrations, "ViewRegistration")
            vr.attrib["timepoint"] = "0"
            vr.attrib["setup"] = f"{i}"

            vt = ET.Element("ViewTransform")
            vt.attrib["type"] = "affine"
            name = ET.SubElement(vt, "Name")
            name.text = "Translation to Nominal Grid"
            affine = ET.SubElement(vt, "affine")

            # TODO THERE IS AN ACQUISITION BUG where X and Y are switched between acqusition and bigstitcher
            # WHEN THIS BUG IS FIXED, this should go back to tr 0 1 2

            # affine.text = f"1.0 0.0 0.0 {tr[1]} 0.0 1.0 0.0 {tr[0]} 0.0 0.0 1.0 {tr[2]+tr[1]}"
            x = float(tr[0])
            y = float(tr[1])
            z = float(tr[2])

            x_prime = x
            y_prime = y

            y_voxel_size = float(log_dict['tiles'][0]['y_voxel_size'])
            z_voxel_size = float(log_dict['tiles'][0]['z_voxel_size'])

            #TODO REFactor this to be more general
            z_prime = z + (y*y_voxel_size/np.sqrt(2))/z_voxel_size #convert y pixels to z pixels


            affine.text = f"1.0 0.0 0.0 {str(y_prime)} 0.0 1.0 0.0 {str(x_prime)} 0.0 0.0 1.0 {str(z_prime)}"


            vr.append(vt)

    # Gather info from acq json
    tile_translations: dict[str, list[float]] = extract_tile_translation()
    filtered_tiles: list[str] = list(tile_translations.keys())
    filtered_translations: list[list[float]] = list(tile_translations.values())
    acquisition_style = log_dict["config_toml"]["imaging_specs"][
        "acquisition_style"
    ]
    if zarr and condition != "" or acquisition_style == "interleaved":
        filtered_tiles, new_filtered_tilenames = filter_tiles_on_condition(
            condition, acquisition_style=acquisition_style
        )

    # if interleaved, we cannot filter the tiles on the condition, because each tile contains multiple channels
    # therefore we just apply the translation to all tiles when generating the xml

    # Round translations to 4 decimal places
    filtered_translations = []
    for tile in filtered_tiles:
        rounded_translations = [round(x, 4) for x in tile_translations[tile]]
        filtered_translations.append(rounded_translations)

    # Construct the output xml
    spim_data = ET.Element("SpimData")
    spim_data.attrib["version"] = "0.2"
    x = ET.SubElement(spim_data, "BasePath")
    x.attrib["type"] = "relative"
    x.text = "."
    # add_sequence_description(spim_data,log_dict, filtered_tiles)
    if acquisition_style == "interleaved":
        add_sequence_description(spim_data, log_dict, new_filtered_tilenames)
    else:
        add_sequence_description(spim_data, log_dict, filtered_tiles)
    add_view_registrations(spim_data, filtered_translations, log_dict)

    return ET.ElementTree(spim_data)
