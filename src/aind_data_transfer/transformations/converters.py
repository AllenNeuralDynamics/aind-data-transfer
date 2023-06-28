from datetime import datetime
import xml.etree.ElementTree as ET
import pandas as pd

import aind_data_transfer.transformations.file_io as file_io

from aind_data_schema.imaging.acquisition import AxisName, Direction, Axis, Immersion, Acquisition, AcquisitionTile
from aind_data_schema.imaging.tile import Channel, Scale3dTransform, Translation3dTransform
from datetime import datetime
import tifffile
import re
import pathlib

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

    experimenter_full_name: str = 'ISpim Group'
    specimen_id: str = log_dict['specimen_id']
    subject_id: str = log_dict['subject_id']
    instrument_id: str = log_dict['instrument_id']
    session_start_time: datetime = log_dict['session_start_time']
    session_end_time: datetime = log_dict['session_end_time']

    #convert session start and end times to datetime objects
    session_start_time = datetime.strptime(session_start_time, '%Y-%m-%dT%H:%M:%S')
    session_end_time = datetime.strptime(session_end_time, '%Y,%m,%d,%H,%M,%S') #the imaging_log file may change this format



    tiles: list[AcquisitionTile] = []
    for tile_dict in log_dict['tiles'].values():
        scale_tfm = Scale3dTransform(scale=[float(log_dict['config_toml']['tile_specs']['x_field_of_view_um']/log_dict['config_toml']['tile_specs']['row_count_pixels']
), 
                                            float(log_dict['config_toml']['tile_specs']['y_field_of_view_um']/log_dict['config_toml']['tile_specs']['column_count_pixels']
), 
                                            float(log_dict['z_voxel_size'])])
        translation_tfm = Translation3dTransform(translation=
                        [float(tile_dict['tile_x_position']),  
                        float(tile_dict['tile_y_position']),
                        float(tile_dict['tile_z_position'])])
        channel_dict = log_dict['channels'][tile_dict['channel']]
        ch = Channel(channel_name=tile_dict['channel'], 
                    laser_wavelength=int(channel_dict['laser_wavelength']), 
                    laser_power=channel_dict['laser_power'],
                    filter_wheel_index=channel_dict['filter_wheel_index'])

        tile = AcquisitionTile(channel=ch, 
                               file_name=tile_dict['file_name'],
                               imaging_angle=log_dict['lightsheet_angle'], 
                               coordinate_transformations=[scale_tfm, translation_tfm])
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

    chamber_immersion: Immersion = Immersion(medium=log_dict['chamber_immersion_medium'], 
                                             refractive_index=log_dict['chamber_immersion_refractive_index'])
    
    local_storage_directory: str = log_dict['local_storage_directory'] #should come from config.yml (in case of files being moved before uploading to s3)
    #assert local_storage_directory is accessible and exists
    if not pathlib.Path(local_storage_directory).exists():
        local_storage_directory = log_dict['data_src_dir']
    
    external_storage_directory: str = log_dict['external_storage_directory']

    return Acquisition(experimenter_full_name=experimenter_full_name, 
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



def acq_json_to_xml(acq_obj: Acquisition, data_loc: str, zarr: bool = True, condition: str = "") -> ET.ElementTree:
    """
    Parameters
    ----------
    acq_obj: Acquisition
        Acquisition instance
    data_loc: str
        Relative path to dataset in linked Code Ocean data/dir
    zarr: bool
        Boolean that describes the type of the dataset: zarr or n5. 
        If n5, conditioning the dataset is turned off. 
    condition: str
        Conditional statement for filtering the dataset on specific attributes.
        Can filter on following 5 fields: X, Y, Z, channel, camera
        Ex: "channel == 405 and camera = 0"    

    Returns
    -------
    ET.ElementTree: 
        ET.ElementTree instance
    """
    
    # Retrieve map of tile names -> transforms
    def extract_tile_translation() -> dict[str, list[float]]:
        tile_transforms: dict[str, list[float]] = {}

        for tile in acq_obj.tiles:

            #zero the translations from the first tile
            if tile == acq_obj.tiles[0]:
                [offset_z, offset_y, offset_x] = [i/j for i, j in zip(tile.coordinate_transformations[1].translation, tile.coordinate_transformations[0].scale)]
                offset = [offset_z, offset_y, offset_x]

            translation: list[float] = [i/j for i, j in zip(tile.coordinate_transformations[1].translation, tile.coordinate_transformations[0].scale)] #this needs to be in pixels for the xml. Not microns. 

            filename: str = tile.file_name

            tile_transforms[filename] = [el1 - el2 for el1, el2 in zip(translation, offset)]

        return tile_transforms
    
    # Parses tile names into a pandas dataframe and 
    # applies boolean masking on given str condition. 
    # Returns list of filtered tile names. 
    def filter_tiles_on_condition(condition: str) -> list[str]:
        """
        Condition Field Names: 
        X, Y, Z, channel, camera
        """

        # Construct Dataframe
        fields = {'filename': '', 
                  'X': int(), 
                  'Y': int(),
                  'Z': int(),
                  'channel': int(),
                  'camera': int()}
        df = pd.DataFrame(fields, index=[0])

        for i, tile in enumerate(acq_obj.tiles): 
            filename: str = tile.file_name
            #use regex anchors
            file_parse_regex = r"X_(\d*)_Y_(\d*)_Z_(\d*)_ch_(\d*)[_cam_]*(\d)?"
            vals = re.search(file_parse_regex, filename).groups()

            # vals = filename.split('_')
            X = int(vals[0])
            Y = int(vals[1])
            Z = int(vals[2])
            channel = int(vals[3]) #need to ensure this is the channel
            
            if vals[4] is not None:
                cam = int(vals[4])
            else: 
                cam = 0

            df = pd.concat([df, 
                        pd.DataFrame({
                        'filename': filename,
                        'X': X, 
                       'Y': Y, 
                       'Z': Z,
                       'channel': channel,
                       'camera': cam}, index = [i])])


        # Filter Dataframe
        if condition != "": 
            results = df.query(condition)
        else:
            results = df
        output_tilenames = list(results['filename'])

        return output_tilenames

    # Define filepaths of tiles and associated attributes
    def add_sequence_description(parent: ET.Element, filtered_tiles: list[str]) -> None: 
        def add_image_loader(seq_desc: ET.Element, filtered_tiles: list[str]) -> None:
            img_loader = ET.SubElement(seq_desc, "ImageLoader")
            if zarr: 
                img_loader.attrib["format"] = "bdv.multimg.zarr"
                img_loader.attrib["version"] = "1.0"
                x = ET.SubElement(img_loader, "zarr")
                x.attrib["type"] = "absolute"

                # should be relative path on s3 (from code_ocean)
                x.text = '/data/'+data_loc+'/'

                zgs = ET.SubElement(img_loader, "zgroups")
                for i, tile in enumerate(filtered_tiles):
                    zg = ET.SubElement(zgs, "zgroup")
                    zg.attrib["setup"] = f"{i}"
                    zg.attrib["timepoint"] = "0"
                    x = ET.SubElement(zg, "path")
                    x.text = str(pathlib.Path(tile).stem)+".zarr"
                    
            else:  # n5 
                img_loader.attrib["format"] = "bdv.n5"
                img_loader.attrib["version"] = "1.0"
                x = ET.SubElement(img_loader, "n5")
                x.attrib["type"] = "relative"
                x.text = "dataset.n5"

        def add_view_setups(seq_desc: ET.Element, filtered_tiles: list[str]) -> None: 
            def add_attributes(view_setups: ET.Element) -> None:
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
                #this is going to run prior to uploading the data to s3, so we need to get the shape from the metadata file if available
                # metadata_path_res = s3_path[s3_path.index('//') + 2:] + '0/.zarray'

                def get_tiff_dimensions(tile_path: str) -> list[int]:
                    with tifffile.TiffFile(tile_path) as tif:
                        return tif.series[0].shape
                

                #could get shape by reading file directly 

                # shape: list[int] =  file_io.read_json(metadata_path_res)["shape"]  # 5 ints: [t, c, z, y, x]
                shape: list[int] =  [1,1,get_tiff_dimensions(pathlib.Path(acq_obj.local_storage_directory).joinpath('diSPIM',tile))]


                x.text = f"{shape[2][2]} {shape[2][1]} {shape[2][0]}"  #XYZ for BDV, ZYX for Zarr

                voxel_size = ET.SubElement(vs, "voxelSize")
                x = ET.SubElement(voxel_size, "unit")
                x.text = "µm"
                x = ET.SubElement(voxel_size, "size")

                scale_vector = acq_obj.tiles[0].coordinate_transformations[0].scale
                x.text = f"{scale_vector[0]} {scale_vector[1]} {scale_vector[2]}"

                attr = ET.SubElement(vs, "attributes")
                x = ET.SubElement(attr, "illumination")
                x.text = "0"
                x = ET.SubElement(attr, "channel")
                x.text = "0"
                x = ET.SubElement(attr, "tile")
                x.text = f"{i}"
                x = ET.SubElement(attr, "angle")
                x.text = str(acq_obj.tiles[0].imaging_angle)

            add_attributes(view_setups)

        def add_time_points(seq_desc: ET.Element) -> None: 
            x = ET.SubElement(seq_desc, "Timepoints")
            x.attrib["type"] = "pattern"
            y = ET.SubElement(x, "integerpattern")
            y.text = "0"
            ET.SubElement(seq_desc, "MissingViews")

        seq_desc = ET.SubElement(parent, "SequenceDescription")
        add_image_loader(seq_desc, filtered_tiles)
        add_view_setups(seq_desc, filtered_tiles)
        add_time_points(seq_desc)

    # Define transformations applied to tiles
    def add_view_registrations(parent: ET.Element, filtered_translations: list[list[float]]) -> None:
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
            affine.text = f'1.0 0.0 0.0 {tr[0]} 0.0 1.0 0.0 {tr[1]} 0.0 0.0 1.0 {tr[2]}'

            vr.append(vt)

    # Gather info from acq json
    tile_translations: dict[str, list[float]] = extract_tile_translation()
    filtered_tiles: list[str] = list(tile_translations.keys())
    filtered_translations: list[list[float]] = list(tile_translations.values())
    if zarr and condition != "":
        filtered_tiles = filter_tiles_on_condition(condition)
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
    add_sequence_description(spim_data, filtered_tiles)
    add_view_registrations(spim_data, filtered_translations)

    return ET.ElementTree(spim_data)