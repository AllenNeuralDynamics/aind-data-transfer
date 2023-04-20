from datetime import datetime
import xml.etree.ElementTree as ET
import pandas as pd

import file_io

from aind_data_schema.imaging.acquisition import AxisName, Direction, Axis, Immersion, Acquisition, AcquisitionTile
from aind_data_schema.imaging.tile import Channel, Scale3dTransform, Translation3dTransform

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

    tiles: list[AcquisitionTile] = []
    for tile_dict in log_dict['tiles']:
        ch = Channel(channel_name=tile_dict['channel_name'], 
                     laser_wavelength=int(tile_dict['laser_wavelength']), 
                     laser_power=tile_dict['laser_power'],
                     filter_wheel_index=tile_dict['filter_wheel_index'])
        scale_tfm = Scale3dTransform(scale=[float(tile_dict['x_voxel_size']), 
                                            float(tile_dict['y_voxel_size']), 
                                            float(tile_dict['z_voxel_size'])])
        translation_tfm = Translation3dTransform(translation=
                        [float(tile_dict['tile_x_position']) * 1000. / float(tile_dict['x_voxel_size']), 
                        float(tile_dict['tile_y_position']) * 1000. / float(tile_dict['y_voxel_size']),
                        float(tile_dict['tile_z_position']) * 1000. / float(tile_dict['z_voxel_size'])])
        tile = AcquisitionTile(channel=ch, 
                               file_name=tile_dict['file_name'],
                               imaging_angle=tile_dict['lightsheet_angle'], 
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
    local_storage_directory: str = log_dict['local_storage_directory']
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

def acq_json_to_xml(acq_obj: Acquisition, s3_path: str, zarr: bool = True, condition: str = "") -> ET.ElementTree:
    """
    Parameters
    ----------
    acq_obj: Acquisition
        Acquisition instance
    s3_path: str
        S3 URI to dataset
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

        for tile in acq_obj['tiles']:
            translation: list[float] = tile['coordinate_transformations'][1]['translation']
            filename: str = tile['file_name']

            tile_transforms[filename] = translation

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
        df = pd.DataFrame(fields, index=[])
        for tile in acq_obj['tiles']: 
            filename: str = tile['file_name']
            vals = filename.split('_')
            X = int(vals[2])
            Y = int(vals[4])
            Z = int(vals[6])
            
            if len(vals[8]) <= 4: 
                channel = int(vals[8])
                cam = int(vals[9][-1])
            else:
                channel = int(vals[8][:vals[8].index('.')])
                cam = -1

            df.append({'X': X, 
                       'Y': Y, 
                       'Z': Z,
                       'channel': channel,
                       'cam': cam}, ignore_index=True)

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

                # FIXME: Test later
                x.text = 'home/TODO_ADD_USER/' + s3_path[s3_path.index('//') + 2:]

                zgs = ET.SubElement(img_loader, "zgroups")
                for i, tile in enumerate(filtered_tiles):
                    zg = ET.SubElement(zgs, "zgroup")
                    zg.attrib["setup"] = f"{i}"
                    zg.attrib["timepoint"] = "0"
                    x = ET.SubElement(zg, "path")
                    x.text = tile
                    
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
                    name_entry.text = tile
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
                x.text = tile
                x = ET.SubElement(vs, "size")
                
                # FIXME: Test later
                # metadata_path_res = 'home/TODO_ADD_USER/' + s3_path[s3_path.index('//') + 2:] + '0/.zarray'
                # shape: list[int] = file_io.read_json(metadata_path_res)["shape"]  # 5 ints: [t, c, z, y, x]
                # x.text = f"{shape[4]} {shape[3]} {shape[2]}"  
                x.text = f"TODO TODO TODO"

                voxel_size = ET.SubElement(vs, "voxelSize")
                x = ET.SubElement(voxel_size, "unit")
                x.text = "µm"
                x = ET.SubElement(voxel_size, "size")

                scale_vector = acq_obj["tiles"][0]['coordinate_transformations'][0]['scale']
                x.text = f"{scale_vector[0]} {scale_vector[1]} {scale_vector[2]}"

                attr = ET.SubElement(vs, "attributes")
                x = ET.SubElement(attr, "illumination")
                x.text = "0"
                x = ET.SubElement(attr, "channel")
                x.text = "0"
                x = ET.SubElement(attr, "tile")
                x.text = f"{i}"
                x = ET.SubElement(attr, "angle")
                x.text = "0"

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
            affine.text = f'0.0 0.0 0.0 {tr[0]} 0.0 0.0 0.0 {tr[1]} 0.0 0.0 0.0 {tr[2]}'

            vr.append(vt)

    # Gather info from acq json
    tile_translations: dict[str, list[float]] = extract_tile_translation()
    filtered_tiles: list[str] = list(tile_translations.keys())
    filtered_translations: list[list[float]] = list(tile_translations.values())
    if zarr and condition != "":
        filtered_tiles = filter_tiles_on_condition(condition)
        filtered_translations = tile_translations[filtered_tiles]

    # Construct the output xml
    spim_data = ET.Element("SpimData")
    spim_data.attrib["version"] = "0.2"
    x = ET.SubElement(spim_data, "BasePath")
    x.attrib["type"] = "relative"
    x.text = "."
    add_sequence_description(spim_data, filtered_tiles)
    add_view_registrations(spim_data, filtered_translations)

    return ET.ElementTree(spim_data)