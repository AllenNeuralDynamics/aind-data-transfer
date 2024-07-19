import json
import aind_data_schema
from pathlib import Path
import os


from aind_data_schema.imaging.acquisition import (
    AxisName,
    Direction,
    Axis,
    Immersion,
    Acquisition,
    AcquisitionTile,
)
# from aind_data_schema.models.coordinates import AnatomicalDirection, AxisName, ImageAxis

from aind_data_schema.imaging.tile import (
    Channel,
    Scale3dTransform,
    Translation3dTransform,
)
from czitools import metadata_tools as czimd

from datetime import datetime

ZEISS_IMAGING_ANGLE = 90

################################################
#   Functions for zeiss LS 7 Acquisition.json 
################################################


def get_subject_id_from_dataset_loc(dataset_loc):
    subject_id = dataset_loc.name.split('_')[1]
    # assert len(subject_id) == 6
    if len(subject_id)!=6:
        print(f"Warning! Subject_id: {subject_id} in {dataset_loc} is not 6 characters long. Please check the subject_id.")
    return subject_id

def get_instrument_id_from_czi_mdata(mdata):
    instrument_id = "Zeiss " + mdata.czi_box.ImageDocument.Metadata.Information.Instrument.Microscopes.Microscope.System + ", " + mdata.czi_box.ImageDocument.Metadata.Information.Instrument.Microscopes.Microscope.Id
    return instrument_id

def get_session_start_time_from_czi_mdata(mdata):
    session_start_time = mdata.czi_box.ImageDocument.Metadata.Information.Image.Session.SessionName
    start_time = session_start_time.split(' ')[2]
    session_start_time = datetime.strptime(start_time, '%Y%m%d_%H%M%S')
    print(session_start_time)
    return session_start_time

def get_excitation_power_for_channel(mdata, channel_index):

    mw_max_power = int(get_lightsource_name(mdata, channel_index)[-2:])    
    power_fraction = float(get_lightsource_attenuation(mdata, channel_index))

    return power_fraction * mw_max_power / 100

def get_excitation_wavelength_for_channel(mdata, channel_index):
    #depreciated
    excitation_wavelength = float(mdata.czi_box.ImageDocument.Metadata.Information.Instrument.LightSources.LightSource[channel_index].LightSourceType.Laser.Wavelength)
    return excitation_wavelength


def get_filter_names(mdata):
    filter_names = []
    filter_mdata = mdata.czi_box.ImageDocument.Metadata.Information.Instrument.Filters.Filter
    if not isinstance(filter_mdata, list):
        filter_mdata = [filter_mdata]
    for i in range(len(filter_mdata)):
        filter_names.append(filter_mdata[i].Name)
    return filter_names

def get_filter_id_and_names(mdata):
    # write out the dict of filter id and names
    filter_id_and_names = {}
    for i in range(len(mdata.czi_box.ImageDocument.Metadata.Information.Instrument.Filters.Filter)):
        filter_id = mdata.czi_box.ImageDocument.Metadata.Information.Instrument.Filters.Filter[i].Id
        filter_name = mdata.czi_box.ImageDocument.Metadata.Information.Instrument.Filters.Filter[i].Name
        filter_id_and_names[filter_id] = filter_name
    return filter_id_and_names


def get_detector_name(mdata, channel_number):
    detector_mdata = mdata.czi_box.ImageDocument.Metadata.Information.Instrument.Detectors.Detector

    if not isinstance(detector_mdata, list):
        detector_model = detector_mdata.Manufacturer.Model
        detector_type = detector_mdata.Type
    else:
        detector_model = detector_mdata[channel_number].Manufacturer.Model
        detector_type =detector_mdata[channel_number].Type
        
    channel_mdata = mdata.czi_box.ImageDocument.Metadata.Information.Image.Dimensions.Channels.Channel
    if not isinstance(channel_mdata, list):
        detector_id = channel_mdata.DetectorSettings.Detector['@Id']
    else:
        detector_id = channel_mdata[channel_number].DetectorSettings.Detector['@Id']

    detector_name = detector_id + ', ' +  detector_model + ', ' + detector_type

    return detector_name



from aicspylibczi import CziFile
M_TO_UM = 1e6

def get_tile_position_um(mdata):

    czi = CziFile(mdata.filepath)
    bbox = czi.get_scene_bounding_box(0)

    X_resolution = float(dict(mdata.czi_box.ImageDocument.Metadata.Experiment.ExperimentBlocks.AcquisitionBlock.AcquisitionModeSetup)['ScalingX'])
    Y_resolution = float(dict(mdata.czi_box.ImageDocument.Metadata.Experiment.ExperimentBlocks.AcquisitionBlock.AcquisitionModeSetup)['ScalingY'])
    Z_resolution = float(dict(mdata.czi_box.ImageDocument.Metadata.Experiment.ExperimentBlocks.AcquisitionBlock.AcquisitionModeSetup)['ScalingZ'])

    pixel_resolution = [X_resolution, Y_resolution, Z_resolution]

    mdata_position = mdata.czi_box.ImageDocument.Metadata.Information.Image.Dimensions.S.Scenes.Scene.Positions.Position

    z_position_px = float(mdata_position['@Z'])/(Z_resolution*M_TO_UM) 
    czi_position = [bbox.x, bbox.y, z_position_px]


    # print(f'czi_position: {czi_position}') #pixels 
    czi_position_um = [position*resolution*1e6 for position, resolution in zip(czi_position, pixel_resolution)]
    return czi_position_um


def get_tile_resolution(mdata):
    #resolution in um
    X_resolution = float(dict(mdata.czi_box.ImageDocument.Metadata.Experiment.ExperimentBlocks.AcquisitionBlock.AcquisitionModeSetup)['ScalingX'])
    Y_resolution = float(dict(mdata.czi_box.ImageDocument.Metadata.Experiment.ExperimentBlocks.AcquisitionBlock.AcquisitionModeSetup)['ScalingY'])
    Z_resolution = float(dict(mdata.czi_box.ImageDocument.Metadata.Experiment.ExperimentBlocks.AcquisitionBlock.AcquisitionModeSetup)['ScalingZ'])

    pixel_resolution = [X_resolution, Y_resolution, Z_resolution]
    return pixel_resolution

def get_tiff_tile_name(mdata, tile_index, list_of_tiles):
    czi = CziFile(mdata.filepath)
    bbox = czi.get_scene_bounding_box(0)



    # if tile_index == 0:
        
    def find_min_x_y(list_of_tiles):
        for i, dataset_fp in enumerate(list_of_tiles):
            try: 
                czi = CziFile(dataset_fp)
            except:
                print(f'Error reading {dataset_fp}')
                continue
            bbox = czi.get_scene_bounding_box(0)
            if i == 0:
                min_x = bbox.x
                min_y = bbox.y
            else:
                if bbox.x < min_x:
                    min_x = bbox.x
                if bbox.y < min_y:
                    min_y = bbox.y

        return min_x, min_y
    init_x, init_y = find_min_x_y(list_of_tiles)


    try: 
        percent_overlap = float((mdata.czi_box.ImageDocument.Metadata.Experiment.ExperimentBlocks.AcquisitionBlock.TilesSetup.PositionGroups.PositionGroup)['TileAcquisitionOverlap'])
    except:
        percent_overlap = 0
    sh = czi.get_dims_shape()[0]
    # size = [sh['X'][1], sh['Y'][1], sh['Z'][1]]   

    intertile_distance_pixels = sh['X'][1]*(1-percent_overlap)
    x_name = round((bbox.x-init_x)/intertile_distance_pixels)
    y_name = round((bbox.y-init_y)/intertile_distance_pixels)
    z_name = 0

    round_number = 'R' # TODO add some logic to determine round number

    channel_number = int(czi.read_subblock_metadata(Z = 0)[0][0]['C'])
    print(f'channel_number: {channel_number}')

    #channel wavelength 
    channel_wavelength = get_channel_wavelength(mdata, channel_number)
    #int(float(mdata.image.czisource.ImageDocument.Metadata.Information.Image.Dimensions.Channels.Channel[channel_number].IlluminationWavelength.SinglePeak))

    tile_name = f'{round_number}_X_{x_name:04}_Y_{y_name:04}_Z_{z_name:04}_ch_{channel_wavelength}.tiff'

    return tile_name

def get_exposure_time_ms(mdata):
    channel_mdata = mdata.image.czisource.ImageDocument.Metadata.Information.Image.Dimensions.Channels.Channel

    if isinstance(channel_mdata, list):
        exposure_time_ms = float(channel_mdata[0].ExposureTime)/1e6
    else:
        exposure_time_ms = float(channel_mdata.ExposureTime)/1e6
    return exposure_time_ms

def get_zoom(mdata):
    detector_mdata = mdata.image.czisource.ImageDocument.Metadata.Information.Instrument.Detectors.Detector
    if isinstance(detector_mdata, list):
        zoom = detector_mdata[0].Zoom
    else:
        zoom = detector_mdata.Zoom
    return zoom 

def is_filtered_by(filter_id, wavelength):
    # filter_lookup = {'0:0:0': {'low': 0, 'high': 490}, '1:0:0': {'low': 505, 'high': 545}, '1:1:0': {'low': 660, 'high': 20000}} #these are actual values
    filter_lookup = {'0:0:0':       {'low': 0, 'high': 450},      #SP 490, this is for 405
                     'SP 490':      {'low': 0, 'high': 450},  

                     '1:0:0':       {'low': 460, 'high': 545},    #BP 505-545, this is for 488
                     'BP 505-545':  {'low': 460, 'high': 545},

                     '0:1:0':       {'low': 550, 'high': 600},    #BP 575-615, this is for 561
                     'BP 575-615':  {'low': 550, 'high': 600},

                     '1:1:0':       {'low': 600, 'high': 20000},   #LP 660, this is for 638 
                     'LP 660':      {'low': 600, 'high': 20000}}


    buffer = 0 #nm

    if wavelength >= filter_lookup[filter_id]['low']-buffer and wavelength <= filter_lookup[filter_id]['high']+buffer:
        return False #the wavelength is not filtered
    else:
        return True
    
def get_all_channels_in_LightSourcesSettings_list(mdata, channel_number):
    channel_mdata = mdata.image.czisource.ImageDocument.Metadata.Information.Image.Dimensions.Channels.Channel

    if isinstance(channel_mdata, list):
        list_of_lightsourcessettings = channel_mdata[channel_number].LightSourcesSettings.LightSourceSettings
    else:
        list_of_lightsourcessettings = channel_mdata.LightSourcesSettings.LightSourceSettings

    if isinstance(list_of_lightsourcessettings, list):
        list_of_channel_numbers = []
        for lightsourcesettings in list_of_lightsourcessettings:
            channel_number = int(float(lightsourcesettings.Wavelength))
            list_of_channel_numbers.append(channel_number)
    else: 
        list_of_channel_numbers = [int(float(list_of_lightsourcessettings.Wavelength))]

    return list_of_channel_numbers

def get_filter_name_for_channel_number(mdata, channel_number):
    channel_mdata = mdata.image.czisource.ImageDocument.Metadata.Information.Image.Dimensions.Channels.Channel

    if isinstance(channel_mdata, list):
        filter_id_for_channel_number = channel_mdata[channel_number].FilterSet['@Id'][-5:]
    else:
        filter_id_for_channel_number = channel_mdata.FilterSet['@Id'][-5:]
    

    list_of_filters = mdata.image.czisource.ImageDocument.Metadata.Information.Instrument.Filters.Filter

    if not isinstance(list_of_filters, list):
        list_of_filters = [list_of_filters]

    for filter in list_of_filters:
        if filter['@Id'][-5:] == filter_id_for_channel_number:
            return filter['@Name']


def get_channel_wavelength(mdata, channel_number):
    list_of_channels = get_all_channels_in_LightSourcesSettings_list(mdata, channel_number)

    filter_for_channel_number = get_filter_name_for_channel_number(mdata, channel_number)

    for channel in list_of_channels:
        if is_filtered_by(filter_for_channel_number, channel):
            continue
        else:
            return channel

def get_lightsource_name(mdata, channel_number):
    list_of_channels = get_all_channels_in_LightSourcesSettings_list(mdata, channel_number)

    filter_for_channel_number = get_filter_name_for_channel_number(mdata, channel_number)

    for channel in list_of_channels:
        if is_filtered_by(filter_for_channel_number, channel):
            continue
        else:
            #get lightsource id from wavelength
            channel_mdata = mdata.image.czisource.ImageDocument.Metadata.Information.Image.Dimensions.Channels.Channel

            if isinstance(channel_mdata, list):
                list_of_lightsourcessettings = channel_mdata[channel_number].LightSourcesSettings.LightSourceSettings
            else:
                list_of_lightsourcessettings = channel_mdata.LightSourcesSettings.LightSourceSettings

            if isinstance(list_of_lightsourcessettings, list):
                for lightsourcesettings in list_of_lightsourcessettings:
                    if int(float(lightsourcesettings.Wavelength)) == channel:
                        light_source_id = int(lightsourcesettings.LightSource['@Id'][-1])
                        lightsource_name =  mdata.czi_box.ImageDocument.Metadata.Information.Instrument.LightSources.LightSource[light_source_id].Manufacturer.Model
                        return lightsource_name
                    else:
                        continue
            else:
                light_source_id = int(list_of_lightsourcessettings.LightSource['@Id'][-1])
                lightsource_name =  mdata.czi_box.ImageDocument.Metadata.Information.Instrument.LightSources.LightSource[light_source_id].Manufacturer.Model
                return lightsource_name
        


def get_lightsource_attenuation(mdata, channel_number):
    list_of_channels = get_all_channels_in_LightSourcesSettings_list(mdata, channel_number)

    filter_for_channel_number = get_filter_name_for_channel_number(mdata, channel_number)

    for channel in list_of_channels:
        if is_filtered_by(filter_for_channel_number, channel):
            continue
        else:
            #get lightsource id from wavelength
            channel_mdata = mdata.image.czisource.ImageDocument.Metadata.Information.Image.Dimensions.Channels.Channel

            if isinstance(channel_mdata, list):
                list_of_lightsourcessettings = channel_mdata[channel_number].LightSourcesSettings.LightSourceSettings
            else:
                list_of_lightsourcessettings = channel_mdata.LightSourcesSettings.LightSourceSettings            
            if isinstance(list_of_lightsourcessettings, list):
                for lightsourcesettings in list_of_lightsourcessettings:
                    if int(float(lightsourcesettings.Wavelength)) == channel:
                        attenuation = float(lightsourcesettings['Attenuation'])
                        break
                    else:
                        continue
            else:
                attenuation = float(list_of_lightsourcessettings['Attenuation'])
                
            return 1-attenuation

def get_schema_AcquisitionTile(mdata, tile_index, list_of_tiles):

    tile_position_um = get_tile_position_um(mdata)
    translation_tfm = Translation3dTransform(translation=tile_position_um) #XYZ format
    
    tile_resolution = get_tile_resolution(mdata)
    scale_tfm = Scale3dTransform(scale=[float(resolution) for resolution in tile_resolution])
    
    czi = CziFile(mdata.filepath)
    channel_number = int(czi.read_subblock_metadata(Z = 0)[0][0]['C'])
    print(f'fp {mdata.filepath} channel_number: {channel_number}')

    #laser power between data blocks

    #make a dictionary with the channel name being the value
    channel_name = get_channel_wavelength(mdata, channel_number)
    print(f'channel_name: {channel_name}')


    light_source_name = get_lightsource_name(mdata, channel_number) #mdata.czi_box.ImageDocument.Metadata.Information.Instrument.LightSources.LightSource[0].Manufacturer.Model
    filter_names = get_filter_names(mdata)
    detector_name = get_detector_name(mdata, channel_number)
    additional_device_names = []

    excitation_wavelength = channel_name #get_excitation_wavelength_for_channel(mdata, channel_number)
    excitation_wavelength_unit = 'nanometer'
    
    # for g in range(0, 3):
    #     excitation_power = get_excitation_power_for_channel(mdata, g)
    #     print(f'excitation_power: {excitation_power}')

    excitation_power = get_excitation_power_for_channel(mdata, channel_number)
    excitation_power_unit = 'milliwatt'

    filter_wheel_index = channel_number #mdata.czi_box.ImageDocument.Metadata.Information.Instrument.Filters.Filter[0].Id # may have to just put index here 

    dilation = None
    dilation_unit = "pixel"
    description = ""

    tiff_filename = get_tiff_tile_name(mdata, tile_index, list_of_tiles)



    ch = Channel(
        channel_name=channel_name,
        light_source_name=light_source_name,
        filter_names=filter_names,
        detector_name=detector_name,
        additional_device_names=additional_device_names,
        excitation_wavelength=excitation_wavelength,
        excitation_wavelength_unit=excitation_wavelength_unit,
        excitation_power=excitation_power,
        excitation_power_unit=excitation_power_unit,
        filter_wheel_index=filter_wheel_index,
        dilation=dilation,
        dilation_unit=dilation_unit,
        description=description
    )

    tile = AcquisitionTile(channel=ch, 
                            file_name=tiff_filename, #this should be the zarr file name
                            imaging_angle=ZEISS_IMAGING_ANGLE, 
                            coordinate_transformations=[scale_tfm, translation_tfm])
    
    return tile


def get_image_axis():
    axes: list[Axis] = []
    axes.append(Axis(name=AxisName.X, dimension=2, direction=Direction.AP))
    axes.append(Axis(name=AxisName.Y, dimension=1, direction=Direction.LR))
    axes.append(Axis(name=AxisName.Z, dimension=0, direction=Direction.IS))

    return axes
        

def make_acquisition_schema(czi_loc):
    """Acquisition schema object for czi files

    Args:
        czi_loc (Path): Path to the czi files
    Returns:
        Acquisition: Acquisition schema object
    """
    tiles = []
    # czi_loc = Path('/allen/aind/stage/Z1/Christian/probe_characterization/Syto61')

    list_of_tiles = sorted(list(Path(czi_loc).glob('*.czi')))

    #first mdata
    mdata = czimd.CziMetadata(list_of_tiles[0].as_posix())

    tiff_filename = get_tiff_tile_name(mdata, 0, list_of_tiles)
    
    specimen_id = get_subject_id_from_dataset_loc(Path(tiff_filename))
    subject_id = get_subject_id_from_dataset_loc(Path(tiff_filename))#mouse id - can get this from title
    instrument_id = get_instrument_id_from_czi_mdata(mdata)
    calibrations = []
    maintenance = []
    session_start_time = get_session_start_time_from_czi_mdata(mdata)
    session_end_time = get_session_start_time_from_czi_mdata(mdata)
    axes = get_image_axis()

    chamber_immersion = Immersion(medium = mdata.czi_box.ImageDocument.Metadata.Information.Image.ObjectiveSettings.Medium, refractive_index = 1.33) #may not be accurate
    sample_immersion=chamber_immersion
    active_objectives = [mdata.czi_box.ImageDocument.Metadata.Information.Instrument.Objectives.Objective.Manufacturer.Model]
    local_storage_directory = mdata.filepath
    external_storage_directory = mdata.filepath
    processing_steps = []
    exposure_time_ms = get_exposure_time_ms(mdata)
    
    zoom = get_zoom(mdata)


    notes = f"""Exposure time: {exposure_time_ms} ms
                Zoom: {zoom}"""


    for tile_index, czi_file in enumerate(list_of_tiles):
        czi_file = czi_file.as_posix()
        mdata = czimd.CziMetadata(czi_file)
        tile = get_schema_AcquisitionTile(mdata, tile_index, list_of_tiles)
        tiles.append(tile)


    acquisition = Acquisition(
        specimen_id=specimen_id,
        subject_id=subject_id,
        instrument_id=instrument_id,
        calibrations=calibrations,
        maintenance=maintenance,
        session_start_time=session_start_time,
        session_end_time=session_end_time,
        tiles=tiles,
        axes=axes,
        chamber_immersion=chamber_immersion,
        sample_immersion=sample_immersion,
        active_objectives=active_objectives,
        local_storage_directory=local_storage_directory,
        external_storage_directory=external_storage_directory,
        processing_steps=processing_steps,
        notes=notes,
        experimenter_full_name = ['null'],
    )
    return acquisition




def write_acq_json(acq_obj: Acquisition, acq_json_path: str) -> None:
    """
    Parameters
    ----------
    acq_obj: Acquisition
        Acquisition instance
    acq_json_path: str
        Path to output json file
    """
    # convert session_end_time and session_start_time to isoformat
    # acq_obj.session_start_time = acq_obj.session_start_time.isoformat()
    # acq_obj.session_end_time = acq_obj.session_end_time.isoformat()

    with open(acq_json_path, "w") as f:
        json.dump(json.loads(acq_obj.json()), f, indent=4)


if __name__ == "__main__":
    print(f'null')
    # czi_loc = Path('/allen/aind/stage/Z1/Christian/probe_characterization/Syto61')
    # acq_obj = make_acquisition_schema(czi_loc)
    # acq_json_path = '/allen/aind/stage/Z1/Christian/probe_characterization/Syto61/acquisition.json'
    # write_acq_json(acq_obj, acq_json_path)
    # print(f"Acquisition json file saved at {acq_json_path}