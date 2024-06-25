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
 
    mw_max_power = int(mdata.czi_box.ImageDocument.Metadata.Information.Instrument.LightSources.LightSource[channel_index].Manufacturer.Model[-2:])    
    power_percent = int(mdata.czi_box.ImageDocument.Metadata.Information.Instrument.LightSources.LightSource[channel_index].Power)
 
 
    return power_percent * mw_max_power / 100
 
def get_excitation_wavelength_for_channel(mdata, channel_index):
    excitation_wavelength = float(mdata.czi_box.ImageDocument.Metadata.Information.Instrument.LightSources.LightSource[channel_index].LightSourceType.Laser.Wavelength)
    return excitation_wavelength
 
 
def get_filter_names(mdata):
    filter_names = []
    
    filters = mdata.czi_box.ImageDocument.Metadata.Information.Instrument.Filters.Filter
    if isinstance(filters, dict):
        filters = [filters]
    for i in range(len(filters)):
        filter_names.append(filters[i].Name)
    return filter_names
 
def get_detector_name(mdata):
    detector_metadata = mdata.czi_box.ImageDocument.Metadata.Information.Instrument.Detectors.Detector
 
    if isinstance(detector_metadata, list):
        detector_metadata = detector_metadata[0]
   
    detector_model = detector_metadata.Manufacturer.Model
    detector_id = detector_metadata.Id
    detector_type = detector_metadata.Type
 
    detector_name = detector_id + ', ' +  detector_model + ', ' + detector_type
 
    return detector_name
 
def get_detector_id(mdata):
    detector_id = mdata.czi_box.ImageDocument.Metadata.Information.Instrument.Detectors.Detector[0].Id
 
 
    return detector_id
 
 
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
 
 
    percent_overlap = float((mdata.czi_box.ImageDocument.Metadata.Experiment.ExperimentBlocks.AcquisitionBlock.TilesSetup.PositionGroups.PositionGroup)['TileAcquisitionOverlap'])
    sh = czi.get_dims_shape()[0]
    # size = [sh['X'][1], sh['Y'][1], sh['Z'][1]]   
 
    intertile_distance_pixels = sh['X'][1]*(1-percent_overlap)
    x_name = round((bbox.x-init_x)/intertile_distance_pixels)
    y_name = round((bbox.y-init_y)/intertile_distance_pixels)
    z_name = 0
 
    round_number = 'R' # TODO add some logic to determine round number
 
    channel_number = int(czi.read_subblock_metadata(Z = 0)[0][0]['C'])
    print(f'channel_number: {channel_number}')
    channel_wavelength = int(float(get_channel_metadata_from_channel_number(mdata,channel_number).IlluminationWavelength.SinglePeak))
 
    tile_name = f'{round_number}_X_{x_name:04}_Y_{y_name:04}_Z_{z_name:04}_ch_{channel_wavelength}.tiff'
 
    return tile_name
 
def get_channel_metadata_from_channel_number(mdata, channel_number):
    channel_data = mdata.image.czisource.ImageDocument.Metadata.Information.Image.Dimensions.Channels.Channel
    
    if isinstance(channel_data, dict):
        #TODO - Is this modifying mdata, or just within this scope?
        channel_data = [channel_data] #convert to list, expecting channel_number=0 in this case
    
    return channel_data[channel_number]
    
 
def get_schema_AcquisitionTile(mdata, tile_index, list_of_tiles):
 
    tile_position_um = get_tile_position_um(mdata)
    translation_tfm = Translation3dTransform(translation=tile_position_um) #XYZ format
    
    tile_resolution = get_tile_resolution(mdata)
    scale_tfm = Scale3dTransform(scale=[float(resolution) for resolution in tile_resolution])
    
    czi = CziFile(mdata.filepath)
    channel_number = int(czi.read_subblock_metadata(Z = 0)[0][0]['C'])
    channel_name = int(float(get_channel_metadata_from_channel_number(mdata, channel_number).IlluminationWavelength.SinglePeak))
    # channel_name = mdata.czi_box.ImageDocument.Metadata.Information.Instrument.LightSources.LightSource[0].LightSourceType.Laser.Wavelength
    light_source_name = mdata.czi_box.ImageDocument.Metadata.Information.Instrument.LightSources.LightSource[0].Manufacturer.Model
    filter_names = get_filter_names(mdata)
    detector_name = get_detector_name(mdata)
    additional_device_names = []
 
    excitation_wavelength = get_excitation_wavelength_for_channel(mdata, channel_number)
    excitation_wavelength_unit = 'nanometer'
    excitation_power = get_excitation_power_for_channel(mdata, 0)
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
    notes = ""
 
 
    for tile_index, czi_file in enumerate(list_of_tiles):
        czi_file = czi_file.as_posix()
        try: 
            mdata = czimd.CziMetadata(czi_file)
        except:
            print(f'Error reading {czi_file}')
            continue
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