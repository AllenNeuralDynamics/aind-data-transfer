[10:40 AM] Sean McCulloch
#setup some dask workers
import dask
from dask.distributed import Client
import tifffile as tf
# client = Client(n_workers=12, threads_per_worker=2, memory_limit='12GB')
import json
 
from czitools import metadata_tools as czimd
from czitools import read_tools, write_tools
from aicspylibczi import CziFile
import napari
from pathlib import Path
import ome_zarr.reader
import ome_zarr.scale
import ome_zarr.writer
from ome_zarr.io import parse_url
import shutil
import zarr
import requests
import os
from pathlib import Path
from tqdm import tqdm
 
#add the path to the json_to_xml_all_channels.py file
import sys
# sys.path.append('/allen/programs/mindscope/workgroups/omfish/carsonb/czitools/demo/notebooks/json_to_xml_all_channels.py')
 
from aind_data_transfer.transformations.json_to_xml_all_channels import convert_json_to_xml
import argparse
 
from aind_data_transfer.transformations.make_czi_acquisition_json import make_acquisition_schema, write_acq_json
 
def main():
    argparser = argparse.ArgumentParser(description='Convert CZI to tiff')
    argparser.add_argument('--input_folder', type=str, help='input folder containing CZI files')
    argparser.add_argument('--output_folder', type=str, help='output folder to save tiff files')
    argparser.add_argument('--metadata_only', type=str, help='whether to write metadata only', default=False)
 
 
    args = argparser.parse_args()
 
    INPUT_FOLDER = args.input_folder
    OUTPUT_FOLDER = args.output_folder
    METADATA_ONLY = args.metadata_only
 
    print(f'Metadata only: {METADATA_ONLY}')
 
    convert_czi_to_tiff(INPUT_FOLDER, OUTPUT_FOLDER, METADATA_ONLY)
 
# INPUT_FOLDER = '/allen/aind/stage/Z1/Christian/probe_characterization/Gad2/4222024_1'
# OUTPUT_FOLDER = '/allen/aind/stage/Z1/HCR_000000-gad2_2024-04-22_09-00-00/diSPIM'
 
 
def get_channel_metadata_from_channel_number(mdata, channel_number):
    channel_data = mdata.image.czisource.ImageDocument.Metadata.Information.Image.Dimensions.Channels.Channel
    
    if isinstance(channel_data, dict):
        #TODO - Is this modifying mdata, or just within this scope?
        channel_data = [channel_data] #convert to list, expecting channel_number=0 in this case
    
    return channel_data[channel_number]
 
 
def convert_czi_to_tiff(INPUT_FOLDER, OUTPUT_FOLDER, METADATA_ONLY=False):
 
    shape_list = []
    data_type_list = []
 
    invalid_count = 0
    list_of_tiles = list(Path(INPUT_FOLDER).glob('*.czi'))
 
    #raiser error if no czi files are found
    if len(list_of_tiles) == 0:
        raise ValueError('No czi files found in the input folder')
 
 
 
    #order the list of tiles
    list_of_tiles = sorted(list_of_tiles)
    list_of_tile_dicts = []
    list_of_ch_405_tile_dicts = []
 
    #make an acquisition json
    acq_json = make_acquisition_schema(INPUT_FOLDER)
    acq_json_loc = Path(OUTPUT_FOLDER).parent.joinpath('acquisition.json').as_posix()
 
    print(f'Writing acquisition json to {acq_json_loc}')
    write_acq_json(acq_json, acq_json_loc)
 
 
    for i, dataset_fp in tqdm(enumerate(list_of_tiles)):
 
        try:
            mdata = czimd.CziMetadata(dataset_fp)
            # print(f'dataset_fp: {dataset_fp}')
 
            czi = CziFile(dataset_fp)
        except:
            print(f'Error reading {dataset_fp}')
            continue
 
        if METADATA_ONLY in ["False", "false", "FALSE", False]:
            img, shp = czi.read_image(cores = 16)
            img = img[0,0,0,0, 0, 0, 0, 0]
            # print(f'img shape: {img.shape}')
            if img.ndim == 4:
                img = img.squeeze(axis=1)
            print(f'img shape: {img.shape}')
 
        #make new tilename like {round_number}_X_{$.4:x}_Y_{$.4:y}_Z_{$.4:z}_ch_{channel_number}.tiff
 
        #do metadata
        print(f'filename: {dataset_fp}')
 
        channel_number = int(czi.read_subblock_metadata(Z = 0)[0][0]['C'])
        print(f'channel_number: {channel_number}')
 
        #channel wavelength
        channel_wavelength = int(float(get_channel_metadata_from_channel_number(mdata,channel_number).IlluminationWavelength.SinglePeak))
 
        #need to remove negative signs from the relative tile positions
        # in order to this, we need to find not the initial tile position, but the tile at the smallest or most negative position in X and Y (say, the top left tile)
        # then we can subtract the smallest x and y from the x and y of the current tile to get the relative position
        # we can then use this relative position to determine the tile name
 
        #this requires us to write a little function to find the smallest bbox.x and bbox.y if we are on the first tile
 
        #position of the tiles relative to each other
        bbox = czi.get_scene_bounding_box(0)
 
        if i == 0:
            
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
 
 
        # print(f'x {bbox.x}, y {bbox.y}')
        position = [bbox.x, bbox.y, 0]
 
        #tile size
        sh = czi.get_dims_shape()[0]
        size = [sh['X'][1], sh['Y'][1], sh['Z'][1]]    
        # print(f'size: {size}')
 
        percent_overlap = float((mdata.czi_box.ImageDocument.Metadata.Experiment.ExperimentBlocks.AcquisitionBlock.TilesSetup.PositionGroups.PositionGroup)['TileAcquisitionOverlap'])
 
        
        intertile_distance_pixels = sh['X'][1]*(1-percent_overlap)
        x_name = round((bbox.x-init_x)/intertile_distance_pixels)
        y_name = round((bbox.y-init_y)/intertile_distance_pixels)
        z_name = 0
 
 
        # dimensionX = dict(mdata.czi_box.ImageDocument.Metadata.Experiment.ExperimentBlocks.AcquisitionBlock.AcquisitionModeSetup)['DimensionX']
        # dimensionY = dict(mdata.czi_box.ImageDocument.Metadata.Experiment.ExperimentBlocks.AcquisitionBlock.AcquisitionModeSetup)['DimensionY']
        # dimensionZ = dict(mdata.czi_box.ImageDocument.Metadata.Experiment.ExperimentBlocks.AcquisitionBlock.AcquisitionModeSetup)['DimensionZ']
 
        camera_frame_height = dict(mdata.czi_box.ImageDocument.Metadata.Experiment.ExperimentBlocks.AcquisitionBlock.AcquisitionModeSetup)['CameraFrameHeight']
        camera_frame_width = dict(mdata.czi_box.ImageDocument.Metadata.Experiment.ExperimentBlocks.AcquisitionBlock.AcquisitionModeSetup)['CameraFrameWidth']
 
        X_resolution = float(dict(mdata.czi_box.ImageDocument.Metadata.Experiment.ExperimentBlocks.AcquisitionBlock.AcquisitionModeSetup)['ScalingX'])
        Y_resolution = float(dict(mdata.czi_box.ImageDocument.Metadata.Experiment.ExperimentBlocks.AcquisitionBlock.AcquisitionModeSetup)['ScalingY'])
        Z_resolution = float(dict(mdata.czi_box.ImageDocument.Metadata.Experiment.ExperimentBlocks.AcquisitionBlock.AcquisitionModeSetup)['ScalingZ'])
 
        pixel_resolution = [X_resolution, Y_resolution, Z_resolution]
 
        def get_tiff_tile_name(mdata, tile_index):
            czi = CziFile(mdata.filepath)
            bbox = czi.get_scene_bounding_box(0)
 
 
 
            if tile_index == 0:
                
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
        
            intertile_distance_pixels = sh['X'][1]*(1-percent_overlap)
            x_name = round((bbox.x-init_x)/intertile_distance_pixels)
            y_name = round((bbox.y-init_y)/intertile_distance_pixels)
            z_name = 0
 
            round_number = 'R0' # TODO add some logic to determine round number
 
            channel_number = int(czi.read_subblock_metadata(Z = 0)[0][0]['C'])
            print(f'channel_number: {channel_number}')
 
            #channel wavelength
            channel_wavelength = int(float(get_channel_metadata_from_channel_number(mdata,channel_number).IlluminationWavelength.SinglePeak))
            tile_name = f'{round_number}_X_{x_name:04}_Y_{y_name:04}_Z_{z_name:04}_ch_{channel_wavelength}.tiff'
 
            return tile_name
 
            
 
 
        round_number = 'R0' # TODO add some logic to determine round number
        tile_name = f'{round_number}_X_{x_name:04}_Y_{y_name:04}_Z_{z_name:04}_ch_{channel_wavelength}.tiff'
 
        tif_filename = Path(OUTPUT_FOLDER).joinpath(tile_name)
        
        if METADATA_ONLY in ["False", "false", "FALSE", False]:
            tf.imwrite(tif_filename, img, metadata={'axes': 'ZYX'})
 
 
 
        #write the tile dict
        tile_dict = {'file': str(tif_filename), 'size': size, 'pixel_resolution': pixel_resolution, 'position': position, 'channel_wavelength': channel_wavelength}
 
 
        if channel_wavelength == 405:
            list_of_ch_405_tile_dicts.append(tile_dict)
        
        list_of_tile_dicts.append(tile_dict)
    
    #write the json
    metadata_folder = Path(OUTPUT_FOLDER).parent.as_posix()
    with open(metadata_folder+'/all_channel_tile_metadata.json', 'w') as f:
        json.dump(list_of_tile_dicts, f)
 
    with open(metadata_folder+'/ch_405_position_metadata.json', 'w') as f:
        json.dump(list_of_ch_405_tile_dicts, f)
 
    print(f"Invalid Count: {invalid_count}")
 
    #kick off json to xml
    print(f'converting json to xml...')
 
 
 
 
    #should add validation to check that s3_data_path is the correct format....
    spim_data_path = "/data/" + Path(OUTPUT_FOLDER).parent.stem +"/SPIM.ome.zarr/"
    convert_json_to_xml(metadata_folder+'/ch_405_position_metadata.json', spim_data_path, "stitching_405")
 
    s3_data_path = "/data/" + Path(OUTPUT_FOLDER).parent.stem +"/radial_correction.ome.zarr/"
    convert_json_to_xml(metadata_folder+'/all_channel_tile_metadata.json', s3_data_path, "stitching_all_channels")
 
if __name__ == "__main__":
    main()
 
 