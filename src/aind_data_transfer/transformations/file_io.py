import datetime
import json
from pathlib import Path
from typing import Union
import xml.etree.ElementTree as ET
from aind_data_transfer.util.file_utils import read_text_to_list
import re
import toml
from aind_data_schema.core.acquisition import Acquisition


def read_toml(toml_path: Union[Path, str]) -> dict:
    with open(toml_path) as f:
        return toml.load(f)


def read_json(json_path: Union[Path, str]) -> dict:
    with open(json_path) as f:
        return json.load(f)


def read_imaging_log(log_path: str) -> dict:
    """Reads imaging log file into log_dict

    Args:
        log_path (str): path to imaging log file

    Returns:
        log_dict (dict): dict containing imaging log info

        log_dict.keys() =
            ['tiles' (dict of dicts, indexed by filename),
                 keys: 'tile_x_position', 'tile_y_position', 'tile_z_position', 'channel', 'file_name'             )
            'channels' (dict of dicts, indexed by channel name),
                 keys: 'laser_wavelength', 'laser_power', 'filter_wheel_index'

            'specimen_id',
            'subject_id',
            'instrument_id',
            'session_start_time',
            'session_end_time',
            'instrument_id',
            'chamber_immersion_medium',
            'chamber_immersion_refractive_index',
            'x_voxel_size',
            'y_voxel_size',
            'z_voxel_size',
            'lightsheet_angle',
            'local_storage_directory',
            'external_storage_directory'
            ]




    """

    log_list = read_text_to_list(log_path)

    with open(log_path, "r") as f:
        log_text = f.read()

    def get_channel_dict(log_list: list) -> dict:
        """This happens once for each of the channels in the log file"""
        channel_name_regex = r"channel_name.? (\d+)"
        laser_wavelength_regex = r"laser_wavelength.? (\d+) nanometers"
        laser_power_regex = r"laser_power.? (\d+\.?\d+?)"
        filter_wheel_index_regex = r"filter_wheel_index.? (\d+)"

        channel_dict = {}
        for line in log_list:
            # get channel info
            if "channel_name" in line:
                ch_name_match = re.search(channel_name_regex, line)
                if ch_name_match:
                    channel_name = ch_name_match.group(1)

                    # move to next line
                    line = log_list[log_list.index(line) + 1]
                    laser_wavelength = re.search(
                        laser_wavelength_regex, line
                    ).group(1)
                    line = log_list[log_list.index(line) + 1]
                    laser_power = re.search(laser_power_regex, line).group(1)
                    line = log_list[log_list.index(line) + 1]
                    filter_wheel_index = re.search(
                        filter_wheel_index_regex, line
                    ).group(1)
                    channel_dict[channel_name] = {
                        "laser_wavelength": laser_wavelength,
                        "laser_power": laser_power,
                        "filter_wheel_index": filter_wheel_index,
                    }
            else:
                continue
        return channel_dict

    # also want to get specimen_id, subject_id, instrument_id, session_start_time, session_end_time, channel_dict

    def get_session_info(log_text: str, log_dict: dict) -> dict:
        """This is only run once per log file"""
        session_start_time_regex = r"session_start_time, (.*?)\n"
        session_end_time_regex = r"Ending time: (.*?)$"
        specimen_id_regex = r"specimen_id,(.*?)\n"
        subject_id_regex = r"subject_id,(.*?)\n"
        instrument_id_regex = r"instrument_id, (\w+)"
        chamber_immersion_medium_regex = r"chamber_immersion_medium, (\w+)"
        chamber_immersion_refractive_index_regex = (
            r"chamber_immersion_refractive_index, (\d+\.\d+)"
        )
        lightsheet_angle_regex = r"lightsheet_angle, (\d+) degrees"
        local_storage_dir_regex = r"local_storage_directory, (.*?)\n"
        external_storage_dir_regex = r"external_storage_directory, (.*?)\n"

        x_voxel_regex = r"x_voxel_size, (\d+\.\d+) micrometers"
        y_voxel_regex = r"y_voxel_size, (\d+\.\d+) micrometers"
        z_voxel_regex = r"z_voxel_size, (\d+\.\d+) micrometers"

        try: 
            log_dict["session_start_time"] = re.search(
                session_start_time_regex, log_text).group(1)
        except: 
            print(f'Could not find session start time in {log_path}')
            log_dict["session_start_time"] = None
        try:
            log_dict["session_end_time"] = re.search(
                session_end_time_regex, log_text).group(1)
        except:
            print(f'Could not find session end time in {log_path}')
            log_dict["session_end_time"] = None
        try:
            log_dict["specimen_id"] = re.search(specimen_id_regex, log_text).group(1)
        except: 
            print(f'Could not find specimen id in {log_path}')
            log_dict["specimen_id"] = None
        try:
            log_dict["subject_id"] = re.search(subject_id_regex, log_text).group(1)
        except:
            print(f'Could not find subject id in {log_path}')
            log_dict["subject_id"] = None
        try:
            log_dict["instrument_id"] = re.search(
                instrument_id_regex, log_text).group(1)
        except: 
            print(f'Could not find instrument id in {log_path}')
            log_dict["instrument_id"] = None

        try:
            log_dict["chamber_immersion_medium"] = re.search(
                chamber_immersion_medium_regex, log_text
            ).group(1)
        except: 
            print(f'Could not find chamber immersion medium in {log_path}')
            log_dict["chamber_immersion_medium"] = None
        try:
            log_dict["chamber_immersion_refractive_index"] = re.search(
                chamber_immersion_refractive_index_regex, log_text
            ).group(1)
        except:
            print(f'Could not find chamber immersion refractive index in {log_path}')
            log_dict["chamber_immersion_refractive_index"] = None
        try:
            log_dict["x_voxel_size"] = re.search(x_voxel_regex, log_text).group(1)
            log_dict["y_voxel_size"] = re.search(y_voxel_regex, log_text).group(1)
            log_dict["z_voxel_size"] = re.search(z_voxel_regex, log_text).group(1)
        except:
            print(f'Could not find voxel size in {log_path}')
            log_dict["x_voxel_size"] = None
            log_dict["y_voxel_size"] = None
            log_dict["z_voxel_size"] = None
        try:
            log_dict["lightsheet_angle"] = str(
                re.search(lightsheet_angle_regex, log_text).group(1))
        except:
            print(f'Could not find lightsheet angle in {log_path}')
            log_dict["lightsheet_angle"] = None
        try:
            log_dict["local_storage_directory"] = re.search(
                local_storage_dir_regex, log_text).group(1)  # these are the directories at time of writing the log file, but are specific to aquisition computer computer, which if it is saving to a NAS, breaks the rest of the workflow
        except: 
            print(f'Could not find local storage directory in {log_path}')
            log_dict["local_storage_directory"] = None
        try:
            log_dict["external_storage_directory"] = re.search(
                external_storage_dir_regex, log_text).group(1)
        except: 
            print(f'Could not find external storage directory in {log_path}')
            log_dict["external_storage_directory"] = None
        
        return log_dict

    # now the log file is a list of strings, each string is a line in the log file

    # we care about lines with this string in it "Collecting tile stacks at (X, Y) [um] for channels [405] and saving to: [*]"
    # we want to extract the file name, and the x,y,z coordinates
    def get_tile_dict(log_list: list) -> dict:
        tile_loc_regex = "Collecting tile stacks at \((.?\d+.\d+), (.?\d+.\d+)\).*?\[um\] for channels \[([\d, ]+)\].*?saving to.*?'(.*?)'"
        tile_z_position_regex = r"Starting scan at Z = .?(\d+\.\d+) mm"

        tile_dict = {}
        for line in log_list:
            if "Collecting tile stacks at" in line:
                # print(line)
                match = re.search(tile_loc_regex, line)
                if match:
                    # print(match.groups())
                    try:
                        x = match.group(1)
                        y = match.group(2)
                    except:
                        print(f'Could not find x and y coordinates in {log_path}')
                        x = None
                        y = None
                    if len(match.group(3)) > 3:
                        channel = match.group(3).split(", ")
                    else:
                        channel = match.group(3)
                    file_name = Path(match.group(4)).name

                    # move to next next line in log_list without using next
                    line = log_list[log_list.index(line) + 2]
                    try: 
                        z = (
                            float(re.search(tile_z_position_regex, line).group(1))
                            * 1000
                        )  # convert to micrometers
                    except:
                        print(f'Could not find z coordinate in {log_path}')
                        z = None
                    tile_dict[file_name] = {
                        "tile_x_position": x,
                        "tile_y_position": y,
                        "tile_z_position": z,
                        "channel": channel,
                        "file_name": file_name,
                    }
                else:
                    continue
        return tile_dict

    log_dict = {}
    log_dict = get_session_info(log_text, log_dict)
    log_dict["tiles"] = get_tile_dict(log_list)
    log_dict["channels"] = get_channel_dict(log_list)
    return log_dict


def read_log_file(log_path: str) -> dict:
    with open(log_path, "r") as f:
        lines = f.readlines()

    log_dict = {}
    log_dict["tiles"]: list[dict] = []
    for i, line in enumerate(lines):
        line = line.replace(
            "'", '"'
        )  # replace single quotes with double quotes
        try:
            tmp = json.loads(f"{line}")
            # print(tmp)
        except json.decoder.JSONDecodeError as e:
            print(f"Error decoding line {i}: {line} with error {e}")
            # continue
            raise e
        if i == 0:
            log_dict["session_start_time"] = datetime.datetime.fromisoformat(
                tmp["created_strftime"]
            )
        if "local_storage_directory" in tmp.keys():
            tmp.pop("name")
            tmp.pop("msg")
            tmp.pop("levelname")
            tmp.pop("created")
            tmp.pop("created_strftime")
            log_dict = {**log_dict, **tmp}
        if "file_name" in tmp.keys():
            print(f'Found file name: {tmp["file_name"]}')

            tmp.pop("name")
            tmp.pop("msg")
            tmp.pop("levelname")
            tmp.pop("created")
            tmp.pop("created_strftime")
            log_dict["tiles"].append(tmp)

        # for iSPIM schema_log, where file_name is embdedded in the 'message'
        if "message" in tmp.keys() and "file_name" in tmp["message"]:
            print(f'Found file name: {tmp["message"]["file_name"]}')

            tmp.pop("name")
            tmp.pop("msg")
            tmp.pop("levelname")
            tmp.pop("created")
            tmp.pop("created_strftime")
            log_dict["tiles"].append(tmp)

        if i == len(lines) - 1:
            log_dict["session_end_time"] = datetime.datetime.fromisoformat(
                tmp["created_strftime"]
            )

    return log_dict


def read_schema_log_file(log_path: str) -> dict: 
    with open(log_path, 'r') as f: 
        lines = f.readlines()

    log_dict = {}
    log_dict['tiles']: list[dict] = []
    for i, line in enumerate(lines): 
        line = line.replace("\'", "\"")
        #remove windows path   
        line = line.replace('WindowsPath(','').replace(')', '')

        #escape file_name quotations marks with \\
        if 'file_name, [' in line:
            line = line.replace('file_name, ["', 'file_name, [\\"').replace('tiff"]', 'tiff\\"]')

        # if 'None' in line:
        #     line = line.replace('None', '"None"')

        # if '"["' in line and 'tiff"]' in line:
        #     line = line.replace('"["', "['")
        #     line = line.replace('tiff"]', "tiff'],")
            
    
        try:
            tmp = json.loads(f'{line}')
        except: 
            pass
        # Only specific lines matter:
        if i == 0: 
            log_dict['session_start_time'] = datetime.datetime.fromisoformat(tmp['created_strftime'])
        if 'local_storage_directory' in tmp.keys():
            tmp.pop('name')
            tmp.pop('msg')
            tmp.pop('levelname')
            tmp.pop('created')
            tmp.pop('created_strftime')
            log_dict = {**log_dict, **tmp}
        if 'file_name' in tmp.keys():
            tmp.pop('name')
            tmp.pop('msg')
            tmp.pop('levelname')
            tmp.pop('created')
            tmp.pop('created_strftime')
            log_dict['tiles'].append(tmp)
        if i == len(lines) - 1: 
            log_dict['session_end_time'] = datetime.datetime.fromisoformat(tmp['created_strftime'])

    return log_dict

def write_xml(tree: ET.ElementTree, path: Union[Path, str]) -> None: 
    ET.indent(tree, space="\t", level=0)

    # write xml to file
    tree.write(path, encoding="utf-8", xml_declaration=True)


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
    acq_obj.session_start_time = acq_obj.session_start_time.isoformat()
    acq_obj.session_end_time = acq_obj.session_end_time.isoformat()

    with open(acq_json_path, "w") as f:
        json.dump(json.loads(acq_obj.model_dump_json()), f, indent=4)
