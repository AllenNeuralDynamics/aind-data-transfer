from aind_data_transfer.transformations import converters, file_io
from aind_data_transfer.util import io_utils

from aind_data_transfer.config_loader.imaging_configuration_loader import (
    ImagingJobConfigurationLoader,
)
import pathlib
import json
import datetime
import sys
import argparse


def main():
    read_yml = False  # broken...

    if read_yml:
        # get path to yml file relative to repo root
        repo_root = pathlib.Path.cwd()
        yml_path = repo_root.joinpath("conf/transcode_job_configs.yml")

        sys.argv = ["-c", str(yml_path)]

        job_configs = ImagingJobConfigurationLoader().load_configs(sys.argv)

        data_src_dir = pathlib.Path(job_configs["endpoints"]["raw_data_dir"])
        dest_data_dir = job_configs["endpoints"]["dest_data_dir"]
        if dest_data_dir.endswith("/"):
            # remove trailing slash
            dest_data_dir = dest_data_dir[:-1]
    else:
        data_src_dir = pathlib.Path(
            "/allen/programs/mindscope/workgroups/omfish/mfish/temp_raw/diSPIM/HCR_677594-Christian-ID_2023-10-20_15-10-36"
        )
        dest_data_dir = "HCR_677594_2023-10-20_15-10-36"
    # log_path = data_src_dir.joinpath("imaging_log.log")
    # toml_dict = file_io.read_toml(data_src_dir.joinpath("config.toml"))
    # # read log file into dict
    # log_dict = file_io.read_imaging_log(log_path)
    # # add data_src_dir to log_dict
    # log_dict["data_src_dir"] = str(data_src_dir)
    # log_dict["config_toml"] = toml_dict
    # # convert to acq json
    # acq_json = converters.log_to_acq_json(log_dict)

    # # convert acq json to xml
    # is_zarr = True
    # condition = ""
    # acq_xml = converters.acq_json_to_xml(
    #     acq_json, log_dict, dest_data_dir + "/diSPIM.zarr", is_zarr, condition
    # )  # needs s3 path

    # # write xml to file
    # xml_file_path = data_src_dir.joinpath("Camera_405.xml")
    # file_io.write_xml(acq_xml, xml_file_path)
    # json_fp = data_src_dir.joinpath("acquisition.json")

    # file_io.write_acq_json(acq_json, json_fp)

    log_file = data_src_dir.joinpath('imaging_log.log')
    acq_file = data_src_dir.joinpath('dispim_acquisition.json')


    toml_dict = file_io.read_toml(data_src_dir.joinpath('config.toml'))
    

    # read log file into dict
    if not log_file.exists():
        log_dict = file_io.read_imaging_log(log_file)
        log_dict['data_src_dir'] = (data_src_dir.as_posix())
        log_dict['config_toml'] = toml_dict
    
    else: 
        log_dict = {'imaging_log_file': None} #set this to none to read schema_log


    if acq_file.exists():
        acq_json = converters.read_dispim_aquisition(acq_file)
        

    #if any of the values of log_dict are None, then get it from schema_log
    elif any(v is None for v in log_dict.values()):
        print('Some values in imaging_log.log are None. Reading schema_log.log')
        log_file =  data_src_dir.joinpath('schema_log.log')
        log_dict = {}
        log_dict = file_io.read_schema_log_file(log_file)
        log_dict['data_src_dir'] = (data_src_dir.as_posix())
        log_dict['config_toml'] = toml_dict
        print('Finished reading schema_log.log')
        try:
            acq_json = converters.schema_log_to_acq_json(log_dict)
            print('Finished converting schema_log.log to acq json')
        except Exception as e:
            print(f"Failed to convert schema_log.log to acq json: {e}") 

    else:
        # convert imaging_log to acq json
        try:
            acq_json = converters.log_to_acq_json(log_dict)
        except Exception as e:
            print(f"Failed to convert imaging_log.log to acq json: {e}")

    # convert to acq json
    acq_json_path = data_src_dir.joinpath('acquisition.json')


    try:
        file_io.write_acq_json(acq_json, acq_json_path)
        print('Finished writing acq json')
    except Exception as e:
        print(f"Failed to write acquisition.json: {e}")

    # convert acq json to xml
    is_zarr = True
    condition = "channel=='405'"
    acq_xml = converters.acq_json_to_xml(acq_json, log_dict, dest_data_dir + f'/SPIM.ome.zarr', is_zarr, condition)  # needs relative path to zarr file (as seen by code ocean)

    # write xml to file
    xml_file_path = data_src_dir.joinpath('Camera_405.xml')  #
    file_io.write_xml(acq_xml, xml_file_path)


if __name__ == "__main__":
    main()
