from aind_data_transfer.transformations import converters, file_io
from aind_data_transfer.util import io_utils

import pathlib
import json
import datetime


def main():
    test_dir = pathlib.Path.cwd().joinpath('tests/resources/imaging/iSPIM_test')
    log_path = test_dir.joinpath('imaging_log.log')
    data_src_dir = pathlib.Path('/allen/programs/mindscope/workgroups/omfish/mfish/temp_raw/diSPIM_624852_2023-06-03_10-11-33')
    dest_data_dir = "s3://aind-scratch-data/diSPIM_624852_2023-06-03_10-11-33" #needs to be s3 path

    #read log file into dict
    log_dict = file_io.read_imaging_log(log_path)
    #convert to acq json
    acq_json = converters.log_to_acq_json(log_dict)

    #convert acq json to xml
    is_zarr = True
    condition = ""
    acq_xml = converters.acq_json_to_xml(acq_json, dest_data_dir, is_zarr, condition) #needs s3 path

    #write xml to file
    xml_file_path = data_src_dir.joinpath('Camera_405.xml')
    file_io.write_xml(acq_xml, xml_file_path)



if __name__ == "__main__":
    main()