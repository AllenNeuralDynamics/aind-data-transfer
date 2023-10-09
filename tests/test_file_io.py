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
            "/allen/aind/scratch/diSPIM/diSPIM_685890_2023-06-29_14-39-56"
        )
        dest_data_dir = "diSPIM_685890_2023-06-29_14-39-56"
    log_path = data_src_dir.joinpath("imaging_log.log")
    toml_dict = file_io.read_toml(data_src_dir.joinpath("config.toml"))
    # read log file into dict
    log_dict = file_io.read_imaging_log(log_path)
    # add data_src_dir to log_dict
    log_dict["data_src_dir"] = str(data_src_dir)
    log_dict["config_toml"] = toml_dict
    # convert to acq json
    acq_json = converters.log_to_acq_json(log_dict)

    # convert acq json to xml
    is_zarr = True
    condition = ""
    acq_xml = converters.acq_json_to_xml(
        acq_json, log_dict, dest_data_dir + "/diSPIM.zarr", is_zarr, condition
    )  # needs s3 path

    # write xml to file
    xml_file_path = data_src_dir.joinpath("Camera_405.xml")
    file_io.write_xml(acq_xml, xml_file_path)
    json_fp = data_src_dir.joinpath("acquisition.json")

    file_io.write_acq_json(acq_json, json_fp)


if __name__ == "__main__":
    main()
