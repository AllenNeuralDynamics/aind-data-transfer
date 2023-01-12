"""Loads Imaging job configurations"""
import argparse
from pathlib import Path

import yaml
from numcodecs import Blosc

from aind_data_transfer.util.file_utils import is_cloud_url, parse_cloud_url


class ImagingJobConfigurationLoader:
    def load_configs(self, sys_args):
        """Load yaml config at conf_src Path as python dict"""
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "-c", "--conf-file-location", required=True, type=str
        )
        parser.add_argument(
            "-r", "--raw-data-source", required=False, type=str
        )
        args = parser.parse_args(sys_args)
        conf_src = args.conf_file_location
        with open(conf_src) as f:
            config = yaml.load(f, Loader=yaml.SafeLoader)
        if args.raw_data_source is not None:
            config["endpoints"]["raw_data_dir"] = args.raw_data_source
        self.__resolve_endpoints(config)
        self.__parse_compressor_configs(config)
        return config

    @staticmethod
    def __resolve_endpoints(configs):
        """
        If the destination folder is a cloud bucket without prefix, a prefix
        will be created using the acquisition directory name.
        Args:
            configs (dic): Configurations

        Returns:
            None, modifies the base configs in place
        """

        dest_data_dir = configs["endpoints"]["dest_data_dir"]
        if is_cloud_url(dest_data_dir):
            provider, bucket, prefix = parse_cloud_url(dest_data_dir)
            if prefix == "":
                prefix = Path(configs["endpoints"]["raw_data_dir"]).name
                dest_data_dir = dest_data_dir.strip("/") + "/" + prefix
                configs["endpoints"]["dest_data_dir"] = dest_data_dir

    @staticmethod
    def __parse_compressor_configs(configs):
        """Util method to map a string to class attribute"""
        try:
            compressor_name = configs["transcode_job"]["compressor"][
                "compressor_name"
            ]
        except KeyError:
            compressor_name = None
        try:
            compressor_kwargs = configs["transcode_job"]["compressor"][
                "kwargs"
            ]
        except KeyError:
            compressor_kwargs = None
        if (
            compressor_name
            and compressor_name == Blosc.codec_id
            and compressor_kwargs
            and "shuffle" in compressor_kwargs
        ):
            shuffle_str = configs["transcode_job"]["compressor"]["kwargs"][
                "shuffle"
            ]
            shuffle_val = getattr(Blosc, shuffle_str)
            configs["transcode_job"]["compressor"]["kwargs"][
                "shuffle"
            ] = shuffle_val
