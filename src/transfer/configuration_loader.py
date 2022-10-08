"""Loads job configurations"""
import argparse
import re
from enum import Enum
from pathlib import Path

import yaml
from numcodecs import Blosc


class EphysJobConfigurationLoader:
    """Class to handle loading ephys job configs"""

    class RegexPatterns(Enum):
        """Enum for compression algorithms a user can select"""

        subject_datetime = r"\d+_\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}"
        ecephys_subject_datetime = (
            r"ecephys_\d+_\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}"
        )

    def __remove_none(self, data):
        """Remove keys whose value is None."""
        if isinstance(data, dict):
            return {
                k: self.__remove_none(v)
                for k, v in data.items()
                if v is not None
            }
        else:
            return data

    @staticmethod
    def __parse_compressor_configs(configs):
        """Util method to map a string to class attribute"""
        try:
            compressor_name = configs["compress_data_job"]["compressor"][
                "compressor_name"
            ]
        except KeyError:
            compressor_name = None
        try:
            compressor_kwargs = configs["compress_data_job"]["compressor"][
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
            shuffle_str = configs["compress_data_job"]["compressor"]["kwargs"][
                "shuffle"
            ]
            shuffle_val = getattr(Blosc, shuffle_str)
            configs["compress_data_job"]["compressor"]["kwargs"][
                "shuffle"
            ] = shuffle_val

    def __resolve_endpoints(self, configs):
        """
        Only the raw data source needs to be provided as long as the base dir
        name is formatted correctly. If the shrunk_data_dir and cloud endpoints
        are not set in the conf file, they will be created automatically based
        on the name of the raw_data_source.
        Args:
            configs (dic): Configurations

        Returns:
            None, modifies the base configs in place
        """
        raw_data_folder = Path(configs["endpoints"]["raw_data_dir"]).name

        shrunk_data_dir = configs["endpoints"]["shrunk_data_dir"]
        if shrunk_data_dir is None and re.match(
            self.RegexPatterns.subject_datetime.value, raw_data_folder
        ):
            configs["endpoints"]["shrunk_data_dir"] = (
                "ecephys_" + raw_data_folder
            )
        if shrunk_data_dir is None and re.match(
            self.RegexPatterns.ecephys_subject_datetime.value, raw_data_folder
        ):
            configs["endpoints"]["shrunk_data_dir"] = raw_data_folder

        if configs["endpoints"]["s3_prefix"] is None:
            shrunk_data_folder = Path(
                configs["endpoints"]["shrunk_data_dir"]
            ).name
            configs["endpoints"]["s3_prefix"] = shrunk_data_folder

        if configs["endpoints"]["gcp_prefix"] is None:
            shrunk_data_folder = Path(
                configs["endpoints"]["shrunk_data_dir"]
            ).name
            configs["endpoints"]["gcp_prefix"] = shrunk_data_folder

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
            raw_config = yaml.load(f, Loader=yaml.SafeLoader)
        if args.raw_data_source is not None:
            raw_config["endpoints"]["raw_data_dir"] = args.raw_data_source
        self.__resolve_endpoints(raw_config)
        config_without_nones = self.__remove_none(raw_config)
        self.__parse_compressor_configs(config_without_nones)
        return config_without_nones
