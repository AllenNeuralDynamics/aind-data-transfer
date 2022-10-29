"""Loads job configurations"""
import argparse
import os
import re
from pathlib import Path

import yaml
from numcodecs import Blosc

from aind_data_transfer.readers import EphysReaders
from aind_data_transfer.util.file_utils import is_cloud_url, parse_cloud_url


class EphysJobConfigurationLoader:
    """Class to handle loading ephys job configs"""

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

    @staticmethod
    def __resolve_endpoints(configs):
        """
        Only the raw data source needs to be provided as long as the base dir
        name is formatted correctly. If the dest_data_dir and cloud endpoints
        are not set in the conf file, they will be created automatically based
        on the name of the raw_data_source.
        Args:
            configs (dic): Configurations

        Returns:
            None, modifies the base configs in place
        """
        raw_data_folder = Path(configs["endpoints"]["raw_data_dir"]).name

        dest_data_dir = configs["endpoints"]["dest_data_dir"]
        if dest_data_dir is None and re.match(
            EphysReaders.SourceRegexPatterns.subject_datetime.value,
            raw_data_folder,
        ):
            configs["endpoints"]["dest_data_dir"] = (
                "ecephys_" + raw_data_folder
            )
        if dest_data_dir is None and re.match(
            EphysReaders.SourceRegexPatterns.ecephys_subject_datetime.value,
            raw_data_folder,
        ):
            configs["endpoints"]["dest_data_dir"] = raw_data_folder

        if configs["endpoints"]["s3_prefix"] is None:
            dest_data_folder = Path(configs["endpoints"]["dest_data_dir"]).name
            configs["endpoints"]["s3_prefix"] = dest_data_folder

        if configs["endpoints"]["gcp_prefix"] is None:
            dest_data_folder = Path(configs["endpoints"]["dest_data_dir"]).name
            configs["endpoints"]["gcp_prefix"] = dest_data_folder

        if configs["register_on_codeocean_job"]["asset_name"] is None:
            configs["register_on_codeocean_job"]["asset_name"] = (
                configs["endpoints"]["s3_prefix"])

        if configs["register_on_codeocean_job"]["mount"] is None:
            configs["register_on_codeocean_job"]["mount"] = (
                configs["endpoints"]["s3_prefix"])

        if configs["trigger_codeocean_spike_sorting_job"]["mount"] is None:
            configs["trigger_codeocean_spike_sorting_job"]["mount"] = (
                configs["endpoints"]["s3_prefix"])

    @staticmethod
    def __resolve_logging(configs: dict) -> None:
        """
        Resolves logging config in place
        Parameters
        ----------
        configs : dict
          Configurations

        Returns
        -------
        None
        """

        if configs["logging"]["level"] is None:
            configs["logging"]["level"] = "INFO"
        if os.getenv("LOG_LEVEL"):
            configs["logging"]["level"] = os.getenv("LOG_LEVEL")

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
        self.__resolve_logging(raw_config)
        config_without_nones = self.__remove_none(raw_config)
        self.__parse_compressor_configs(config_without_nones)
        return config_without_nones


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
        If the destination folder is a cloud bucket without prefix, a prefix will be
        created using the acquisition directory name.
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
                dest_data_dir = dest_data_dir.strip("/") + '/' + prefix
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
