from types import MappingProxyType

import yaml
from numcodecs import Blosc

from aind_data_transfer.config_loader.generic_config_loader import (
    GenericJobConfigurationLoader,
)


class EphysCompressionConfigs:

    _CLIP_DATA_DEFAULT = {"clip_kwargs": {"nframes": None}}
    _COMPRESS_DATA_DEFAULT = {
        "write_kwargs": {
            "n_jobs": -1,
            "chunk_duration": "1s",
            "progress_bar": True,
        },
        "format_kwargs": {"output_format": None},
        "compressor": {"compressor_name": "wavpack", "kwargs": {"level": 3}},
        "scale_params": {
            "num_chunks_per_segment": None,
            "chunk_size": None,
            "disable_tqdm": None,
        },
        "max_windows_filename_len": None,
    }

    _DEFAULT_CONFIGS = MappingProxyType(
        {
            "clip_data": _CLIP_DATA_DEFAULT,
            "compress_data": _COMPRESS_DATA_DEFAULT,
        }
    )

    def __init__(self, configs=_DEFAULT_CONFIGS, data_name="openephys"):
        configs_without_nones = self.__remove_none(configs)
        self.configs = configs_without_nones
        self.data_name = data_name
        self.__parse_compressor_configs(configs_without_nones)

    @classmethod
    def from_yaml(cls, yaml_file_location):
        with open(yaml_file_location) as f:
            raw_config = yaml.load(f, Loader=yaml.SafeLoader)
        return cls(configs=raw_config)

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
            compressor_name = configs["compress_data"]["compressor"][
                "compressor_name"
            ]
        except KeyError:
            compressor_name = None
        try:
            compressor_kwargs = configs["compress_data"]["compressor"][
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
            shuffle_str = configs["compress_data"]["compressor"]["kwargs"][
                "shuffle"
            ]
            shuffle_val = getattr(Blosc, shuffle_str)
            configs["compress_data"]["compressor"]["kwargs"][
                "shuffle"
            ] = shuffle_val


class EphysConfigLoader(GenericJobConfigurationLoader):
    def __init__(
        self,
        args: list,
        compression_configs: EphysCompressionConfigs = (
            EphysCompressionConfigs()
        ),
    ):
        super().__init__(args=args)
        self.compression_configs = compression_configs
        if self.configs.extra_config_source is not None:
            self.compression_configs = EphysCompressionConfigs.from_yaml(
                self.configs.extra_config_source
            )
