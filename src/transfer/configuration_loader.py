"""Loads job configurations"""
import yaml
from numcodecs import Blosc


class EphysJobConfigurationLoader:
    """Class to handle loading ephys job configs"""

    def __remove_none(self, data):
        """Remove keys whose value is None."""
        if isinstance(data, dict):
            return ({k: self.__remove_none(v)
                     for k, v in data.items() if v is not None})
        else:
            return data

    @staticmethod
    def __parse_compressor_configs(configs):
        """Util method to map a string to class attribute"""
        try:
            compressor_name = (
                configs["compress_data_job"]["compressor"]["compressor_name"])
        except KeyError:
            compressor_name = None
        try:
            compressor_kwargs = (
                configs["compress_data_job"]["compressor"]["kwargs"])
        except KeyError:
            compressor_kwargs = None
        if(
                compressor_name
                and compressor_name == Blosc.codec_id
                and compressor_kwargs
                and "shuffle" in compressor_kwargs
        ):
            shuffle_str = (
                configs["compress_data_job"]["compressor"]["kwargs"]["shuffle"]
            )
            shuffle_val = getattr(Blosc, shuffle_str)
            configs["compress_data_job"]["compressor"]["kwargs"]["shuffle"] = (
                shuffle_val
            )

    def load_configs(self, conf_src):
        """Load yaml config at conf_src Path as python dict"""
        with open(conf_src) as f:
            raw_config = yaml.load(f, Loader=yaml.SafeLoader)
        config_without_nones = self.__remove_none(raw_config)
        self.__parse_compressor_configs(config_without_nones)
        return config_without_nones
