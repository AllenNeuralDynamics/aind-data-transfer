"""Loads job configurations"""
import argparse
import json
from pathlib import Path

from numcodecs import Blosc


class EphysJobConfigurationLoader:
    """Loads Ephys compression job configs"""

    # TODO: Add sanity checks, error handling, and better document what the
    #  configs are
    _parser = argparse.ArgumentParser()
    _parser.add_argument(
        "-r", "--reader-configs", required=True, type=json.loads
    )
    _parser.add_argument(
        "-c", "--compressor-configs", required=True, type=json.loads
    )
    _parser.add_argument(
        "-w", "--write-configs", required=True, type=json.loads
    )

    @staticmethod
    def _parse_compressor_configs(compressor_configs):
        """
        Parses compressor configs. If the compressor is blosc, and shuffle is
        defined, it will convert the string to the appropriate enum value. So
        if 'BITSHUFFLE' is defined, it will be mapped to Blosc.BITSHUFFLE
        Args:
            compressor_configs (dict): parameter settings for the compressor.
            Will look like
                '{"compressor_conf":{"compressor_name":"wavpack",
                                     "kwargs":{"level":3}},
                  "scale_read_block_conf":{"chunk_size":20}
                }'
            where the scale_read_block_conf is optional.

        Returns: A tuple where the first part is a dictionary of compressor
        configs, and the second part is the config dict of scale_read_block
        options.
        """
        compressor_conf = {}
        scale_read_block_conf = {}
        if "compressor_conf" in compressor_configs:
            compressor_conf = compressor_configs["compressor_conf"]
        if (
            compressor_conf
            and "compressor_name" in compressor_conf
            and compressor_conf["compressor_name"] == Blosc.codec_id
            and "kwargs" in compressor_conf
            and "shuffle" in compressor_conf["kwargs"]
            and isinstance(compressor_conf["kwargs"]["shuffle"], str)
        ):
            shuffle_setting = compressor_conf["kwargs"]["shuffle"]
            # If "shuffle" is something like "1", convert it to an int
            # Else, convert the string like "BITSHUFFLE" to Blosc attr
            if shuffle_setting.isdigit():
                compressor_conf["kwargs"]["shuffle"] = int(shuffle_setting)
            else:
                compressor_conf["kwargs"]["shuffle"] = getattr(
                    Blosc, compressor_conf["kwargs"]["shuffle"]
                )
        if "scale_read_block_conf" in compressor_configs:
            scale_read_block_conf = compressor_configs["scale_read_block_conf"]

        return compressor_conf, scale_read_block_conf

    def get_configs(self, args=None):
        """
        Parses command line arguments into configs.
        Args:
            args (List[str]): Command line arguments.

        Returns:
            A tuple of configs for an Ephys Job. scale_read_block_conf
            may be an empty dict if the defaults are to be used.
            (read_conf, comp_configs, scale_read_block_conf, write_conf)
        """
        job_args = self._parser.parse_args(args=args)

        read_conf = job_args.reader_configs
        read_conf["input_dir"] = Path(read_conf["input_dir"])
        write_conf = job_args.write_configs
        write_conf["output_dir"] = Path(write_conf["output_dir"])
        compressor_configs = job_args.compressor_configs
        comp_conf, scale_read_block_conf = self._parse_compressor_configs(
            compressor_configs
        )
        return read_conf, comp_conf, scale_read_block_conf, write_conf
