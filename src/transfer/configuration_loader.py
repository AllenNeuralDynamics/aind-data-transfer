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
        if("compressor_name" in compressor_configs and
                compressor_configs["compressor_name"] == Blosc.codec_id and
                "kwargs" in compressor_configs and
                "shuffle" in compressor_configs["kwargs"] and
                isinstance(compressor_configs["kwargs"]["shuffle"], str)):
            shuffle_setting = compressor_configs["kwargs"]["shuffle"]
            # If "shuffle" is something like "1", convert it to an int
            # Else, convert the string like "BITSHUFFLE" to Blosc attr
            if shuffle_setting.isdigit():
                compressor_configs["kwargs"]["shuffle"] = int(shuffle_setting)
            else:
                compressor_configs["kwargs"]["shuffle"] = (
                    getattr(Blosc, compressor_configs["kwargs"]["shuffle"])
                )

    def get_configs(self, args=None):
        job_args = self._parser.parse_args(args=args)

        read_configs = job_args.reader_configs
        read_configs["input_dir"] = Path(read_configs["input_dir"])
        write_configs = job_args.write_configs
        write_configs["output_dir"] = Path(write_configs["output_dir"])
        compressor_configs = job_args.compressor_configs
        self._parse_compressor_configs(compressor_configs)
        return read_configs, compressor_configs, write_configs
