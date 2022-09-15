"""Loads job configurations"""
import argparse
import json
from pathlib import Path


class EphysJobConfigs:
    """Loads Ephys compression job configs"""
    # TODO: Add sanity checks and error handling.
    parser = argparse.ArgumentParser()
    parser.add_argument('-r',
                        '--reader-configs',
                        required=True,
                        type=json.loads)
    parser.add_argument('-c',
                        '--compressor-configs',
                        required=True,
                        type=json.loads)
    parser.add_argument('-w',
                        '--write-configs',
                        required=True,
                        type=json.loads)
    args = parser.parse_args()

    read_configs = args.reader_configs
    read_configs['input_dir'] = Path(read_configs['input_dir'])
    write_configs = args.write_configs
    write_configs['output_dir'] = Path(write_configs['output_dir'])
    compressor_configs = args.compressor_configs
