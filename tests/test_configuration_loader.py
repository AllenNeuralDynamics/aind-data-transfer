import unittest
from pathlib import Path
from transfer.configuration_loader import EphysJobConfigurationLoader
from numcodecs import Blosc


class TestEphysJobConfigs(unittest.TestCase):

    reader_configs = (
        '{"reader_name":"openephys",'
        '"input_dir":"some_dir/some_sub_dir"}'
    )

    writer_configs = (
        '{"output_dir":"another_dir/zarr_stuff",'
        '"job_kwargs":{"n_jobs":20,"chunk_duration":"1s","progress_bar":true}}'
    )

    expected_read_configs = {
        'reader_name': 'openephys',
        'input_dir': Path('some_dir/some_sub_dir')
    }

    expected_write_configs = {
        'output_dir': Path('another_dir/zarr_stuff'),
        'job_kwargs': {'n_jobs': 20,
                       'chunk_duration': '1s',
                       'progress_bar': True}
    }

    config_loader = EphysJobConfigurationLoader()

    def test_ephys_job_configs_loader_wavpack(self):
        wavpack_compressor_configs = (
            '{"compressor_name":"wavpack",'
            '"kwargs":{"level":3}}'
        )
        expected_wavpack_configs = {
            'compressor_name': 'wavpack',
            'kwargs': {'level': 3}
        }
        actual_reader_conf, actual_compressor_conf, actual_write_conf = (
            self.config_loader.get_configs(
                args=['-r', self.reader_configs,
                      '-c', wavpack_compressor_configs,
                      '-w', self.writer_configs])
        )

        self.assertEqual(actual_reader_conf, self.expected_read_configs)
        self.assertEqual(actual_compressor_conf, expected_wavpack_configs)
        self.assertEqual(actual_write_conf, self.expected_write_configs)

    def test_ephys_job_configs_loader_blosc(self):
        blosc_compressor_configs = (
            '{"compressor_name":"blosc",'
            '"kwargs":{"shuffle":"BITSHUFFLE"}}'
        )
        expected_blosc_configs = {
            'compressor_name': 'blosc',
            'kwargs': {'shuffle': Blosc.BITSHUFFLE}
        }
        _, actual_compressor_conf, _ = self.config_loader.get_configs(
            args=['-r', self.reader_configs,
                  '-c', blosc_compressor_configs,
                  '-w', self.writer_configs])

        self.assertEqual(actual_compressor_conf, expected_blosc_configs)
