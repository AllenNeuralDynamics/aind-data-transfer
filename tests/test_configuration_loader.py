import unittest
from transfer.configuration_loader import EphysJobConfigurationLoader


class TestEphysJobConfigs(unittest.TestCase):

    reader_configs = (
        '{"reader_name":"openephys",'
        '"input_dir":"some_dir/some_sub_dir"}'
    )

    compressor_configs = (
        '{"compressor_name":"wavpack",'
        '"kwargs":{"level":3}}'
    )

    writer_configs = (
        '{"output_dir":"another_dir/zarr_stuff",'
        '"job_kwargs":{"n_jobs":20,"chunk_duration":"1s","progress_bar":true}}'
    )

    config_loader = EphysJobConfigurationLoader()

    def test_ephys_job_configs_loader_wavpack(self):
        wavpack_compressor_configs = (
            '{"compressor_name":"wavpack",'
            '"kwargs":{"level":3}}'
        )
        rc, cc, wc = self.config_loader.get_configs(
            args=['-r', self.reader_configs,
                  '-c', wavpack_compressor_configs,
                  '-w', self.writer_configs])

        print(rc, cc, wc)

        self.assertTrue(True)

    def test_ephys_job_configs_loader_blosc(self):
        blosc_compressor_configs = (
            '{"compressor_name":"blosc",'
            '"kwargs":{"shuffle":"BITSHUFFLE"}}'
        )
        rc, cc, wc = self.config_loader.get_configs(
            args=['-r', self.reader_configs,
                  '-c', blosc_compressor_configs,
                  '-w', self.writer_configs])

        print(rc, cc, wc)

        self.assertTrue(True)
