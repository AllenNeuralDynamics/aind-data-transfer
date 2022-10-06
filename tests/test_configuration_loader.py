import os
import unittest
from pathlib import Path

from numcodecs import Blosc

from transfer.configuration_loader import EphysJobConfigurationLoader

TEST_DIR = Path(os.path.dirname(os.path.realpath(__file__)))


class TestEphysJobConfigs(unittest.TestCase):

    conf_file_path = (
        TEST_DIR / "resources" / "ephys_upload_job_test_configs.yml"
    )

    test_configs = EphysJobConfigurationLoader().load_configs(conf_file_path)

    expected_configs = {
        "data": {
            "name": "openephys",
            "source_dir":
                "tests/resources/v0.6.x_neuropixels_multiexp_multistream",
            "dest_dir":
                "tests/resources/new/v0.6.x_neuropixels_multiexp_multistream",
        },
        "clip_data_job": {
            "clip": True,
            "clip_kwargs": {},
        },
        "compress_data_job": {
            "compress": True,
            "write_kwargs": {
                "n_jobs": 20,
                "chunk_duration": "1s",
                "progress_bar": True,
            },
            "format_kwargs": {},
            "compressor": {
                "compressor_name": "blosc",
                "kwargs": {"shuffle": Blosc.BITSHUFFLE},
            },
            "scale_params": {"chunk_size": 20, "disable_tqdm": False},
        },
        "upload_data_job": {
            "dryrun": True,
            "upload_to_s3": True,
            "s3_dest": "s3://aind-transfer-test/test_20221001",
            "upload_to_gcp": True,
            "gcp_dest": "gs://aind-data-dev/test_20221001",
        },
    }

    def test_conf_loads(self):
        self.assertEqual(self.test_configs, self.expected_configs)
