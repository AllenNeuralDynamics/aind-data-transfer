"""Tests configurations are set properly"""
import os
import unittest
from pathlib import Path

from numcodecs import Blosc

from aind_data_transfer.config_loader.imaging_configuration_loader import (
    ImagingJobConfigurationLoader,
)

TEST_DIR = Path(os.path.dirname(os.path.realpath(__file__)))
CONFIGS_DIR = TEST_DIR / "resources" / "test_configs"


class TestImagingJobConfigs(unittest.TestCase):
    code_repo_url = "https://github.com/AllenNeuralDynamics/aind-data-transfer"

    """Basic config loads test"""

    def test_conf_loads(self):
        """Basic config loads test"""

        raw_data_dir = (
            "tests/resources/imaging/exaSPIM_125L_2022-08-05_17-25-36"
        )

        expected_configs = {
            "jobs": {
                "upload_aux_files": False,
                "transcode": True,
            },
            "endpoints": {
                "raw_data_dir": raw_data_dir,
                "dest_data_dir": (
                    "s3://aind-transfer-test/"
                    "exaSPIM_125L_2022-08-05_17-25-36"
                ),
                "code_repo_location": self.code_repo_url,
            },
            "data": {"name": "imaging"},
            "transcode_job": {
                "compressor": {
                    "compressor_name": "blosc",
                    "kwargs": {
                        "cname": "zstd",
                        "clevel": 1,
                        "shuffle": Blosc.SHUFFLE,
                    },
                },
                "chunk_size": 64,
                "resume": False,
                "n_levels": 8,
                "submit_args": {
                    "conda_activate": (
                        "/allen/programs/aind/workgroups/msma/"
                        "cameron.arshadi/miniconda3/bin/"
                        "activate"
                    ),
                    "conda_env": "aind-data-transfer",
                    "cpus_per_task": 1,
                    "mail_user": "cameron.arshadi@alleninstitute.org",
                    "mem_per_cpu": 3000,
                    "nodes": 8,
                    "ntasks_per_node": 8,
                    "run_parent_dir": (
                        "/home/cameron.arshadi/"
                        "exaSPIM-transcode-jobs/"
                        "exaSPIM_125L_2022-08-05_17-25-36"
                    ),
                    "tmp_space": "8GB",
                    "walltime": "72:00:00",
                    "queue": "aind",
                },
            },
        }
        conf_file_path = (
            CONFIGS_DIR / "imaging" / "transcode_job_test_config.yml"
        )

        args = ["-c", str(conf_file_path)]

        loaded_configs = ImagingJobConfigurationLoader().load_configs(args)
        self.assertEqual(loaded_configs, expected_configs)


if __name__ == "__main__":
    unittest.main()
