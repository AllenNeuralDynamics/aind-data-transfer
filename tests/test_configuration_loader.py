"""Tests configurations are set properly"""
import os
import unittest
from pathlib import Path

from numcodecs import Blosc

from aind_data_transfer.configuration_loader import EphysJobConfigurationLoader, ImagingJobConfigurationLoader

TEST_DIR = Path(os.path.dirname(os.path.realpath(__file__)))
CONFIGS_DIR = TEST_DIR / "resources" / "test_configs"


class TestEphysJobConfigs(unittest.TestCase):
    """Tests ephys job pipeline methods"""

    metadata_schemas_url = (
        "https://raw.githubusercontent.com/AllenNeuralDynamics/"
        "data_schema/main/schemas"
    )
    code_repo_url = "https://github.com/AllenNeuralDynamics/aind-data-transfer"

    def test_conf_loads(self):
        """Basic config loads test"""

        raw_data_dir = (
            "tests/resources/v0.6.x_neuropixels_multiexp_multistream"
        )
        dest_data_dir = (
            "tests/resources/new/v0.6.x_neuropixels_multiexp_multistream"
        )

        expected_configs = {
            "jobs": {
                "clip": True,
                "compress": True,
                "attach_metadata": True,
                "upload_to_s3": True,
                "upload_to_gcp": True,
                "register_to_codeocean": False,
                "trigger_codeocean_spike_sorting": False
            },
            "endpoints": {
                "raw_data_dir": raw_data_dir,
                "dest_data_dir": dest_data_dir,
                "s3_bucket": "aind-transfer-test",
                "s3_prefix": "v0.6.x_neuropixels_multiexp_multistream",
                "gcp_bucket": "aind-data-dev",
                "gcp_prefix": "test_20221001",
                "codeocean_domain": "https://acmecorp.codeocean.com",
                "metadata_schemas": self.metadata_schemas_url,
                "code_repo_location": self.code_repo_url,
            },
            "data": {"name": "openephys"},
            "clip_data_job": {"clip_kwargs": {}},
            "compress_data_job": {
                "write_kwargs": {
                    "n_jobs": -1,
                    "chunk_duration": "1s",
                    "progress_bar": True,
                },
                "format_kwargs": {},
                "compressor": {
                    "compressor_name": "blosc",
                    "kwargs": {"shuffle": Blosc.BITSHUFFLE},
                },
                "scale_params": {"chunk_size": 20},
            },
            "upload_data_job": {"dryrun": True},
            "register_on_codeocean_job": {
                "tags": ["ecephys", "raw"],
                "asset_name": "v0.6.x_neuropixels_multiexp_multistream",
                "mount": "v0.6.x_neuropixels_multiexp_multistream",
            },
            "trigger_codeocean_spike_sorting_job": {"mount": "v0.6.x_neuropixels_multiexp_multistream"},
            "logging": {"level": "INFO"}
        }
        conf_file_path = CONFIGS_DIR / "ephys_upload_job_test_configs.yml"

        args = ["-c", str(conf_file_path)]

        loaded_configs = EphysJobConfigurationLoader().load_configs(args)
        self.assertEqual(loaded_configs, expected_configs)

    def test_endpoints_are_resolved(self):
        """Tests default endpoints are resolved correctly"""

        raw_data_dir = "/some/random/folder/625463_2022-10-06_10-14-25"
        expected_configs = {
            "jobs": {
                "clip": True,
                "compress": True,
                "attach_metadata": True,
                "upload_to_s3": True,
                "upload_to_gcp": True,
                "register_to_codeocean": False,
                "trigger_codeocean_spike_sorting": False
            },
            "endpoints": {
                "raw_data_dir": raw_data_dir,
                "dest_data_dir": "ecephys_625463_2022-10-06_10-14-25",
                "s3_bucket": "aind-ephys-data",
                "s3_prefix": "ecephys_625463_2022-10-06_10-14-25",
                "gcp_bucket": "aind-data-dev",
                "gcp_prefix": "ecephys_625463_2022-10-06_10-14-25",
                "codeocean_domain": "https://acmecorp.codeocean.com",
                "metadata_schemas": self.metadata_schemas_url,
                "code_repo_location": self.code_repo_url,
            },
            "data": {"name": "openephys"},
            "clip_data_job": {"clip_kwargs": {}},
            "compress_data_job": {
                "write_kwargs": {
                    "n_jobs": -1,
                    "chunk_duration": "1s",
                    "progress_bar": True,
                },
                "format_kwargs": {},
                "compressor": {
                    "compressor_name": "wavpack",
                    "kwargs": {"level": 3},
                },
                "scale_params": {},
            },
            "upload_data_job": {"dryrun": True},
            "register_on_codeocean_job": {
                "tags": ["ecephys"],
                "asset_name": "ecephys_625463_2022-10-06_10-14-25",
                "mount": "ecephys_625463_2022-10-06_10-14-25",
            },
            "trigger_codeocean_spike_sorting_job": {"mount": "ecephys_625463_2022-10-06_10-14-25"},
            "logging": {"level": "INFO"}
        }

        conf_file_path1 = CONFIGS_DIR / "example_configs_src_pattern1.yml"
        conf_file_path2 = CONFIGS_DIR / "example_configs_src_pattern2.yml"
        args1 = ["-c", str(conf_file_path1)]
        args2 = ["-c", str(conf_file_path2)]
        loaded_configs1 = EphysJobConfigurationLoader().load_configs(args1)
        loaded_configs2 = EphysJobConfigurationLoader().load_configs(args2)
        self.assertEqual(loaded_configs1, expected_configs)
        self.assertEqual(loaded_configs2, expected_configs)


class TestImagingJobConfigs(unittest.TestCase):
    metadata_schemas_url = (
        "https://raw.githubusercontent.com/AllenNeuralDynamics/"
        "data_schema/main/schemas"
    )
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
                "dest_data_dir": "s3://aind-transfer-test/exaSPIM_125L_2022-08-05_17-25-36",
                "metadata_schemas": self.metadata_schemas_url,
                "code_repo_location": self.code_repo_url,
            },
            "data": {"name": "imaging"},
            "transcode_job": {
                "compressor": {
                    "compressor_name": "blosc",
                    "kwargs": {"cname": "zstd", "clevel": 1, "shuffle": Blosc.SHUFFLE},
                },
                "chunk_size": 64,
                "resume": False,
                "n_levels": 8,
                'submit_args': {
                    'conda_activate': '/allen/programs/aind/workgroups/msma/cameron.arshadi/miniconda3/bin/activate',
                    'conda_env': 'aind-data-transfer',
                    'cpus_per_task': 1,
                    'mail_user': 'cameron.arshadi@alleninstitute.org',
                    'mem_per_cpu': 3000,
                    'nodes': 8,
                    'ntasks_per_node': 8,
                    'run_parent_dir': '/home/cameron.arshadi/exaSPIM-transcode-jobs/exaSPIM_125L_2022-08-05_17-25-36',
                    'tmp_space': '8GB',
                    'walltime': '72:00:00',
                    "queue": "aind"
                }
            }
        }
        conf_file_path = CONFIGS_DIR / "imaging" / "transcode_job_test_config.yml"

        args = ["-c", str(conf_file_path)]

        loaded_configs = ImagingJobConfigurationLoader().load_configs(args)
        self.assertEqual(loaded_configs, expected_configs)


if __name__ == "__main__":
    unittest.main()
