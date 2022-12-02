"""Module to test that Processing metadata is processed correctly."""

import datetime
import json
import os
import unittest
from pathlib import Path

from aind_data_schema import Processing

from aind_data_transfer.config_loader.ephys_configuration_loader import (
    EphysJobConfigurationLoader,
)
from aind_data_transfer.transformations.metadata_creation import (
    ProcessingMetadata,
)

TEST_DIR = Path(os.path.dirname(os.path.realpath(__file__)))
CONFIGS_DIR = TEST_DIR / "resources" / "test_configs"
METADATA_DIR = TEST_DIR / "resources" / "test_metadata"

with open(METADATA_DIR / "processing.json", "r") as f:
    expected_processing_instance_json = json.load(f)


class TestProcessingMetadata(unittest.TestCase):
    """Tests methods in ProcessingMetadata class"""

    conf_file_path = CONFIGS_DIR / "ephys_upload_job_test_configs.yml"
    args = ["-c", str(conf_file_path)]
    loaded_configs = EphysJobConfigurationLoader().load_configs(args)

    def test_create_processing_metadata(self) -> None:
        """
        Tests that the processing metadata is created correctly.

        Returns:

        """

        start_date_time = datetime.datetime.fromisoformat(
            "2020-10-20T00:00:00.000+00:00"
        )
        end_date_time = datetime.datetime.fromisoformat(
            "2020-10-20T01:00:00.000+00:00"
        )
        input_location = "some_input_location"
        output_location = "some_output_location"
        code_url = "https://github.com/AllenNeuralDynamics/aind-data-transfer"

        parameters = self.loaded_configs

        processing_instance = ProcessingMetadata.ephys_job_to_processing(
            start_date_time=start_date_time,
            end_date_time=end_date_time,
            input_location=input_location,
            output_location=output_location,
            code_url=code_url,
            parameters=parameters,
        )

        # Hack to get match version to be the same as in the example file
        expected_processing_instance_json["data_processes"][0][
            "version"
        ] = processing_instance.data_processes[0].version

        expected_processing_instance = Processing.parse_obj(
            expected_processing_instance_json
        )

        self.assertEqual(expected_processing_instance, processing_instance)


if __name__ == "__main__":
    unittest.main()
