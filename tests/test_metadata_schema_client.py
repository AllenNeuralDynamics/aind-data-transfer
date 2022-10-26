"""Module to test that Processing metadata is processed correctly."""

import datetime
import json
import os
import unittest
from pathlib import Path
from unittest import mock

from aind_data_transfer.configuration_loader import EphysJobConfigurationLoader
from aind_data_transfer.transformations.metadata_creation import ProcessingMetadata

TEST_DIR = Path(os.path.dirname(os.path.realpath(__file__)))
CONFIGS_DIR = TEST_DIR / "resources" / "test_configs"
METADATA_DIR = TEST_DIR / "resources" / "test_metadata"

with open(METADATA_DIR / "processing_schema.json", "r") as f:
    processing_schema = json.load(f)

with open(METADATA_DIR / "processing.json", "r") as f:
    expected_processing_instance = json.load(f)


class TestProcessingMetadata(unittest.TestCase):
    """Tests methods in ProcessingMetadata class"""

    def mocked_request_get(self):
        """Mock the return of a request.get"""

        class MockResponse:
            def __init__(self, message: dict, status_code: int) -> None:
                self.message = message
                self.status_code = status_code

            def json(self):
                return self.message

        json_contents = processing_schema

        return MockResponse(status_code=200, message=json_contents)

    conf_file_path = CONFIGS_DIR / "ephys_upload_job_test_configs.yml"
    args = ["-c", str(conf_file_path)]
    loaded_configs = EphysJobConfigurationLoader().load_configs(args)

    @mock.patch("requests.get")
    def test_create_processing_metadata(
        self, mocked_request_get: mock.Mock
    ) -> None:
        """
        Tests that the processing metadata is created correctly.
        Args:
            mocked_request_get (mock.Mock): The mocked request

        Returns:

        """
        mocked_request_get.return_value = self.mocked_request_get()
        start_date_time = datetime.datetime.fromisoformat(
            "2020-10-20T00:00:00.000+00:00"
        )
        end_date_time = datetime.datetime.fromisoformat(
            "2020-10-20T01:00:00.000+00:00"
        )
        input_location = "some_input_location"
        output_location = "some_output_location"
        code_url = "https://github.com/AllenNeuralDynamics/aind-data-transfer"
        schema_url = (
            "https://raw.githubusercontent.com/AllenNeuralDynamics/"
            "data_schema/main/schemas"
        )
        parameters = self.loaded_configs

        processing_metadata = ProcessingMetadata(schema_url=schema_url)

        processing_metadata_instance = (
            processing_metadata.ephys_job_to_processing(
                start_date_time=start_date_time,
                end_date_time=end_date_time,
                input_location=input_location,
                output_location=output_location,
                code_url=code_url,
                parameters=parameters,
            )
        )

        # Hack to get match version to be the same as in the example file
        processing_metadata_instance["data_processes"][0]["version"] = "0.0.3"

        self.assertEqual(
            expected_processing_instance, processing_metadata_instance
        )


if __name__ == "__main__":
    unittest.main()
