import datetime
import unittest
from unittest import mock
from pathlib import Path
import os

from transfer.transformations.metadata_creation import ProcessingMetadata
from transfer.configuration_loader import EphysJobConfigurationLoader

import json

TEST_DIR = Path(os.path.dirname(os.path.realpath(__file__)))
CONFIGS_DIR = TEST_DIR / "resources" / "test_configs"
METADATA_DIR = TEST_DIR / "resources" / "test_metadata"

with open(METADATA_DIR / "processing_schema.json", "r") as f:
    processing_schema = json.load(f)

with open(METADATA_DIR / "processing.json", "r") as f:
    expected_processing_instance = json.load(f)


class TestMetadataSchemaClient(unittest.TestCase):

    def mocked_request_get(self):
        class MockResponse:
            def __init__(self, message, status_code):
                self.message = message
                self.status_code = status_code

            def json(self):
                return self.message

        json_contents = processing_schema

        return MockResponse(status_code=200, message=json_contents)

    conf_file_path = CONFIGS_DIR / "ephys_upload_job_test_configs.yml"
    args = ["-c", str(conf_file_path)]
    loaded_configs = EphysJobConfigurationLoader().load_configs(args)

    @mock.patch('requests.get')
    def test_create_processing_metadata(self, mocked_request_get):
        mocked_request_get.return_value = self.mocked_request_get()
        start_date_time = datetime.datetime.fromisoformat("2020-10-20T00:00:00.000+00:00")
        end_date_time = datetime.datetime.fromisoformat("2020-10-20T01:00:00.000+00:00")
        input_location = "some_input_location"
        output_location = "some_output_location"
        code_url = "https://github.com/AllenNeuralDynamics/nd-data-transfer"
        parameters = self.loaded_configs

        processing_metadata_instance = ProcessingMetadata.ephys_job_to_processing(
            schema_url="some_schema_url",
            start_date_time=start_date_time,
            end_date_time=end_date_time,
            input_location=input_location,
            output_location=output_location,
            code_url=code_url,
            parameters=parameters
        )

        self.assertEqual(expected_processing_instance,
                         processing_metadata_instance)
