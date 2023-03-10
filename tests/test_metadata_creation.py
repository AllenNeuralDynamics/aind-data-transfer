"""Module to test that Processing metadata is processed correctly."""
import datetime
import json
import os
import unittest
from pathlib import Path
from unittest import mock

import requests
from aind_data_schema import Processing, RawDataDescription, Subject
from aind_data_schema.processing import ProcessName

from aind_data_transfer.config_loader.ephys_configuration_loader import (
    EphysJobConfigurationLoader,
)
from aind_data_transfer.transformations.metadata_creation import (
    ProcessingMetadata,
    RawDataDescriptionMetadata,
    SubjectMetadata,
)

TEST_DIR = Path(os.path.dirname(os.path.realpath(__file__)))
CONFIGS_DIR = TEST_DIR / "resources" / "test_configs"
METADATA_DIR = TEST_DIR / "resources" / "test_metadata"

with open(METADATA_DIR / "processing.json", "r") as f:
    expected_processing_instance_json = json.load(f)

with open(METADATA_DIR / "data_description.json", "r") as f:
    expected_data_description_instance_json = json.load(f)


class TestProcessingMetadata(unittest.TestCase):
    """Tests methods in ProcessingMetadata class"""

    conf_file_path = CONFIGS_DIR / "ephys_upload_job_test_configs.yml"
    args = ["-c", str(conf_file_path)]

    @mock.patch(
        "aind_data_transfer.config_loader.ephys_configuration_loader."
        "EphysJobConfigurationLoader._get_endpoints"
    )
    def test_create_processing_metadata(self, mock_get_endpoints) -> None:
        """
        Tests that the processing metadata is created correctly.

        Returns:

        """

        mock_get_endpoints.return_value = {"codeocean_trigger_capsule": None}

        loaded_configs = EphysJobConfigurationLoader().load_configs(self.args)

        start_date_time = datetime.datetime.fromisoformat(
            "2020-10-20T00:00:00.000+00:00"
        )
        end_date_time = datetime.datetime.fromisoformat(
            "2020-10-20T01:00:00.000+00:00"
        )
        input_location = "some_input_location"
        output_location = "some_output_location"
        code_url = "https://github.com/AllenNeuralDynamics/aind-data-transfer"

        parameters = loaded_configs

        processing_metadata_json = ProcessingMetadata.from_inputs(
            process_name=ProcessName.EPHYS_PREPROCESSING,
            start_date_time=start_date_time,
            end_date_time=end_date_time,
            input_location=input_location,
            output_location=output_location,
            code_url=code_url,
            parameters=parameters,
        ).model_obj

        # Hack to get match version to be the same as in the example file
        expected_processing_instance_json["data_processes"][0][
            "version"
        ] = processing_metadata_json["data_processes"][0]["version"]

        expected_processing_instance = Processing.parse_obj(
            expected_processing_instance_json
        )

        self.assertEqual(expected_processing_instance,
                         processing_metadata_json)


class TestSubjectMetadata(unittest.TestCase):
    """Tests methods in SubjectMetadata class"""

    successful_response_message = {
        "message": "Valid Model.",
        "data": {
            "describedBy": "https://github-location.org/subject.py",
            "schema_version": "0.2.2",
            "species": "Mus musculus",
            "subject_id": "632269",
            "sex": "Female",
            "date_of_birth": "2022-05-01",
            "genotype": "Pvalb-IRES-Cre/wt;RCL-somBiPoles_mCerulean-WPRE/wt",
            "mgi_allele_ids": None,
            "background_strain": None,
            "source": None,
            "rrid": None,
            "restrictions": None,
            "breeding_group": (
                "Pvalb-IRES-Cre;RCL-somBiPoles_mCerulean-WPRE(ND)"
            ),
            "maternal_id": "615310",
            "maternal_genotype": "Pvalb-IRES-Cre/wt",
            "paternal_id": "623236",
            "paternal_genotype": "RCL-somBiPoles_mCerulean-WPRE/wt",
            "light_cycle": None,
            "home_cage_enrichment": None,
            "wellness_reports": None,
            "notes": None,
        },
    }

    multiple_subjects_response = {
        "message": "Multiple Items Found.",
        "data": (
            [
                successful_response_message["data"],
                successful_response_message["data"],
            ]
        ),
    }

    @mock.patch(
        "aind_metadata_service.client.AindMetadataServiceClient.get_subject"
    )
    def test_successful_response(
        self, mock_api_get: unittest.mock.MagicMock
    ) -> None:
        """Tests parsing successful response from metadata service."""

        successful_response = requests.Response()
        successful_response.status_code = 200
        successful_response._content = json.dumps(
            self.successful_response_message
        ).encode("utf-8")

        mock_api_get.return_value = successful_response

        actual_subject = SubjectMetadata.from_service(
            "632269",
            "http://a-fake-url"
        ).model_obj

        expected_subject = self.successful_response_message["data"]

        self.assertEqual(expected_subject, actual_subject)

    @mock.patch("logging.warning")
    @mock.patch(
        "aind_metadata_service.client.AindMetadataServiceClient.get_subject"
    )
    def test_multiple_response_warning(
        self,
        mock_api_get: unittest.mock.MagicMock,
        mock_log_warn: unittest.mock.MagicMock,
    ) -> None:
        """Tests parsing multiples subjects from metadata service."""

        multiple_response = requests.Response()
        multiple_response.status_code = 300
        multiple_response._content = json.dumps(
            self.multiple_subjects_response
        ).encode("utf-8")

        mock_api_get.return_value = multiple_response

        actual_subject = SubjectMetadata.from_service(
            "632269",
            "http://a-fake-url"
        ).model_obj

        expected_subject = self.successful_response_message["data"]
        mock_log_warn.assert_called_once_with("Multiple Items Found.")
        self.assertEqual(expected_subject, actual_subject)

    @mock.patch("logging.warning")
    @mock.patch(
        "aind_metadata_service.client.AindMetadataServiceClient.get_subject"
    )
    def test_invalid_response_warning(
        self,
        mock_api_get: unittest.mock.MagicMock,
        mock_log_warn: unittest.mock.MagicMock,
    ) -> None:
        """Tests parsing invalid Subject from metadata service."""

        invalid_response = requests.Response()
        invalid_response.status_code = 406
        msg = self.successful_response_message
        msg["message"] = "Validation Errors: Errors here!"
        invalid_response._content = json.dumps(
            self.successful_response_message
        ).encode("utf-8")

        mock_api_get.return_value = invalid_response

        actual_subject = SubjectMetadata.from_service(
            "632269",
            "http://a-fake-url"
        ).model_obj

        expected_subject = self.successful_response_message["data"]
        mock_log_warn.assert_called_once_with(
            "Validation Errors: Errors here!"
        )
        self.assertEqual(expected_subject, actual_subject)

    @mock.patch("logging.error")
    @mock.patch(
        "aind_metadata_service.client.AindMetadataServiceClient.get_subject"
    )
    def test_server_response_warning(
        self,
        mock_api_get: unittest.mock.MagicMock,
        mock_log_err: unittest.mock.MagicMock,
    ) -> None:
        """Tests parsing server error response from metadata service."""

        err_response = requests.Response()
        err_response.status_code = 500
        err_message = {"message": "Internal Server Error.", "data": None}
        err_response._content = json.dumps(
            err_message
        ).encode("utf-8")
        mock_api_get.return_value = err_response

        actual_subject = SubjectMetadata.from_service(
            "632269",
            "http://a-fake-url"
        ).model_obj
        expected_subject = Subject.construct().dict()

        mock_log_err.assert_called_once_with(
            "Internal Server Error."
        )
        self.assertEqual(expected_subject, actual_subject)

    @mock.patch("logging.error")
    @mock.patch(
        "aind_metadata_service.client.AindMetadataServiceClient.get_subject"
    )
    def test_no_response_warning(
        self,
        mock_api_get: unittest.mock.MagicMock,
        mock_log_err: unittest.mock.MagicMock,
    ) -> None:
        """Tests parsing no response from metadata service."""

        mock_api_get.side_effect = requests.ConnectionError(
            "Unable to connect"
        )

        actual_subject = SubjectMetadata.from_service(
            "632269",
            "http://a-fake-url"
        ).model_obj
        expected_subject = Subject.construct().dict()

        mock_log_err.assert_called_once_with(
            "An error occured connecting to metadata service: Unable to "
            "connect"
        )
        self.assertEqual(expected_subject, actual_subject)


class TestDataDescriptionMetadata(unittest.TestCase):
    """Tests methods in DataDescriptionMetadata class"""

    def test_create_data_description_metadata(self) -> None:
        """
        Tests that the data description metadata is created correctly.
        """
        data_description = RawDataDescriptionMetadata.from_inputs(
            name="ecephys_0000_2022-10-20_16-30-01"
        )

        expected_data_description_instance = RawDataDescription.parse_obj(
            expected_data_description_instance_json
        )

        self.assertEqual(
            expected_data_description_instance, data_description.model_obj
        )


if __name__ == "__main__":
    unittest.main()
