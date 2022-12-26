"""Module to test that Processing metadata is processed correctly."""
import datetime
import json
import os
import unittest
from pathlib import Path
from unittest import mock

import requests
from aind_data_schema import Processing, RawDataDescription

from aind_data_transfer.config_loader.ephys_configuration_loader import (
    EphysJobConfigurationLoader,
)
from aind_data_transfer.transformations.metadata_creation import (
    DataDescriptionMetadata,
    ProcessingMetadata,
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

    @mock.patch("requests.get")
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

        actual_subject = SubjectMetadata.ephys_job_to_subject(
            metadata_service_url="http://a-fake-url",
            filepath="ecephys_632269_2022-10-10_16-13-22",
        )

        expected_subject = self.successful_response_message["data"]

        self.assertEqual(expected_subject, actual_subject)

    @mock.patch("logging.warning")
    @mock.patch("requests.get")
    def test_multiple_response_warning(
        self,
        mock_api_get: unittest.mock.MagicMock,
        mock_log_warn: unittest.mock.MagicMock,
    ) -> None:
        """Tests parsing multiples subjects from metadata service."""

        multiple_response = requests.Response()
        multiple_response.status_code = 418
        multiple_response._content = json.dumps(
            self.multiple_subjects_response
        ).encode("utf-8")

        mock_api_get.return_value = multiple_response

        actual_subject = SubjectMetadata.ephys_job_to_subject(
            metadata_service_url="http://a-fake-url",
            filepath="ecephys_632269_2022-10-10_16-13-22",
        )

        expected_subject = self.successful_response_message["data"]
        mock_log_warn.assert_called_once_with("Multiple Items Found.")
        self.assertEqual(expected_subject, actual_subject)

    @mock.patch("logging.warning")
    @mock.patch("requests.get")
    def test_invalid_response_warning(
        self,
        mock_api_get: unittest.mock.MagicMock,
        mock_log_warn: unittest.mock.MagicMock,
    ) -> None:
        """Tests parsing invalid Subject from metadata service."""

        invalid_response = requests.Response()
        invalid_response.status_code = 418
        msg = self.successful_response_message
        msg["message"] = "Validation Errors: Errors here!"
        invalid_response._content = json.dumps(
            self.successful_response_message
        ).encode("utf-8")

        mock_api_get.return_value = invalid_response

        actual_subject = SubjectMetadata.ephys_job_to_subject(
            metadata_service_url="http://a-fake-url",
            filepath="ecephys_632269_2022-10-10_16-13-22",
        )

        expected_subject = self.successful_response_message["data"]
        mock_log_warn.assert_called_once_with(
            "Validation Errors: Errors here!"
        )
        self.assertEqual(expected_subject, actual_subject)

    @mock.patch("logging.error")
    @mock.patch("requests.get")
    def test_no_response_warning(
        self,
        mock_api_get: unittest.mock.MagicMock,
        mock_log_err: unittest.mock.MagicMock,
    ) -> None:
        """Tests parsing no response from metadata service."""

        no_response = requests.Response()
        no_response.status_code = 500

        mock_api_get.return_value = no_response

        actual_subject = SubjectMetadata.ephys_job_to_subject(
            metadata_service_url="http://a-fake-url",
            filepath="ecephys_632269_2022-10-10_16-13-22",
        )

        mock_log_err.assert_called_once_with("No data retrieved!")
        self.assertIsNone(actual_subject)


class TestDataDescriptionMetadata(unittest.TestCase):
    """Tests methods in DataDescriptionMetadata class"""

    def test_create_data_description_metadata(self) -> None:
        """
        Tests that the data description metadata is created correctly.

        Returns:

        """
        data_description_instance = (
            DataDescriptionMetadata.ephys_job_to_data_description(
                name="ecephys_0000_2022-10-20_16-30-01"
            )
        )

        expected_data_description_instance = RawDataDescription.parse_obj(
            expected_data_description_instance_json
        )

        self.assertEqual(
            expected_data_description_instance, data_description_instance
        )


if __name__ == "__main__":
    unittest.main()
