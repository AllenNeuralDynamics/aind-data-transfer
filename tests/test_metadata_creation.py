"""Module to test that Processing metadata is processed correctly."""
import datetime
import json
import os
import unittest
from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

from aind_data_schema.core.data_description import Funding, RawDataDescription
from aind_data_schema.core.procedures import Procedures
from aind_data_schema.core.processing import Processing
from aind_data_schema.core.subject import Subject
from aind_data_schema.models.modalities import Modality
from aind_data_schema.models.organizations import Organization
from aind_data_schema.models.process_names import ProcessName
from requests import ConnectionError, Response

from aind_data_transfer import __version__
from aind_data_transfer.config_loader.ephys_configuration_loader import (
    EphysJobConfigurationLoader,
)
from aind_data_transfer.transformations.metadata_creation import (
    ProceduresMetadata,
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

with open(METADATA_DIR / "metadata.json", "r") as f:
    expected_metadata_instance_json = json.load(f)

with open(METADATA_DIR / "subject.json", "r") as f:
    expected_subject_instance_json = json.load(f)

with open(METADATA_DIR / "procedures.json", "r") as f:
    expected_procedures_instance_json = json.load(f)


class TestProcessingMetadata(unittest.TestCase):
    """Tests methods in ProcessingMetadata class"""

    conf_file_path = CONFIGS_DIR / "ephys_upload_job_test_configs.yml"
    args = ["-c", str(conf_file_path)]

    @patch(
        "aind_data_transfer.config_loader.ephys_configuration_loader."
        "EphysJobConfigurationLoader._get_endpoints"
    )
    def test_create_processing_metadata(self, mock_get_endpoints) -> None:
        """Tests that the processing metadata is created correctly."""

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
        processor_full_name = "some name"
        code_url = "https://github.com/AllenNeuralDynamics/aind-data-transfer"

        parameters = loaded_configs

        processing_metadata = ProcessingMetadata.from_inputs(
            process_name=ProcessName.EPHYS_PREPROCESSING,
            start_date_time=start_date_time,
            end_date_time=end_date_time,
            input_location=input_location,
            output_location=output_location,
            processor_full_name=processor_full_name,
            code_url=code_url,
            parameters=parameters,
        )

        expected_processing_instance = Processing.model_validate(
            expected_processing_instance_json
        )
        expected_processing_instance.processing_pipeline.data_processes[
            0
        ].software_version = __version__
        self.assertEqual(
            json.loads(expected_processing_instance.model_dump_json()),
            processing_metadata.model_obj,
        )
        self.assertEqual(Processing, processing_metadata._model())
        self.assertEqual(
            "processing.json", processing_metadata.output_filename
        )


class TestSubjectMetadata(unittest.TestCase):
    """Tests methods in SubjectMetadata class"""

    successful_response_message = {
        "message": "Valid Model.",
        "data": {
            "describedBy": "https://raw.githubusercontent.com/AllenNeuralDynamics/aind-data-schema/main/src/aind_data_schema/subject.py",
            "schema_version": "0.4.2",
            "species": {
                "name": "Mus musculus",
                "abbreviation": None,
                "registry": {
                    "name": "National Center for Biotechnology Information",
                    "abbreviation": "NCBI",
                },
                "registry_identifier": "10090",
            },
            "subject_id": "632269",
            "sex": "Female",
            "date_of_birth": "2022-05-01",
            "genotype": "Pvalb-IRES-Cre/wt;RCL-somBiPoles_mCerulean-WPRE/wt",
            "mgi_allele_ids": None,
            "background_strain": None,
            "source": None,
            "rrid": None,
            "restrictions": None,
            "breeding_group": "Pvalb-IRES-Cre;RCL-somBiPoles_mCerulean-WPRE(ND)",
            "maternal_id": "615310",
            "maternal_genotype": "Pvalb-IRES-Cre/wt",
            "paternal_id": "623236",
            "paternal_genotype": "RCL-somBiPoles_mCerulean-WPRE/wt",
            "wellness_reports": None,
            "housing": None,
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

    @patch("os.path.isdir")
    @patch("builtins.open", new_callable=mock_open())
    @patch(
        "aind_metadata_service.client.AindMetadataServiceClient.get_subject"
    )
    @patch("logging.info")
    @patch("logging.warning")
    def test_successful_response(
        self,
        mock_log_warning: MagicMock,
        mock_log_info: MagicMock,
        mock_api_get: MagicMock,
        mock_open: MagicMock,
        mock_os: MagicMock,
    ) -> None:
        """Tests parsing successful response from metadata service. Currently,
        responses from the metadata service are several aind-data-schema
        versions behind"""

        successful_response = Response()
        successful_response.status_code = 200
        successful_response._content = json.dumps(
            self.successful_response_message
        ).encode("utf-8")

        mock_api_get.return_value = successful_response

        actual_subject = SubjectMetadata.from_service(
            "632269", "http://a-fake-url"
        )
        is_model_valid = actual_subject.validate_obj()
        # Mock writing out to a directory
        mock_os.side_effect = [True, False]
        # Each call to write_to_json makes a call to validate_obj()
        actual_subject.write_to_json(Path("/some_path/"))
        actual_subject.write_to_json(Path("/some_path/subject2.json"))

        expected_subject = self.successful_response_message["data"]

        # We can update this once aind-metadata-service is updated
        mock_log_info.assert_not_called()
        mock_log_warning.assert_called()
        mock_open.assert_called()
        self.assertEqual(expected_subject, actual_subject.model_obj)
        self.assertFalse(is_model_valid)

    @patch("logging.warning")
    @patch(
        "aind_metadata_service.client.AindMetadataServiceClient.get_subject"
    )
    def test_multiple_response_warning(
        self,
        mock_api_get: MagicMock,
        mock_log_warn: MagicMock,
    ) -> None:
        """Tests parsing multiples subjects from metadata service."""

        multiple_response = Response()
        multiple_response.status_code = 300
        multiple_response._content = json.dumps(
            self.multiple_subjects_response
        ).encode("utf-8")

        mock_api_get.return_value = multiple_response

        actual_subject = SubjectMetadata.from_service(
            "632269", "http://a-fake-url"
        ).model_obj

        expected_subject = self.successful_response_message["data"]
        mock_log_warn.assert_called_once_with(
            "SubjectMetadata: Multiple Items Found."
        )
        self.assertEqual(expected_subject, actual_subject)

    @patch("logging.warning")
    @patch(
        "aind_metadata_service.client.AindMetadataServiceClient.get_subject"
    )
    def test_invalid_response_warning(
        self,
        mock_api_get: MagicMock,
        mock_log_warn: MagicMock,
    ) -> None:
        """Tests parsing invalid Subject from metadata service."""

        invalid_response = Response()
        invalid_response.status_code = 406
        msg = self.successful_response_message
        msg["message"] = "Validation Errors: Errors here!"
        invalid_response._content = json.dumps(
            self.successful_response_message
        ).encode("utf-8")

        mock_api_get.return_value = invalid_response

        actual_subject = SubjectMetadata.from_service(
            "632269", "http://a-fake-url"
        ).model_obj

        expected_subject = self.successful_response_message["data"]
        mock_log_warn.assert_called_once_with(
            "SubjectMetadata: Validation Errors: Errors here!"
        )
        self.assertEqual(expected_subject, actual_subject)

    @patch("logging.warning")
    @patch("logging.error")
    @patch(
        "aind_metadata_service.client.AindMetadataServiceClient.get_subject"
    )
    def test_server_response_warning(
        self,
        mock_api_get: MagicMock,
        mock_log_err: MagicMock,
        mock_log_warn: MagicMock,
    ) -> None:
        """Tests parsing server error response from metadata service."""

        err_response = Response()
        err_response.status_code = 500
        err_message = {"message": "Internal Server Error.", "data": None}
        err_response._content = json.dumps(err_message).encode("utf-8")
        mock_api_get.return_value = err_response

        actual_subject = SubjectMetadata.from_service(
            "632269", "http://a-fake-url"
        )
        expected_subject = Subject.model_construct().model_dump()
        is_model_valid = actual_subject.validate_obj()

        mock_log_err.assert_called_once_with(
            "SubjectMetadata: Internal Server Error."
        )
        mock_log_warn.assert_called_once()
        self.assertEqual(expected_subject, actual_subject.model_obj)
        self.assertEqual("subject.json", actual_subject.output_filename)
        self.assertFalse(is_model_valid)

    @patch("logging.error")
    @patch(
        "aind_metadata_service.client.AindMetadataServiceClient.get_subject"
    )
    def test_no_response_warning(
        self,
        mock_api_get: MagicMock,
        mock_log_err: MagicMock,
    ) -> None:
        """Tests parsing no response from metadata service."""

        mock_api_get.side_effect = ConnectionError("Unable to connect")

        actual_subject = SubjectMetadata.from_service(
            "632269", "http://a-fake-url"
        ).model_obj
        expected_subject = Subject.model_construct().model_dump()

        mock_log_err.assert_called_once_with(
            "SubjectMetadata: An error occurred connecting to metadata "
            "service: Unable to connect"
        )
        self.assertEqual(expected_subject, actual_subject)


class TestDataDescriptionMetadata(unittest.TestCase):
    """Tests methods in DataDescriptionMetadata class"""

    def test_create_data_description_metadata(self) -> None:
        """
        Tests that the data description metadata is created correctly.
        """
        data_description = RawDataDescriptionMetadata.from_inputs(
            name="exaSPIM_12345_2022-02-21_16-30-01",
            investigators=["John Apple"],
            modality=[Modality.SPIM],
            funding_source=(Funding(funder=Organization.AI),),
        )

        expected_data_description_instance = RawDataDescription.model_validate(
            expected_data_description_instance_json
        )

        self.assertEqual(
            json.loads(expected_data_description_instance.model_dump_json()),
            data_description.model_obj,
        )
        self.assertEqual(RawDataDescription, data_description._model())
        self.assertEqual(
            "data_description.json", data_description.output_filename
        )


class TestProceduresMetadata(unittest.TestCase):
    """Tests methods in the ProceduresMetadata class."""

    successful_response_message = {
        "message": "Valid Model.",
        "data": {
            "describedBy": "https://raw.githubusercontent.com/AllenNeuralDynamics/aind-data-schema/main/src/aind_data_schema/procedures.py",
            "schema_version": "0.9.2",
            "subject_id": "436083",
            "subject_procedures": [
                {
                    "start_date": "2019-01-09",
                    "end_date": "2019-01-09",
                    "experimenter_full_name": "NSB-118",
                    "iacuc_protocol": None,
                    "animal_weight_prior": 21.3,
                    "animal_weight_post": 23,
                    "weight_unit": "gram",
                    "anaesthesia": {
                        "type": "isoflurane",
                        "duration_unit": "minute",
                        "level": 1.5,
                    },
                    "notes": None,
                    "procedure_type": "Headframe",
                    "headframe_type": "CAM-style",
                    "headframe_part_number": "0160-100-10 Rev A",
                    "headframe_material": None,
                    "well_part_number": None,
                    "well_type": "CAM-style",
                },
                {
                    "start_date": "2019-01-09",
                    "end_date": "2019-01-09",
                    "experimenter_full_name": "NSB-118",
                    "iacuc_protocol": None,
                    "animal_weight_prior": 21.3,
                    "animal_weight_post": 23,
                    "weight_unit": "gram",
                    "anaesthesia": {
                        "type": "isoflurane",
                        "duration": None,
                        "duration_unit": "minute",
                        "level": 1.5,
                    },
                    "notes": None,
                    "injection_materials": [
                        {
                            "material_id": None,
                            "full_genome_name": None,
                            "plasmid_name": None,
                            "genome_copy": None,
                            "titer": None,
                            "titer_unit": "gc/mL",
                            "prep_lot_number": None,
                            "prep_date": None,
                            "prep_type": None,
                            "prep_protocol": None,
                        }
                    ],
                    "recovery_time": None,
                    "recovery_time_unit": "minute",
                    "injection_duration": 10,
                    "injection_duration_unit": "minute",
                    "workstation_id": "SWS 6",
                    "instrument_id": "NJ#6",
                    "injection_coordinate_ml": -2.3,
                    "injection_coordinate_ap": -2.3,
                    "injection_coordinate_depth": 2.6,
                    "injection_coordinate_unit": "millimeter",
                    "injection_coordinate_reference": None,
                    "bregma_to_lambda_distance": 4.22,
                    "bregma_to_lambda_unit": "millimeter",
                    "injection_angle": 0,
                    "injection_angle_unit": "degree",
                    "targeted_structure": None,
                    "injection_hemisphere": "Left",
                    "procedure_type": "Nanoject injection",
                    "injection_volume": 200,
                    "injection_volume_unit": "nanoliter",
                },
                {
                    "start_date": "2019-01-09",
                    "end_date": "2019-01-09",
                    "experimenter_full_name": "NSB-118",
                    "iacuc_protocol": None,
                    "animal_weight_prior": 21.3,
                    "animal_weight_post": 23,
                    "weight_unit": "gram",
                    "anaesthesia": {
                        "type": "isoflurane",
                        "duration_unit": "minute",
                        "level": 1.5,
                    },
                    "notes": None,
                    "procedure_type": "Craniotomy",
                    "craniotomy_type": "Visual Cortex",
                    "craniotomy_hemisphere": "Left",
                    "craniotomy_coordinates_ml": 2.8,
                    "craniotomy_coordinates_ap": 1.3,
                    "craniotomy_coordinates_unit": "millimeter",
                    "craniotomy_coordinates_reference": "Lambda",
                    "bregma_to_lambda_distance": 4.22,
                    "bregma_to_lambda_unit": "millimeter",
                    "craniotomy_size": None,
                    "craniotomy_size_unit": "millimeter",
                    "implant_part_number": None,
                    "dura_removed": True,
                    "protective_material": None,
                    "workstation_id": "SWS 6",
                    "recovery_time": None,
                    "recovery_time_unit": "minute",
                },
            ],
            "specimen_procedures": [],
            "notes": None,
        },
    }

    @patch(
        "aind_metadata_service.client.AindMetadataServiceClient.get_procedures"
    )
    def test_procedures_from_service(self, mock_api_get: MagicMock):
        """Tests that the procedures is generated from service call."""

        successful_response = Response()
        successful_response.status_code = 200
        successful_response._content = json.dumps(
            self.successful_response_message
        ).encode("utf-8")

        mock_api_get.return_value = successful_response

        procedures = ProceduresMetadata.from_service(
            "436083", "http://a-fake-url"
        )

        expected_subject = self.successful_response_message["data"]

        self.assertEqual(expected_subject, procedures.model_obj)
        self.assertEqual(Procedures, procedures._model())
        self.assertEqual("procedures.json", procedures.output_filename)


if __name__ == "__main__":
    unittest.main()
