"""This module will have classes that handle mapping to metadata files."""
import logging
import re
from datetime import datetime
from typing import Optional
import json

import pydantic
from aind_data_schema.data_description import (
    Funding,
    Institution,
    RawDataDescription,
)
from aind_data_schema.processing import DataProcess, Processing, ProcessName
from aind_data_schema.subject import Subject
from aind_data_schema.procedures import Procedures
from aind_metadata_service.client import AindMetadataServiceClient

import aind_data_transfer
from pathlib import Path


class ProcessingMetadata:
    """Class to handle the creation of the processing metadata file."""

    # TODO: import this from aind_data_schema.Processing class
    output_file_name = Processing.construct().default_filename()

    @staticmethod
    def ephys_job_to_processing(
        start_date_time: datetime,
        end_date_time: datetime,
        input_location: str,
        output_location: str,
        code_url: str,
        parameters: dict,
        notes: str = None,
    ) -> Processing:
        """
        Creates a processing instance based on the openephys_job settings
        Parameters
        ----------
        start_date_time : datetime
          Time the processing job started
        end_date_time : datetime
          Time the processing job ended
        input_location : str
          Location of the raw data source
        output_location : str
          Location of the processed data
        code_url : str
          Location of where the code is maintained.
          TODO: Extract this from pyproject.toml file.
        parameters : dict
          Parameters of the processing job
        notes : Optional[str]
            Optional notes to attach
        Returns
        -------
        aind_data_schema.Processing
          A Processing instance to annotate a dataset with.

        """
        data_processing_instance = DataProcess(
            name=ProcessName.EPHYS_PREPROCESSING.value,
            version=aind_data_transfer.__version__,
            start_date_time=start_date_time,
            end_date_time=end_date_time,
            input_location=input_location,
            output_location=output_location,
            code_url=code_url,
            parameters=parameters,
            notes=notes,
        )
        processing_instance = Processing(
            data_processes=[data_processing_instance]
        )

        return processing_instance


class RawDataDescriptionMetadata:
    """Class to handle the creation of the processing metadata file."""

    output_file_name = RawDataDescription.construct().default_filename()

    @staticmethod
    def get_data_description(
        name: str,
        institution=Institution.AIND,
        funding_source=(Funding(funder=Institution.AIND.value),),
    ) -> RawDataDescription:
        """
        Creates a data description instance
        Parameters
        ----------
        name : str
          Name of data, also the name of the directory containing all metadata
        institution : str
          The name of the organization that collected this data
        funding_source : tuple
          Funding sources. If internal label as Institution
        Returns
        -------
        aind_data_schema.RawDataDescription
          A RawDataDescription instance to annotate a dataset with.

        """
        funding_source_list = (
            list(funding_source)
            if isinstance(funding_source, tuple)
            else funding_source
        )
        data_description_instance = RawDataDescription.from_name(
            name,
            institution=institution,
            funding_source=funding_source_list,
        )
        return data_description_instance


class ProceduresMetadata:

    def __init__(self, procedures_info: Optional[dict] = None):
        self.procedures_info = procedures_info

    @property
    def output_filename(self):
        return Procedures.construct().default_filename()

    @classmethod
    def from_file(cls, file_location: Path):
        with open(file_location) as f:
            contents = json.load(f)
        return cls(procedures_info=contents)

    @classmethod
    def from_service(cls, subject_id, domain):
        ams_client = AindMetadataServiceClient(domain=domain)
        response = ams_client.get_procedures(subject_id=subject_id)
        response_json = response.json()
        status_code = response.status_code
        # Everything is okay
        if status_code == 200:
            contents = response_json["data"]
        # Multiple items were found
        elif status_code == 300:
            logging.warning(response_json["message"])
            contents = response_json["data"][0]
        # The data retrieved is invalid
        elif status_code == 406:
            logging.warning(response_json["message"])
            contents = response_json["data"]
        # Connected to the service, but no data was found
        elif status_code == 404:
            logging.warning(response_json["message"])
            contents = response_json["data"]
        else:
            logging.error(response_json["message"])
            contents = json.loads(Procedures.construct().json())
        return cls(procedures_info=contents)

    def validate_obj(self) -> bool:
        *_, validation_error = (
            pydantic.validate_model(Procedures, self.procedures_info)
        )
        if validation_error:
            logging.warning(f"Validation Errors: {validation_error}")
            return False
        else:
            logging.info("Procedures model is valid.")
            return True


class SubjectMetadata:

    def __init__(self, subject_info: Optional[dict] = None):
        self.subject_info = subject_info

    @property
    def output_filename(self):
        return Subject.construct().default_filename()

    @classmethod
    def from_file(cls, file_location: Path):
        with open(file_location) as f:
            contents = json.load(f)
        return cls(subject_info=contents)

    @classmethod
    def from_service(cls, subject_id, domain):
        ams_client = AindMetadataServiceClient(domain=domain)
        response = ams_client.get_subject(subject_id=subject_id)
        response_json = response.json()
        status_code = response.status_code
        # Everything is okay
        if status_code == 200:
            contents = response_json["data"]
        # Multiple items were found
        elif status_code == 300:
            logging.warning(response_json["message"])
            contents = response_json["data"][0]
        # The data retrieved is invalid
        elif status_code == 406:
            logging.warning(response_json["message"])
            contents = response_json["data"]
        # Connected to the service, but no data was found
        elif status_code == 404:
            logging.warning(response_json["message"])
            contents = response_json["data"]
        else:
            logging.error(response_json["message"])
            contents = json.loads(Subject.construct().json())
        return cls(subject_info=contents)

    def validate_obj(self) -> bool:
        *_, validation_error = (
            pydantic.validate_model(Subject, self.subject_info)
        )
        if validation_error:
            logging.warning(f"Validation Errors: {validation_error}")
            return False
        else:
            logging.info("Subject model is valid.")
            return True
