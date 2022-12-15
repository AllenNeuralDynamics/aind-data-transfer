"""This module will have classes that handle mapping to metadata files."""
import logging
from datetime import datetime
import re

from aind_data_schema.processing import Processing, ProcessName, DataProcess
from aind_data_schema.subject import Subject
import requests
from pydantic import ValidationError

import aind_data_transfer
from typing import Optional


class ProcessingMetadata:
    """Class to handle the creation of the processing metadata file."""

    # TODO: import this from aind_data_schema.Processing class
    output_file_name = "processing.json"

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


class SubjectMetadata:

    output_file_name = "subject.json"

    @staticmethod
    def ephys_job_to_subject(metadata_service_url: str,
                             subject_id: Optional[str] = None,
                             filepath: Optional[str] = None,
                             file_subject_regex: Optional[str] = (
                          "(ecephys|ephys)*_*(\\d+)_\\d{4}")
                             ) -> Subject:

        # TODO: Import aind_metadata_service.client once it's written
        if subject_id is None:
            parsed_filepath = re.findall(file_subject_regex, filepath)
            subject_id = parsed_filepath[0][1]

        subject_url = metadata_service_url + f"/subject/{subject_id}"
        response = requests.get(subject_url)

        if response.status_code == 200:
            response_json = response.json()
            response_data = response_json["data"]
            try:
                s = Subject.parse_obj(response_data)
                return s
            except ValidationError:
                logging.warning("Validation error parsing subject!")
                s = Subject.construct()
                for key, value in response_data.items():
                    if hasattr(s, key):
                        setattr(s, key, value)
                return s

        elif response.status_code == 418:
            response_json = response.json()
            logging.warning(response_json["message"])
            response_data_original = response_json["data"]
            if type(response_data_original) == list:
                response_data = response_data_original[0]
            else:
                response_data = response_data_original
            s = Subject.construct()
            for key, value in response_data.items():
                if hasattr(s, key):
                    setattr(s, key, value)
            return s
        else:
            logging.warning("Data not retrieved!")
            s = Subject.construct(subject_id=subject_id)
            return s
