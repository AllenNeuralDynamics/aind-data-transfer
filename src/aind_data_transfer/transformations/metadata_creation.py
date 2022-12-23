"""This module will have classes that handle mapping to metadata files."""
import logging
import re
from datetime import datetime
from typing import Optional

import requests
from aind_data_schema.data_description import (
    Funding,
    Institution,
    RawDataDescription,
)
from aind_data_schema.processing import DataProcess, Processing, ProcessName

import aind_data_transfer


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
    """Class to handle the creation of the subject metadata file."""

    output_file_name = "subject.json"

    @staticmethod
    def ephys_job_to_subject(
        metadata_service_url: str,
        subject_id: Optional[str] = None,
        filepath: Optional[str] = None,
        file_subject_regex: Optional[str] = (
            "(ecephys|ephys)*_*(\\d+)_\\d{4}"
        ),
    ) -> dict:
        """

        Parameters
        ----------
        metadata_service_url : str
        subject_id : Optional[str]
          The subject id. If not provided, this method will try to parse it
          from the filepath. Default is None
        filepath : Optional[str]
          If the subject_id is None, then this method will try to parse the
          subject id from the filepath. Default is None.
        file_subject_regex : Optional[str]
          Regex pattern that will be used if the subject_id is None and the
          filepath is not None.

        Returns
        -------
        dict
          The subject information retrieved from aind-metadata-service.

        """

        # TODO: Import aind_metadata_service.client once it's written
        if subject_id is None:
            parsed_filepath = re.findall(file_subject_regex, filepath)
            subject_id = parsed_filepath[0][1]

        # TODO: construct this from aind_metadata_service.client
        subject_url = metadata_service_url + f"/subject/{subject_id}"
        response = requests.get(subject_url)

        if response.status_code == 200:
            response_json = response.json()
            response_data = response_json["data"]
            return response_data
        elif response.status_code == 418:
            response_json = response.json()
            logging.warning(response_json["message"])
            response_data_original = response_json["data"]
            if type(response_data_original) == list:
                response_data = response_data_original[0]
            else:
                response_data = response_data_original
            return response_data
        else:
            logging.error("No data retrieved!")
            return None


class DataDescriptionMetadata:
    """Class to handle the creation of the processing metadata file."""

    @staticmethod
    def ephys_job_to_data_description(
        name: str,
        institution=Institution.AIND,
        funding_source=(Funding(funder=Institution.AIND.value),),
    ) -> RawDataDescription:
        """
        Creates a data description instance based on the openephys_job settings
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
