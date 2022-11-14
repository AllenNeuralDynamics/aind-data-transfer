"""This module will have classes that handle mapping to metadata files."""

from datetime import datetime

import aind_data_schema
from aind_data_schema.processing import Processing, ProcessName, DataProcess

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
    ) -> aind_data_schema.Processing:
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
