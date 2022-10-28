"""This module will have classes that handle mapping to metadata files."""

import json
from datetime import datetime
from enum import Enum
from pathlib import Path

import requests

import aind_data_transfer


class MetadataSchemaClient:
    """Class to retrieve metadata schemas. TODO: Move this into own project."""

    class Schemas(Enum):
        """Enum for schemas."""

        processing = "processing"

    def __init__(self, base_url: str) -> None:
        self.base_url = base_url

    @staticmethod
    def extract_schema_constants(schema: dict) -> dict:
        """
        Given a given, pull out schema url and version
        Args:
            schema (dict): A metadata schema

        Returns:
        dict
          {'describedBy: schema description, 'version': schema version}

        """
        described_by = schema["properties"]["describedBy"]["const"]
        schema_version = schema["properties"]["schema_version"]["const"]
        return {"describedBy": described_by, "schema_version": schema_version}

    def retrieve_schema(self, schema: Schemas):
        """
        Retrieve the schema from where it is stored
        Args:
            schema (Schemas): Schema to retrieve

        Returns:
        json
          Schema contents

        """
        schema_location = f"{self.base_url}/{schema.value}.json"
        response = requests.get(schema_location)
        return response.json()

    @staticmethod
    def create_data_processing_instance(
        name: str,
        version: str,
        start_date_time: datetime,
        end_date_time: datetime,
        input_location: str,
        output_location: str,
        code_url: str,
        parameters: dict,
        notes: str = None,
    ) -> dict:
        """
        Will create a data processing instance given some fields.
        Args:
            name (str): 'Ephys preprocessing' for example
            version (str): Version of the software used
            start_date_time (datetime): Time the processing job started
            end_date_time (datetime): Time the processing job ended
            input_location (str): Location of the raw data source
            output_location (str): Location of the processed data
            code_url (str): Location of where the code is maintained
            parameters (dict): Parameters of the processing job
            notes (str): Optional notes. Defaults to None

        Returns:
        dict
            A valid Data Processing instance

        """
        contents = {
            "name": name,
            "version": version,
            "start_date_time": start_date_time.isoformat(sep=" "),
            "end_date_time": end_date_time.isoformat(sep=" "),
            "input_location": input_location,
            "output_location": output_location,
            "code_url": code_url,
            "parameters": parameters,
        }
        if notes:
            contents["notes"] = notes
        return contents

    def create_processing_instance(self, data_processes: list) -> dict:
        """
        Creates a Processing instance given a list of data processes
        Args:
            data_processes (list[dict]): A list of data processes

        Returns:
        dict
            A valid Processing instance
        """
        schema = self.retrieve_schema(self.Schemas.processing)
        contents = MetadataSchemaClient.extract_schema_constants(schema)
        contents["data_processes"] = data_processes
        return contents


class MetadataHandler:
    """Base class for handling metadata."""

    def __init__(
        self, schema_url: str, schema: MetadataSchemaClient.Schemas
    ) -> None:
        self.schema = schema
        self.schema_url = schema_url

    def write_metadata(self, schema_instance: dict, output_dir: Path) -> None:
        """
        Writes out a processing instance.
        Args:
            schema_instance (dict): Data to write out
            output_dir (Path): location of where to write the data

        Returns:

        """
        file_path = output_dir / f"{str(self.schema.value)}.json"
        with open(file_path, "w") as f:
            json_contents = json.dumps(schema_instance, indent=4)
            f.write(json_contents)


class ProcessingMetadata(MetadataHandler):
    """Class to handle the creation of the processing metadata file."""

    def __init__(self, schema_url):
        processing_schema = MetadataSchemaClient.Schemas.processing
        super(ProcessingMetadata, self).__init__(
            schema_url=schema_url, schema=processing_schema
        )

    def ephys_job_to_processing(
        self,
        start_date_time: datetime,
        end_date_time: datetime,
        input_location: str,
        output_location: str,
        code_url: str,
        parameters: dict,
        notes: str = None,
    ) -> dict:
        """
        Creates a processing instance based on the openephys_job settings
        Args:
            schema_url (str): Location of where to retrieve the schema
            start_date_time (datetime): Time the processing job started
            end_date_time (datetime): Time the processing job ended
            input_location (str): Location of the raw data source
            output_location (str): Location of the processed data
            code_url (str): Location of where the code is maintained
            parameters (dict): Parameters of the processing job
            notes (str): Optional notes. Defaults to None

        Returns:

        """
        name = "Ephys preprocessing"
        version = aind_data_transfer.__version__
        msc = MetadataSchemaClient(self.schema_url)
        data_processing_instance = msc.create_data_processing_instance(
            name=name,
            version=version,
            start_date_time=start_date_time,
            end_date_time=end_date_time,
            input_location=input_location,
            output_location=output_location,
            code_url=code_url,
            parameters=parameters,
            notes=notes,
        )
        processing_instance = msc.create_processing_instance(
            data_processes=[data_processing_instance]
        )

        return processing_instance
