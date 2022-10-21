"""This module will have classes that handle mapping to metadata files."""

import transfer
from enum import Enum
import requests


class MetadataSchemaClient:
    """Class to retrieve metadata schemas. TODO: Move this into own project."""

    class Schemas(Enum):
        processing = "processing"

    def __init__(self, base_url):
        self.base_url = base_url

    @staticmethod
    def extract_schema_constants(schema):
        described_by = schema["properties"]["describedBy"]["const"]
        schema_version = schema["properties"]["schema_version"]["const"]
        return {"describedBy": described_by, "schema_version": schema_version}

    def retrieve_schema(self, schema):
        schema_location = f"{self.base_url}/{schema.value}.json"
        response = requests.get(schema_location)
        return response.json()

    @staticmethod
    def create_data_processing_instance(name,
                                        version,
                                        start_date_time,
                                        end_date_time,
                                        input_location,
                                        output_location,
                                        code_url,
                                        parameters,
                                        notes=None):
        contents = {"name": name,
                    "version": version,
                    "start_date_time": start_date_time,
                    "end_date_time": end_date_time,
                    "input_location": input_location,
                    "output_location": output_location,
                    "code_url": code_url,
                    "parameters": parameters}
        if notes:
            contents["notes"] = notes
        return contents

    def create_processing_instance(self, data_processes):
        schema = self.retrieve_schema(self.Schemas.processing)
        contents = MetadataSchemaClient.extract_schema_constants(schema)
        contents["data_processes"] = data_processes
        return contents


class ProcessingMetadata:
    """Class to handle the creation of the processing metadata file."""

    @staticmethod
    def ephys_job_to_processing(schema_url,
                                start_date_time,
                                end_date_time,
                                input_location,
                                output_location,
                                code_url,
                                parameters,
                                notes=None):
        name = "Ephys preprocessing"
        version = transfer.__version__
        msc = MetadataSchemaClient(schema_url)
        data_processing_instance = (
            msc.create_data_processing_instance(
                name=name,
                version=version,
                start_date_time=start_date_time.isoformat(sep=' '),
                end_date_time=end_date_time.isoformat(sep=' '),
                input_location=input_location,
                output_location=output_location,
                code_url=code_url,
                parameters=parameters,
                notes=notes
            ))
        processing_instance = (
            msc.create_processing_instance(
                data_processes=[data_processing_instance]
            )
        )

        return processing_instance
