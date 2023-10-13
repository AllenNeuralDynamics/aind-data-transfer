"""This module will have classes that handle mapping to metadata files."""
import json
import logging
import os
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple, Type

import aind_data_schema.base
from aind_data_schema.data_description import (
    Funding,
    Institution,
    Modality,
    RawDataDescription,
)
from aind_data_schema.procedures import Procedures
from aind_data_schema.processing import DataProcess, Processing, ProcessName
from aind_data_schema.subject import Subject
from aind_data_schema.metadata import Metadata, MetadataStatus
from aind_metadata_service.client import AindMetadataServiceClient
from pydantic import validate_model
from requests import Response
from requests.exceptions import ConnectionError, JSONDecodeError

from aind_data_transfer import __version__ as aind_data_transfer_version
from aind_data_transfer.config_loader.base_config import ModalityConfigs


class MetadataCreation(ABC):
    """Abstract class with convenient methods to handle metadata creation."""

    def __init__(self, model_obj: Optional[dict] = None):
        """
        Default class constructor.
        Parameters
        ----------
        model_obj : Optional[dict]
          The metadata as a dict object. We'll use this representation to
          avoid potential dependency conflicts from aind_data_schema. We can
          validate any of the incoming data using the version of
          aind_data_schema attached to aind_data_transfer.
        """
        self.model_obj = model_obj

    @staticmethod
    @abstractmethod
    def _model() -> Type[aind_data_schema.base.AindCoreModel]:
        """
        Returns
        -------
          Needs to return an AindCoreModel, such as Subject for example.
        """

    @property
    def output_filename(self):
        """Returns the default json file name for the model as defined in
        aind_data_schema."""
        return self._model().construct().default_filename()

    @classmethod
    def from_file(cls, file_location: Path):
        """
        Construct a MetadataCreation instance from a json file.
        Parameters
        ----------
        file_location : Path
          Location of the json file

        """
        with open(file_location) as f:
            contents = json.load(f)
        return cls(model_obj=contents)

    def validate_obj(self) -> bool:
        """
        Validate the model object. Logs a warning if the model_obj is not
        valid.
        Returns
        -------
        bool
          True if the model is valid. False otherwise.

        """
        *_, validation_error = validate_model(self._model(), self.model_obj)
        if validation_error:
            logging.warning(f"Validation Errors: {validation_error}")
            return False
        else:
            logging.info("Model is valid.")
            return True

    def write_to_json(self, path: Path) -> None:
        """
        Write the model_obj to a json file. If the Path is a directory, it will
        use the output_filename method to generate the filename.
        Parameters
        ----------
        path : Path
          Location of where to save the json file. Can be a directory or file.

        Returns
        -------
        None

        """

        if os.path.isdir(path):
            out_path = path / self.output_filename
        else:
            out_path = path

        with open(out_path, "w") as outfile:
            outfile.write(json.dumps(self.model_obj, indent=3, default=str))


class ServiceMetadataCreation(MetadataCreation):
    """Abstract class for metadata pulled from aind_metadata_service"""

    def __init__(self, model_obj: Optional[dict] = None):
        """
        Default class constructor.
        Parameters
        ----------
        model_obj : Optional[dict]
          The metadata as a dict object. We'll use this representation to
          avoid potential dependency conflicts from aind_data_schema. We can
          validate any of the incoming data using the version of
          aind_data_schema attached to aind_data_transfer.
        """
        super().__init__(model_obj=model_obj)

    @staticmethod
    @abstractmethod
    def _get_service_response(
        subject_id: str,
        ams_client: AindMetadataServiceClient,
    ) -> Response:
        """
        Abstract method to retrieve metadata from the service.
        Parameters
        ----------
        subject_id : str
          ID of the subject we want to get metadata for.
        ams_client : AindMetadataServiceClient
          A client to connect to aind_metadata_service.

        Returns
        -------
        Response
          Has a status code and json contents.

        """

    @classmethod
    def from_service(cls, subject_id: str, domain: str):
        """
        Build the class from data pulled from aind_metadata_service.
        Parameters
        ----------
        subject_id : str
          ID of the subject we want to get metadata for.
        domain : str
          Domain name for aind_metadata_service

        """
        ams_client = AindMetadataServiceClient(domain=domain)
        try:
            response = cls._get_service_response(
                subject_id=subject_id, ams_client=ams_client
            )
            response_json = response.json()
            status_code = response.status_code
            # Everything is okay
            if status_code == 200:
                contents = response_json["data"]
            # Multiple items were found
            elif status_code == 300:
                logging.warning(f"{cls.__name__}: {response_json['message']}")
                contents = response_json["data"][0]
            # The data retrieved is invalid
            elif status_code == 406:
                logging.warning(f"{cls.__name__}: {response_json['message']}")
                contents = response_json["data"]
            # Connected to the service, but no data was found
            elif status_code == 404:
                logging.warning(f"{cls.__name__}: {response_json['message']}")
                contents = json.loads(cls._model().construct().json())
            # A serious error happened. Build a default model.
            else:
                logging.error(f"{cls.__name__}: {response_json['message']}")
                contents = json.loads(cls._model().construct().json())
        except (ConnectionError, JSONDecodeError) as e:
            logging.error(
                f"{cls.__name__}: An error occurred connecting to metadata "
                f"service: {e}"
            )
            contents = json.loads(cls._model().construct().json())
        return cls(model_obj=contents)


class SubjectMetadata(ServiceMetadataCreation):
    """Class to manage building the subject metadata"""

    @staticmethod
    def _model() -> Type[aind_data_schema.base.AindCoreModel]:
        """AindDataSchema model"""
        return Subject

    @staticmethod
    def _get_service_response(
        subject_id: str, ams_client: AindMetadataServiceClient
    ) -> Response:
        """
        Method to retrieve metadata from the service.
        Parameters
        ----------
        subject_id : str
          ID of the subject we want to get metadata for.
        ams_client : AindMetadataServiceClient
          A client to connect to aind_metadata_service.

        Returns
        -------
        Response
          Has a status code and json contents.

        """
        return ams_client.get_subject(subject_id)


class ProceduresMetadata(ServiceMetadataCreation):
    """Class to manage building the procedures metadata"""

    @staticmethod
    def _model() -> Type[aind_data_schema.base.AindCoreModel]:
        """AindDataSchema model"""
        return Procedures

    @staticmethod
    def _get_service_response(
        subject_id: str, ams_client: AindMetadataServiceClient
    ) -> Response:
        """
        Method to retrieve metadata from the service.
        Parameters
        ----------
        subject_id : str
          ID of the subject we want to get metadata for.
        ams_client : AindMetadataServiceClient
          A client to connect to aind_metadata_service.

        Returns
        -------
        Response
          Has a status code and json contents.

        """
        return ams_client.get_procedures(subject_id)


class ProcessingMetadata(MetadataCreation):
    """Class to manage building the processing metadata"""

    @staticmethod
    def _model() -> Type[aind_data_schema.base.AindCoreModel]:
        """AindDataSchema model"""
        return Processing

    @classmethod
    def from_inputs(
        cls,
        process_name: ProcessName,
        start_date_time: datetime,
        end_date_time: datetime,
        input_location: str,
        output_location: str,
        code_url: str,
        parameters: dict,
        notes: Optional[str] = None,
    ):
        """
        Build a ProcessingMetadata instance using some basic parameters.
        Parameters
        ----------
        process_name : ProcessName
          Name of the process
        start_date_time : datetime
          Start date and time of the process
        end_date_time : datetime
          End date and time of the process
        input_location : str
          Location of the files that are being processed
        output_location : str
          Location of the files that are being processed
        code_url : str
          Location of the processing code
        parameters : dict
          Parameters used in the process
        notes : Optional[str]
          Optional notes. Defaults to None.

        """
        data_processing_instance = DataProcess(
            name=process_name.value,
            version=aind_data_transfer_version,
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
        # Do this to use enum strings instead of classes in dict representation
        contents = json.loads(processing_instance.json())
        return cls(model_obj=contents)

    @classmethod
    def from_modalities_configs(
        cls,
        modality_configs: List[ModalityConfigs],
        start_date_time: datetime,
        end_date_time: datetime,
        output_location: str,
        code_url: str,
        notes: Optional[str] = None,
    ):
        """
        Build a ProcessingMetadata instance using some basic parameters.
        Parameters
        ----------
        modality_configs : List[ModalityConfigs]
          List of modality configs
        start_date_time : datetime
          Start date and time of the process
        end_date_time : datetime
          End date and time of the process
        output_location : str
          Location of the files that are being processed
        code_url : str
          Location of the processing code
        notes : Optional[str]
          Optional notes. Defaults to None.

        """
        data_processes = []
        for modality_config in modality_configs:
            if modality_config.modality == Modality.ECEPHYS:
                process_name = ProcessName.EPHYS_PREPROCESSING
            else:
                process_name = ProcessName.OTHER
            data_processing_instance = DataProcess(
                name=process_name.value,
                version=aind_data_transfer_version,
                start_date_time=start_date_time,
                end_date_time=end_date_time,
                input_location=str(modality_config.source),
                output_location=output_location,
                code_url=code_url,
                parameters=modality_config.dict(),
                notes=notes,
            )
            data_processes.append(data_processing_instance)
        processing_instance = Processing(data_processes=data_processes)
        # Do this to use enum strings instead of classes in dict representation
        contents = json.loads(processing_instance.json())
        return cls(model_obj=contents)


class RawDataDescriptionMetadata(MetadataCreation):
    """Class to handle the creation of the raw data description metadata
    file."""

    @staticmethod
    def _model() -> Type[aind_data_schema.base.AindCoreModel]:
        """AindDataSchema model"""
        return RawDataDescription

    @classmethod
    def from_inputs(
        cls,
        name: str,
        modality: List[Modality],
        institution: Optional[Institution] = Institution.AIND,
        funding_source: Optional[Tuple] = (
            Funding(funder=Institution.AIND.value.abbreviation),
        ),
        investigators: Optional[List[str]] = None,
    ):
        """
        Build a RawDataDescriptionMetadata instance using some basic
        parameters.
        Parameters
        ----------
        name : str
          Name of the raw data
        modality : List[Modality]
          Modalities of experiment data
        institution : Optional[Institution]
          Primary Institution. Defaults to AIND.
        funding_source : Optional[Tuple]
          Tuple of funding sources. Defaults to (AIND)
        investigators : Optional[List[str]]

        """
        funding_source_list = (
            list(funding_source)
            if isinstance(funding_source, tuple)
            else funding_source
        )
        investigators = [] if investigators is None else investigators
        basic_settings = RawDataDescription.parse_name(name=name)
        data_description_instance = RawDataDescription(
            institution=institution,
            modality=modality,
            funding_source=funding_source_list,
            investigators=investigators,
            **basic_settings,
        )
        # Do this to use enum strings instead of classes in dict representation
        contents = json.loads(data_description_instance.json())
        return cls(model_obj=contents)


class MetadataRecord(MetadataCreation):
    """Class to handle the creation of the metadata record file."""

    @staticmethod
    def _model() -> Type[aind_data_schema.base.AindCoreModel]:
        """AindDataSchema model"""
        return Metadata

    @classmethod
    def from_inputs(
            cls,
            id: str,
            name: str,
            created: datetime,
            last_modified: datetime,
            location: str,
            subject_metadata: SubjectMetadata,
            procedures_metadata: ProceduresMetadata,
            processing_metadata: ProcessingMetadata,
            data_description_metadata: RawDataDescriptionMetadata,
    ):
        """
        Build a Metadata instance using some basic parameters.
        Parameters
        ----------
        id : str
          Data Asset Record ID.
        name : str
          Name of the raw data
        modality : List[Modality]
          Modalities of experiment data
        institution : Optional[Institution]
          Primary Institution. Defaults to AIND.
        funding_source : Optional[Tuple]
          Tuple of funding sources. Defaults to (AIND)
        investigators : Optional[List[str]]

        """
        metadata_instance = Metadata(
            _id=id,
            name=name,
            created=created,
            last_modified=last_modified,
            location=location,
            metadata_status=MetadataStatus.UNKNOWN,
            subject=subject_metadata.model_obj,
            procedures=procedures_metadata.model_obj,
            processing=processing_metadata.model_obj,
            data_description=data_description_metadata.model_obj
        )
        # Do this to use enum strings instead of classes in dict representation
        contents = json.loads(metadata_instance.json())
        return cls(model_obj=contents)
