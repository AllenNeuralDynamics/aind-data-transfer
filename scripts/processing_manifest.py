import re
from datetime import datetime
from enum import Enum
from typing import List, Optional

from aind_data_schema.base import AindModel
from aind_data_schema.models.organizations import Organization
from aind_data_schema.models.modalities import Modality
from aind_data_schema.core.data_description import (
    Funding,
    datetime_from_name_string,
)
from aind_data_schema.models.units import SizeUnit
from aind_data_schema.core.acquisition import AxisName, Immersion
from pydantic import Field

from aind_data_transfer.util import file_utils


class Status(Enum):
    """Dataset status"""

    PENDING = "pending"
    UPLOADING = "uploading"
    UPLOADED = "uploaded"


class DatasetStatus(AindModel):
    """Status of the datasets"""

    status: Status = Field(
        ...,
        description="Status of the dataset on the local storage",
        title="Institution",
        enumNames=[i.value for i in Status],
    )
    # Creating datetime
    status_date = Field(
        datetime.now().date().strftime("%Y-%m-%d"),
        title="Date the flag was created",
    )
    status_time = Field(
        datetime.now().time().strftime("%H-%M-%S"),
        title="Time the flag was created",
    )


class DataDescription(AindModel):
    project: str = Field(..., description="Project name")
    project_id: str = Field(..., description="Project id")


class Acquisition(AindModel):
    experimenter_full_name: str = Field(
        ...,
        description="First and last name of the experimenter.",
        title="Experimenter full name",
    )
    instrument_id: str = Field(..., title="Instrument ID")
    chamber_immersion: Immersion = Field(
        ..., title="Acquisition chamber immersion data"
    )
    sample_immersion: Optional[Immersion] = Field(
        None, title="Acquisition sample immersion data"
    )
    local_storage_directory: Optional[str] = Field(
        None, title="Local storage directory"
    )


class ChannelRegex(Enum):
    CHANNEL_NAME = r"Ex_\d{3}_Em_\d{3}"


class AxisResolution(AindModel):
    axis_name: AxisName = Field(..., title="Axis name")
    unit: SizeUnit = Field(SizeUnit.UM, title="Axis physical units")
    resolution: float = Field(..., title="Axis resolution")


class StitchingParameters(AindModel):
    channel: str = Field(
        ...,
        title="Stitching channel",
        regex=ChannelRegex.CHANNEL_NAME.value,
        description="Channel we want the dataset to be stitched on",
    )
    dataset_resolution: List[AxisResolution] = Field(
        ..., title="Dataset resolution in each axis"
    )


class CCFParameters(AindModel):
    ccf_channels: List[str] = Field(
        ...,
        title="Channels we want to register the dataset on",
        regex=ChannelRegex.CHANNEL_NAME.value,
    )


class CellSegParameters(AindModel):
    cell_seg_channels: List[str] = Field(
        ...,
        title="Channels we want to segment the dataset on",
        regex=ChannelRegex.CHANNEL_NAME.value,
    )


class ProcessingPipeline(AindModel):
    stitching_parameters: StitchingParameters = Field(
        ..., title="Stitching parameters"
    )
    ccf_parameters: Optional[CCFParameters] = Field(
        None, title="Parameters for the CCF registration"
    )
    cell_segmentation_parameters: Optional[CellSegParameters] = Field(
        None, title="Cell segmentation parameters"
    )


class ProcessingManifest(AindModel):
    """Description of the processing manifest file"""

    schema_version: str = Field("0.1.0", title="Schema Version", const=True)
    license: str = Field("CC-BY-4.0", title="License", const=True)

    specimen_id: str = Field(..., title="Specimen ID")
    dataset_status: DatasetStatus = Field(
        ..., title="Dataset status", description="Dataset status"
    )
    institution: Organization.ONE_OF = Field(
        ...,
        description="An established society, corporation, foundation or other organization that collected this data",
        title="Institution"
    )
    acquisition: Acquisition = Field(
        ...,
        title="Acquisition data",
        description="Acquition data coming from the rig which is necessary to create matadata files",
    )

    processing_pipeline: ProcessingPipeline = Field(
        ...,
        title="SmartSPIM pipeline parameters",
        description="Parameters necessary for the smartspim pipeline parameters",
    )


if __name__ == "__main__":
    # print(ProcessingManifest.schema_json(indent=2))
    # print(ProcessingManifest.schema())

    output_path = "processing_manifest.json"

    processing_manifest_example = ProcessingManifest(
        specimen_id="000000",
        dataset_status=DatasetStatus(status="pending"),
        institution=Organization.AIND,
        acquisition=Acquisition(
            experimenter_full_name="John Rohde",
            instrument_id="SmartSPIM-id-1",
            chamber_immersion=Immersion(
                medium="Cargille oil 1.5200", refractive_index=1.5208
            ),
            sample_immersion=Immersion(
                medium="Cargille oil 1.5200", refractive_index=1.5208
            ),  # Optional parameter
            local_storage_directory="D:/SmartSPIM_Data",
        ),
        processing_pipeline=ProcessingPipeline(
            stitching_parameters=StitchingParameters(
                channel="Ex_488_Em_561",
                dataset_resolution=[
                    AxisResolution(axis_name="X", resolution=1.8),
                    AxisResolution(axis_name="Y", resolution=1.8),
                    AxisResolution(axis_name="Z", resolution=2.0),
                ],
            ),
            ccf_parameters=CCFParameters(
                ccf_channels=["Ex_488_Em_561", "Ex_561_Em_600"]
            ),  # Optional parameter
            cell_segmentation_parameters=CellSegParameters(
                cell_seg_channels=["Ex_488_Em_561", "Ex_561_Em_600"]
            ),  # Optional parameter
        ),
    )

    with open(output_path, "w") as f:
        f.write(processing_manifest_example.json(indent=3))

    # reading back the json
    json_data = file_utils.read_json_as_dict(output_path)
    model = ProcessingManifest(**json_data)
    print(model.model_dump_json())
