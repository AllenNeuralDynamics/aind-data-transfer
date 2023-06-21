"""
Script that defines the pydantic schema
for the processing manifest
"""

from datetime import datetime
from enum import Enum
from typing import List, Optional

from aind_data_schema.base import AindModel
from aind_data_schema.data_description import Funding, Institution
from aind_data_schema.device import SizeUnit
from aind_data_schema.imaging.acquisition import AxisName, Immersion
from pydantic import Field


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
    """Processing manifest data description"""

    # Need to add funding source and group
    project: Optional[str] = Field(None, description="Project name")
    project_id: Optional[str] = Field(None, description="Project id")
    institution: Institution = Field(
        ...,
        description="An established society, corporation, foundation or other organization that collected this data",
        title="Institution",
        enumNames=[i.value.name for i in Institution],
    )
    funding_sources: List[Funding] = Field(
        ..., description="Funding sources", title="Funding"
    )


class Acquisition(AindModel):
    """Processing manifest acquisition"""

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
    """Regular expression for the channel imaging data"""

    CHANNEL_NAME = r"Ex_\d{3}_Em_\d{3}"


class AxisResolution(AindModel):
    """Axial resolution for the channel imaging data"""

    axis_name: AxisName = Field(..., title="Axis name")
    unit: SizeUnit = Field(SizeUnit.UM, title="Axis physical units")
    resolution: float = Field(..., title="Axis resolution")


class StitchingParameters(AindModel):
    """Stitching parameters for the SmartSPIM pipeline"""

    channel: str = Field(
        ...,
        title="Stitching channel",
        regex=ChannelRegex.CHANNEL_NAME.value,
        description="Channel we want the dataset to be stitched on",
    )
    resolution: List[AxisResolution] = Field(
        ..., title="Dataset resolution in each axis"
    )
    cpus: Optional[int] = Field(32, title="cpus for stitching and fusion")


class CCFRegistrationParameters(AindModel):
    """Registration parameters for the SmartSPIM pipeline"""

    channels: List[str] = Field(
        ...,
        title="Channels we want to register the dataset on",
        regex=ChannelRegex.CHANNEL_NAME.value,
    )
    input_scale: Optional[int] = Field(3, title="Input scale")


class CellSegParameters(AindModel):
    """Segmentation parameters for the SmartSPIM pipeline"""

    channels: List[str] = Field(
        ...,
        title="Channels we want to segment the dataset on",
        regex=ChannelRegex.CHANNEL_NAME.value,
    )
    input_scale: Optional[str] = Field(
        "0", title="Input scale to use in the segmentation"
    )
    chunksize: Optional[str] = Field(
        "128", title="Chunksize in the segmentation"
    )
    signal_start: Optional[str] = Field(
        "0", title="Slide in Z where the segmentaiton starts"
    )
    signal_end: Optional[str] = Field(
        "-1", title="Slide in Z where the segmentaiton ends"
    )


class ProcessingPipeline(AindModel):
    """Pipeline parameters for the SmartSPIM pipeline"""

    stitching: StitchingParameters = Field(..., title="Stitching parameters")
    registration: Optional[CCFRegistrationParameters] = Field(
        None, title="Parameters for the CCF registration"
    )
    segmentation: Optional[CellSegParameters] = Field(
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

    data_description: DataDescription = Field(
        ...,
        title="Data description",
        description="Data description necessary to create metadata",
    )

    acquisition: Acquisition = Field(
        ...,
        title="Acquisition data",
        description="Acquition data coming from the rig which is necessary to create matadata files",
    )

    pipeline_processing: ProcessingPipeline = Field(
        ...,
        title="SmartSPIM pipeline parameters",
        description="Parameters necessary for the smartspim pipeline parameters",
    )


def generate_processing_manifest(output_path: str):
    """
    Function that generates a processing manifest
    json based on the schema.

    Parameters
    ----------
    output_path: str
        Path where we want to output the
        processing manifest. It includes the name.
        e.g., processing_manifest.json,
        PATH/TO/FOLDER/processing_manifest.json
        PATH/TO/FOLDER/processing_manifest_specimen_id.json
    """
    output_path = "processing_manifest.json"

    processing_manifest_example = ProcessingManifest(
        specimen_id="000000",
        dataset_status=DatasetStatus(status="pending"),
        data_description=DataDescription(
            # project="asd", # Uncomment if you want this info
            # project_id="asd",
            institution=Institution.AIND,
            funding_sources=[
                Funding(
                    funder=Institution.AIND,
                    # grant_number="00000000",
                    # fundee="AIND"
                ),
            ],
        ),
        acquisition=Acquisition(
            experimenter_full_name="John Rohde",
            instrument_id="SmartSPIM-2-1",
            chamber_immersion=Immersion(
                medium="Cargille oil 1.5200", refractive_index=1.5208
            ),
            sample_immersion=Immersion(
                medium="Cargille oil 1.5200", refractive_index=1.5208
            ),  # Optional parameter
            local_storage_directory="D:/SmartSPIM_Data",
        ),
        pipeline_processing=ProcessingPipeline(
            stitching=StitchingParameters(
                channel="Ex_488_Em_561",
                resolution=[
                    AxisResolution(axis_name="X", resolution=1.8),
                    AxisResolution(axis_name="Y", resolution=1.8),
                    AxisResolution(axis_name="Z", resolution=2.0),
                ],
            ),
            registration=CCFRegistrationParameters(
                channels=["Ex_488_Em_561", "Ex_561_Em_600"]
            ),  # Optional parameter
            segmentation=CellSegParameters(
                channels=["Ex_488_Em_561", "Ex_561_Em_600"]
            ),  # Optional parameter
        ),
    )

    with open(output_path, "w") as f:
        f.write(processing_manifest_example.json(indent=3))


def main():
    """Main function"""
    output_path = "processing_manifest.json"
    generate_processing_manifest(output_path)


if __name__ == "__main__":
    main()
