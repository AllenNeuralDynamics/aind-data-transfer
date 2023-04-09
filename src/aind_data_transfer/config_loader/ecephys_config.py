from aind_data_schema.data_description import ExperimentType
from aind_data_schema.processing import ProcessName
from pydantic import Field

from aind_data_transfer.config_loader.base_config import BasicUploadJobConfigs
from aind_data_transfer.readers.ephys_readers import DataReader
from aind_data_transfer.transformations.ephys_compressors import CompressorName


class EcephysConfigs(BasicUploadJobConfigs):

    # Override these values from the base settings
    experiment_type: ExperimentType = Field(
        default=ExperimentType.ECEPHYS,
        description="Experiment type",
        title="Experiment Type",
        const=True,
    )
    compress_raw_data: bool = Field(
        default=True,
        description="Run compression on data",
        title="Compress Raw Data",
    )
    process_name: ProcessName = Field(
        default=ProcessName.EPHYS_PREPROCESSING,
        description="Type of processing performed on the raw data source.",
        title="Process Name",
        const=True,
    )

    data_reader: DataReader = Field(
        default=DataReader.OPENEPHYS,
        description="Type of reader to use to read the data source.",
        title="Data Reader",
    )

    # Clip settings
    clip_n_frames: int = Field(
        default=100,
        description="Number of frames to clip the data.",
        title="Clip N Frames",
    )
    # Compress settings
    compress_write_output_format: str = Field(
        default="zarr",
        description=(
            "Output format for compression. Currently, only zarr supported."
        ),
        title="Write Output Format",
        const=True,
    )
    compress_max_windows_filename_len: int = Field(
        default=150,
        description=(
            "Windows OS max filename length is 256. The zarr write will "
            "raise an error if it detects that the destination directory has "
            "a long name."
        ),
        title="Compress Max Windows Filename Len",
    )
    compressor_name: CompressorName = Field(
        default=CompressorName.WAVPACK,
        description=(f"Type of compressor to use."),
        title="Compressor Name.",
    )
    compressor_kwargs: dict = Field(
        default={"level": 3},
        description="Argurments to be used for the compressor.",
        title="Compressor Kwargs",
    )
    compress_write_n_jobs: int = Field(
        default=-1,
        description=(
            "Number of CPUs to be used during the compression process. -1 "
            "defaults to all available cpus."
        ),
        title="Compress Write N Jobs",
    )
    compress_chunk_duration: str = Field(
        default="1s",
        description="Duration to be used for chunks.",
        title="Compress Chunk Duration",
    )

    # Scale settings
    scale_num_chunks_per_segment: int = Field(
        default=100,
        description="Num of chunks per segment to scale.",
        title="Scale Num Chunks Per Segment",
    )
    scale_chunk_size: int = Field(
        default=10000,
        description="Chunk size to scale.",
        title="Scale Chunk Size",
    )
