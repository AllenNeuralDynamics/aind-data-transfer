"""Module to define ecephys upload job"""

import shutil
from pathlib import Path
from typing import Optional

from numpy import memmap
from aind_data_transfer.readers.ephys_readers import EphysReaders
from aind_data_transfer.transformations.ephys_compressors import (
    EphysCompressors,
)
from aind_data_transfer.util.npopto_correction import (
    correct_np_opto_electrode_locations,
)
from aind_data_transfer.writers.ephys_writers import EphysWriters
import logging
from aind_data_schema.processing import ProcessName
from pydantic import Field

from aind_data_transfer.config_loader.base_config import ModalityConfigs
from aind_data_transfer.readers.ephys_readers import DataReader
from aind_data_transfer.transformations.ephys_compressors import CompressorName
from aind_data_schema.data_description import Modality


class EcephysCompressionParameters(ModalityConfigs):
    """Extra configs for Ecephys upload job."""

    modality = Modality.ECEPHYS

    # Override these values from the base settings
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
        description="Type of compressor to use.",
        title="Compressor Name.",
    )
    compressor_kwargs: dict = Field(
        default={"level": 3},
        description="Arguments to be used for the compressor.",
        title="Compressor Kwargs",
    )
    compress_job_save_kwargs: dict = Field(
        default={"n_jobs": -1},  # -1 to use all available cpu cores.
        description="Arguments for recording save method.",
        title="Compress Job Save Kwargs",
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


class EcephysCompressionJob:
    """Class to define methods needed to compress and upload ecephys job"""

    def __init__(
        self,
        job_configs: EcephysCompressionParameters,
        behavior_dir: Optional[Path] = None,
        log_level: str = "WARNING",
    ):
        """Class constructor"""
        self.job_configs = job_configs
        self.behavior_dir = behavior_dir
        self._instance_logger = (
            logging.getLogger(__name__)
            .getChild(self.__class__.__name__)
            .getChild(str(id(self)))
        )
        self._instance_logger.setLevel(log_level)

    def _copy_and_clip_data(
        self,
        dst_dir,
        stream_gen,
    ):
        """
        Copies the raw data to a new directory with the .dat files clipped to
        just a small number of frames. This allows someone to still use the
        spikeinterface api on the clipped data set.
        Parameters
        ----------
        dst_dir : Path
          Desired location for clipped data set
        stream_gen : dict
          A dict with
            'data': memmap(dat file),
              'relative_path_name': path name of raw data
                to new dir correctly
              'n_chan': number of channels.
        Returns
        -------
        None
          Moves some directories around.

        """

        # first: copy everything except .dat files and files in behavior dir
        if self.behavior_dir is None:
            patterns_to_ignore = ["*.dat"]
        else:
            behavior_glob = self.behavior_dir / "*"
            patterns_to_ignore = ["*.dat", str(behavior_glob)]
        shutil.copytree(
            self.job_configs.source,
            dst_dir,
            ignore=shutil.ignore_patterns(*patterns_to_ignore),
        )
        # second: copy clipped dat files
        for stream in stream_gen:
            data = stream["data"]
            rel_path_name = stream["relative_path_name"]
            n_chan = stream["n_chan"]
            dst_raw_file = dst_dir / rel_path_name
            dst_data = memmap(
                dst_raw_file,
                dtype="int16",
                shape=(self.job_configs.clip_n_frames, n_chan),
                order="C",
                mode="w+",
            )
            dst_data[:] = data[: self.job_configs.clip_n_frames]

    def compress_raw_data(self, temp_dir: Path) -> None:
        """If compress data is set to False, the data will be uploaded to s3.
        Otherwise, it will be compressed to zarr, stored in temp_dir, and
        uploaded later."""

        # Correct NP-opto electrode positions:
        # correction is skipped if Neuropix-PXI version > 0.4.0
        # It'd be nice if the original data wasn't modified.
        correct_np_opto_electrode_locations(self.job_configs.source)
        # Clip the data
        self._instance_logger.info(
            "Clipping source data. This may take a minute."
        )
        clipped_data_path = temp_dir / "ecephys_clipped"
        streams_to_clip = EphysReaders.get_streams_to_clip(
            self.job_configs.data_reader.value,
            self.job_configs.source,
        )
        self._copy_and_clip_data(
            dst_dir=clipped_data_path,
            stream_gen=streams_to_clip,
        )

        self._instance_logger.info("Finished clipping source data.")

        # Compress the data
        self._instance_logger.info("Compressing source data.")
        compressed_data_path = temp_dir / "ecephys_compressed"
        read_blocks = EphysReaders.get_read_blocks(
            self.job_configs.data_reader.value,
            self.job_configs.source,
        )
        compressor = EphysCompressors.get_compressor(
            self.job_configs.compressor_name.value,
            **self.job_configs.compressor_kwargs,
        )
        scaled_read_blocks = EphysCompressors.scale_read_blocks(
            read_blocks=read_blocks,
            num_chunks_per_segment=(
                self.job_configs.scale_num_chunks_per_segment
            ),
            chunk_size=self.job_configs.scale_chunk_size,
        )
        EphysWriters.compress_and_write_block(
            read_blocks=scaled_read_blocks,
            compressor=compressor,
            output_dir=compressed_data_path,
            max_windows_filename_len=(
                self.job_configs.compress_max_windows_filename_len
            ),
            output_format=self.job_configs.compress_write_output_format,
            job_kwargs=self.job_configs.compress_job_save_kwargs,
        )
        self._instance_logger.info("Finished compressing source data.")

        return None
