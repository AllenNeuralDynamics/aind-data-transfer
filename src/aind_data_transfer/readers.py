"""This module contains the api to retrieve a reader for ephys data.
"""
import re
from enum import Enum
from pathlib import Path

import numpy as np
import spikeinterface.extractors as se


class EphysReaders:
    """This class contains the methods to retrieve a reader."""

    class Readers(Enum):
        """Enum for readers."""

        openephys = "openephys"

    readers = [member.value for member in Readers]

    class RecordingBlockPrefixes(Enum):
        """Enum for types of recording blocks."""

        # TODO: Convert these to regex patterns?
        neuropix = "Neuropix"
        nidaq = "NI-DAQ"

    class SourceRegexPatterns(Enum):
        """Enum for regex patterns the source folder name should match"""

        subject_datetime = r"\d+_\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}"
        ecephys_subject_datetime = (
            r"ecephys_\d+_\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}"
        )

    @staticmethod
    def get_read_blocks(reader_name, input_dir):
        """
        Given a reader name and input directory, generate a stream of
        recording blocks.
        Args:
            reader_name (str): Matches one of the names Readers enum
            input_dir (Path): Directory of ephys data to read.
        Returns:
            A generator of read blocks. A read_block is dict of
            {'recording', 'experiment_name', 'stream_name'}.

        """
        if reader_name == EphysReaders.Readers.openephys.value:
            nblocks = se.get_neo_num_blocks(reader_name, input_dir)
            stream_names, stream_ids = se.get_neo_streams(
                reader_name, input_dir
            )
            # load first stream to map block_indices to experiment_names
            rec_test = se.read_openephys(
                input_dir, block_index=0, stream_name=stream_names[0]
            )
            record_node = list(rec_test.neo_reader.folder_structure.keys())[0]
            experiments = rec_test.neo_reader.folder_structure[record_node][
                "experiments"
            ]
            exp_ids = list(experiments.keys())
            experiment_names = [
                experiments[exp_id]["name"] for exp_id in sorted(exp_ids)
            ]
            for block_index in range(nblocks):
                for stream_name in stream_names:
                    rec = se.read_openephys(
                        input_dir,
                        stream_name=stream_name,
                        block_index=block_index,
                    )
                    yield (
                        {
                            "recording": rec,
                            "experiment_name": experiment_names[block_index],
                            "stream_name": stream_name,
                        }
                    )
        else:
            raise Exception(
                f"Unknown reader: {reader_name}. "
                f"Please select one of {EphysReaders.readers}"
            )

    @staticmethod
    def get_streams_to_clip(reader_name, input_dir):
        stream_names, stream_ids = se.get_neo_streams(reader_name, input_dir)
        for dat_file in input_dir.glob("**/*.dat"):
            oe_stream_name = dat_file.parent.name
            si_stream_name = [
                stream_name
                for stream_name in stream_names
                if oe_stream_name in stream_name
            ][0]
            n_chan = se.read_openephys(
                input_dir, block_index=0, stream_name=si_stream_name
            ).get_num_channels()
            data = np.memmap(
                dat_file, dtype="int16", order="C", mode="r"
            ).reshape(-1, n_chan)
            yield {
                "data": data,
                "relative_path_name": str(dat_file.relative_to(input_dir)),
                "n_chan": n_chan,
            }


class ImagingReaders:
    """This class contains the methods to retrieve a reader for aind imaging data."""

    class Readers(Enum):
        """Enum for readers."""

        exaspim = "exaSPIM"
        mesospim = "mesoSPIM"

    readers = [member.value for member in Readers]

    class SourceRegexPatterns(Enum):
        """Enum for regex patterns the source folder name should match"""

        exaspim_acquisition = r"exaSPIM_[A-Z0-9]+_\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}"
        mesospim_acquisition = r"mesoSPIM_[A-Z0-9]+_\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}"

    @staticmethod
    def get_raw_data_dir(reader_name, input_dir):
        if reader_name not in ImagingReaders.readers:
            raise Exception(
                f"Unknown reader: {reader_name}. "
                f"Please select one of {ImagingReaders.readers}"
            )
        raw_data_dir = Path(input_dir) / reader_name
        if not raw_data_dir.is_dir():
            raise FileNotFoundError(f"Raw data directory not found: {raw_data_dir}")
        return raw_data_dir

    @staticmethod
    def get_reader_name(input_dir):
        # re.search does not work with Path objects
        input_dir = str(input_dir)
        if re.search(ImagingReaders.Readers.exaspim.value, input_dir) is not None:
            return ImagingReaders.Readers.exaspim.value
        elif re.search(ImagingReaders.Readers.mesospim.value, input_dir) is not None:
            return ImagingReaders.Readers.mesospim.value
        else:
            raise Exception(f"An appropriate reader could not be created for {input_dir}")

