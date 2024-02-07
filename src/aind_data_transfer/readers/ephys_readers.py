"""Module for ephys data readers."""

from enum import Enum

import numpy as np
from spikeinterface import extractors as se


class DataReader(Enum):
    """Enum for readers."""

    OPENEPHYS = "openephys"


class EphysReaders:
    """This class contains the methods to retrieve a reader."""

    readers = [member.value for member in DataReader]

    class RecordingBlockPrefixes(Enum):
        """Enum for types of recording blocks."""

        # TODO: Convert these to regex patterns?
        neuropix = "Neuropix"
        nidaq = "NI-DAQ"

    class SourceRegexPatterns(Enum):
        """Enum for regex patterns the source folder name should match"""

        subject_datetime = r"(\d+)_(\d{4}-\d{2}-\d{2})_(\d{2}-\d{2}-\d{2})"
        ecephys_subject_datetime = (
            r"ecephys_(\d+)_(\d{4}-\d{2}-\d{2})_(\d{2}-\d{2}-\d{2})"
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
        if reader_name == DataReader.OPENEPHYS.value:
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
                        load_sync_timestamps=True,
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
