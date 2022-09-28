"""This module contains the api to retrieve a reader for ephys data.
"""
from enum import Enum

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
                input_dir, 
                block_index=0,
                stream_name=stream_names[0]
            )
            record_node = list(rec_test.neo_reader.folder_structure.keys())[0]
            experiments = rec_test.neo_reader.folder_structure[record_node]["experiments"]
            experiment_names = [exp["name"] for exp_id, exp in experiments.items()]
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
