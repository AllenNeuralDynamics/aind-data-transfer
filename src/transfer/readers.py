"""This module contains the api to retrieve a reader for ephys data.
"""
import spikeinterface.extractors as se
from enum import Enum


class EphysReaders:
    """This class contains the methods to retrieve a reader.
    """

    class Readers(Enum):
        """Enum for readers."""
        openephys = "openephys"

    readers = ([member.value for member in Readers])

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
            {'recording', 'block_index', 'stream_name'}.

        """
        if reader_name == EphysReaders.Readers.openephys.name:
            nblocks = se.get_neo_num_blocks(reader_name,
                                            input_dir)
            stream_names, stream_ids = se.get_neo_streams(reader_name,
                                                          input_dir)
            for block_index in range(nblocks):
                for stream_name in stream_names:
                    # TODO: Move the filtering logic to own method
                    if "NI-DAQ" in stream_name:
                        continue
                    rec = se.read_openephys(input_dir,
                                            stream_name=stream_name,
                                            block_index=block_index)
                    yield ({"recording": rec,
                            "block_index": block_index,
                            "stream_name": stream_name})
        else:
            raise Exception(f"Unknown reader: {reader_name}. "
                            f"Please select one of {EphysReaders.readers}")

