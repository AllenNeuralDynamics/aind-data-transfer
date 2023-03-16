"""Module that contains the API to retrieve a Compressor for Ephys Data.
"""
from enum import Enum

import numpy as np
import spikeinterface.full as si
import spikeinterface.preprocessing as spre
from numcodecs import Blosc
from tqdm import tqdm
from wavpack_numcodecs import WavPack

from aind_data_transfer.readers.ephys_readers import EphysReaders


class EphysCompressors:
    """This class contains the methods to retrieve a compressor, and to scale
    a read block by lsb and median values.
    """

    class Compressors(Enum):
        """Enum for compression algorithms a user can select"""

        blosc = Blosc.codec_id
        wavpack = "wavpack"

    compressors = [member.value for member in Compressors]

    @staticmethod
    def get_compressor(compressor_name, **kwargs):
        """
        Retrieve a compressor for a given name and optional kwargs.
        Args:
            compressor_name (str): Matches one of the names Compressors enum
            **kwargs (dict): Options to pass into the Compressor
        Returns:
            An instantiated compressor class.
        """
        if compressor_name == EphysCompressors.Compressors.blosc.name:
            return Blosc(**kwargs)
        elif compressor_name == EphysCompressors.Compressors.wavpack.name:
            return WavPack(**kwargs)
        else:
            raise Exception(
                f"Unknown compressor. Please select one of "
                f"{EphysCompressors.compressors}"
            )

    @staticmethod
    def scale_read_blocks(
        read_blocks,
        num_chunks_per_segment=100,
        chunk_size=10000,
        disable_tqdm=False,
    ):
        """
        Scales a read_block. A read_block is dict of
        {'recording', 'block_index', 'stream_name'}.
        Args:
            read_blocks (iterable): A generator of read_blocks
            num_chunks_per_segment (int):
            chunk_size (int):
            disable_tqdm (boolean): Optionally disable a progress bar.
              Defaults to False.
        Returns:
            A generated scaled_read_block. A dict of
            {'scaled_recording', 'block_index', 'stream_name'}.

        """
        for read_block in read_blocks:
            # We don't need to scale the NI-DAQ recordings
            # TODO: Convert this to regex matching?
            if (
                EphysReaders.RecordingBlockPrefixes.nidaq.value
                in read_block["stream_name"]
            ):
                rec_to_compress = read_block["recording"]
            else:
                rec_to_compress = spre.correct_lsb(read_block["recording"], 
                                                   num_chunks_per_segment=num_chunks_per_segment,
                                                   chunk_size=chunk_size)
            yield (
                {
                    "scaled_recording": rec_to_compress,
                    "experiment_name": read_block["experiment_name"],
                    "stream_name": read_block["stream_name"],
                }
            )
