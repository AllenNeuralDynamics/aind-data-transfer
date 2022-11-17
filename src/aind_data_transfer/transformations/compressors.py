"""Module that contains the API to retrieve a Compressor for Ephys Data.
"""
import os
from enum import Enum
from pathlib import Path
from typing import Optional

import numpy as np
import pyminizip
import spikeinterface.full as si
import spikeinterface.preprocessing as spre
from numcodecs import Blosc
from tqdm import tqdm
from wavpack_numcodecs import WavPack

from aind_data_transfer.readers import EphysReaders


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
    def _get_median_and_lsb(
        recording,
        disable_tqdm=False,
        **random_chunk_kwargs,
    ):
        """This function estimates the channel-wise medians and the overall
        LSB from a recording
        Parameters
        ----------
        recording : si.BaseRecording
            The input recording object
        disable_tqdm : bool, optional
            Disable progress bar, default is False
        **random_chunk_kwargs: keyword arguments for
            si.get_random_data_chunks() (chunk_size, num_chunks_per_segment)
        Returns
        -------
        int
            lsb_value
        np.array
            median_values
        """
        # gather chunks
        chunks = si.get_random_data_chunks(
            recording, seed=0, **random_chunk_kwargs
        )

        lsb_value = 0
        num_channels = recording.get_num_channels()
        dtype = recording.get_dtype()

        channel_idxs = np.arange(num_channels)
        min_values = np.zeros(num_channels, dtype=dtype)
        median_values = np.zeros(num_channels, dtype=dtype)
        offsets = np.zeros(num_channels, dtype=dtype)

        for ch in tqdm(
            channel_idxs, desc="Estimating channel stats", disable=disable_tqdm
        ):
            unique_vals = np.unique(chunks[:, ch])
            unique_vals_abs = np.abs(unique_vals)
            # it might happen that there is a single unique value
            # (e.g. a channel is broken)
            if len(unique_vals) > 1:
                lsb_val = np.min(np.diff(unique_vals))
            else:
                # in this case we can't estimate the LSB for the channel
                lsb_val = 0

            min_values[ch] = np.min(unique_vals_abs)
            median_values[ch] = np.median(chunks[:, ch]).astype(dtype)

            unique_vals_m = np.unique(chunks[:, ch] - median_values[ch])
            unique_vals_abs_m = np.abs(unique_vals_m)
            offsets[ch] = np.min(unique_vals_abs_m)

            if lsb_val > lsb_value:
                lsb_value = lsb_val

        return lsb_value, median_values

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
                (
                    lsb_value,
                    median_values,
                ) = EphysCompressors._get_median_and_lsb(
                    read_block["recording"],
                    num_chunks_per_segment=num_chunks_per_segment,
                    chunk_size=chunk_size,
                    disable_tqdm=disable_tqdm,
                )
                dtype = read_block["recording"].get_dtype()
                rec_to_compress = spre.scale(
                    read_block["recording"],
                    gain=1.0,
                    offset=-median_values,
                    dtype=dtype,
                )
                rec_to_compress = spre.scale(
                    rec_to_compress, gain=1.0 / lsb_value, dtype=dtype
                )
                rec_to_compress.set_channel_gains(
                    rec_to_compress.get_channel_gains() * lsb_value
                )
            yield (
                {
                    "scaled_recording": rec_to_compress,
                    "experiment_name": read_block["experiment_name"],
                    "stream_name": read_block["stream_name"],
                }
            )


class ImagingCompressors:
    class Compressors(Enum):
        """Enum for compression algorithms a user can select"""

        blosc = Blosc.codec_id

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
        if compressor_name == ImagingCompressors.Compressors.blosc.name:
            return Blosc(**kwargs)
        else:
            raise Exception(
                f"Unknown compressor. Please select one of "
                f"{ImagingCompressors.compressors}"
            )


class VideoCompressor:
    """Class to handle video compression and encryption."""

    def __init__(
        self, compression_level: int = 5, encryption_key: Optional[str] = None
    ) -> None:
        """
        Creates a video compressor with compression level and encryption key
        Parameters
        ----------
        compression_level : int
          Integer between 1 and 9. Default is 5.
        encryption_key : Optional[str]
          Optional password to use. Default is None.
        """
        self.compression_level = compression_level
        self.encryption_key = encryption_key

    class VideoFileTypes(Enum):
        """Enum for types of video to compress"""

        MPEG_4 = ".mp4"
        QUICKTIME_MOVIE = ".mov"
        WINDOWSMEDIA_VIEWER = ".wmv"
        AUDIO_VIDEO_INTERLEAVE = ".avi"

    video_file_extensions = tuple([member.value for member in VideoFileTypes])

    def compress_all_videos_in_dir(self, video_dir: Path) -> None:
        """
        Compress and optionally encrypt video files in a directory
        Parameters
        ----------
        video_dir : Path
          Directory where video files are stored.

        Returns
        -------
        None

        """

        for root, dirs, files in os.walk(video_dir):
            for file in files:
                if file.endswith(self.video_file_extensions):
                    raw_file_path = os.path.join(root, file)
                    zip_file_path = os.path.join(root, file) + ".zip"
                    pyminizip.compress(
                        str(raw_file_path),
                        None,
                        str(zip_file_path),
                        self.encryption_key,
                        self.compression_level,
                    )
                    os.remove(raw_file_path)
