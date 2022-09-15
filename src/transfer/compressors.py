from enum import Enum
from numcodecs import Blosc
from wavpack_numcodecs import WavPack

import spikeinterface.full as si
import spikeinterface.preprocessing as spre
import numpy as np
from tqdm import tqdm


class EphysCompressors:
    class Compressors(Enum):
        blosc = "blosc"
        wavpack = "wavpack"

    compressors = ([member.value for member in Compressors])

    @staticmethod
    def get_compressor(compressor_name, **kwargs):
        if compressor_name == EphysCompressors.Compressors.blosc.name:
            return Blosc(**kwargs)
        elif compressor_name == EphysCompressors.Compressors.wavpack.name:
            return WavPack(**kwargs)
        else:
            raise Exception(f"Unknown compressor. Please select one of "
                            f"{EphysCompressors.compressors}")

    @staticmethod
    def _get_median_and_lsb(recording,
                            num_random_chunks=10,
                            disable_tqdm=False,
                            **random_chunk_kwargs):
        """This function estimates the channel-wise medians and the overall
        LSB from a recording
        Parameters
        ----------
        recording : si.BaseRecording
            The input recording object
        num_random_chunks : int, optional
            Number of random chunks to extract, by default 10
        disable_tqdm : bool, optional
            Disable progress bar, default is False
        **random_chunk_kwargs: keyword arguments for
            si.get_random_data_chunks() (mainly chunk_size)
        Returns
        -------
        int
            lsb_value
        np.array
            median_values
        """
        # compute lsb and median
        # gather chunks
        chunks = None
        for i in tqdm(range(num_random_chunks), desc="Extracting chunks",
                      disable=disable_tqdm):
            chunks_i2 = si.get_random_data_chunks(
                recording, seed=i ** 2, **random_chunk_kwargs)
            if chunks is None:
                chunks = chunks_i2
            else:
                chunks = np.vstack((chunks, chunks_i2))

        lsb_value = 0
        num_channels = recording.get_num_channels()
        dtype = recording.get_dtype()

        channel_idxs = np.arange(num_channels)
        min_values = np.zeros(num_channels, dtype=dtype)
        median_values = np.zeros(num_channels, dtype=dtype)
        offsets = np.zeros(num_channels, dtype=dtype)

        for ch in tqdm(channel_idxs, desc="Estimating channel stats",
                       disable=disable_tqdm):
            unique_vals = np.unique(chunks[:, ch])
            unique_vals_abs = np.abs(unique_vals)
            lsb_val = np.min(np.diff(unique_vals))

            min_values[ch] = np.min(unique_vals_abs)
            median_values[ch] = np.median(chunks[:, ch]).astype(dtype)

            unique_vals_m = np.unique(chunks[:, ch] - median_values[ch])
            unique_vals_abs_m = np.abs(unique_vals_m)
            offsets[ch] = np.min(unique_vals_abs_m)

            if lsb_val > lsb_value:
                lsb_value = lsb_val

        return lsb_value, median_values

    @staticmethod
    def scale_read_blocks(read_blocks,
                          num_random_chunks=10,
                          num_chunks_per_segment=1,
                          chunk_size=20,
                          disable_tqdm=False):
        for read_block in read_blocks:
            lsb_value, median_values = (
                EphysCompressors._get_median_and_lsb(
                    read_block['recording'],
                    num_random_chunks=num_random_chunks,
                    num_chunks_per_segment=num_chunks_per_segment,
                    chunk_size=chunk_size,
                    disable_tqdm=disable_tqdm
                )
            )
            dtype = read_block['recording'].get_dtype()
            rec_to_compress = (
                spre.scale(read_block['recording'], gain=1.0,
                           offset=median_values, dtype=dtype)
            )
            rec_to_compress = (
                spre.scale(rec_to_compress, gain=1.0 / lsb_value, dtype=dtype)
            )
            rec_to_compress.set_channel_gains(
                rec_to_compress.get_channel_gains() * lsb_value
            )
            yield ({"scaled_recording": rec_to_compress,
                    "block_index": read_block['block_index'],
                    "stream_name": read_block['stream_name']})