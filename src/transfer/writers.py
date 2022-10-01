"""This module contains the api to write ephys data.
"""
import shutil

import numpy as np


class EphysWriters:
    """This class contains the methods to write ephys data."""

    @staticmethod
    def compress_and_write_block(
        read_blocks, compressor, output_dir, job_kwargs, output_format="zarr"
    ):
        """
        Compress and write read_blocks.
        Args:
            read_blocks (iterable dict):
              Either [{'recording', 'block_index', 'stream_name'}] or
              [{'scale_recording', 'block_index', 'stream_name'}].
            compressor (obj): A compressor class
            output_dir (Path): Output directory to write compressed data
            job_kwargs (dict): Recording save job kwargs.
            output_format (str): Defaults to zarr

        Returns:
            Nothing. Writes data to a folder.
        """
        for read_block in read_blocks:
            if "recording" in read_block:
                rec = read_block["recording"]
            else:
                rec = read_block["scaled_recording"]
            experiment_name = read_block["experiment_name"]
            stream_name = read_block["stream_name"]
            zarr_path = output_dir / f"{experiment_name}_{stream_name}.zarr"
            _ = rec.save(
                format=output_format,
                zarr_path=zarr_path,
                compressor=compressor,
                **job_kwargs,
            )

    @staticmethod
    def copy_and_clip_data(src_dir, dst_dir, stream_gen, n_frames=100):
        # first: copy everything except .dat files
        shutil.copytree(
            src_dir, dst_dir, ignore=shutil.ignore_patterns("*.dat")
        )
        # second: copy clipped dat files
        for stream in stream_gen:
            data = stream["data"]
            rel_path_name = stream["relative_path_name"]
            n_chan = stream["n_chan"]
            dst_raw_file = dst_dir / rel_path_name
            dst_data = np.memmap(
                dst_raw_file,
                dtype="int16",
                shape=(n_frames, n_chan),
                order="C",
                mode="w+",
            )
            dst_data[:] = data[:n_frames]
