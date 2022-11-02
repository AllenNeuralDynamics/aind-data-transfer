"""This module contains the api to write ephys data.
"""
import shutil
import os
import platform

import numpy as np

# Zarr adds many characters for groups, datasets,
# file names and temporary files. 
MAX_WINDOWS_FILENAME_LEN = 100


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
        if job_kwargs["n_jobs"] == -1:
            job_kwargs["n_jobs"] = os.cpu_count()

        for read_block in read_blocks:
            if "recording" in read_block:
                rec = read_block["recording"]
            else:
                rec = read_block["scaled_recording"]
            experiment_name = read_block["experiment_name"]
            stream_name = read_block["stream_name"]
            zarr_path = output_dir / f"{experiment_name}_{stream_name}.zarr"
            if platform.system() == "Windows" and \
                len(str(zarr_path)) > MAX_WINDOWS_FILENAME_LEN:
                raise Exception(
                    f"File name for zarr path is too long ({len(str(zarr_path))})"
                    f" and might lead to errors. Use a shorter destination path."
                )
            _ = rec.save(
                format=output_format,
                zarr_path=zarr_path,
                compressor=compressor,
                **job_kwargs,
            )

    @staticmethod
    def copy_and_clip_data(src_dir, dst_dir, stream_gen, n_frames=100):
        """
        Copies the raw data to a new directory with the .dat files clipped to
        just a small number of frames. This allows someone to still use the
        spikeinterface api on the clipped data set.
        Args:
            src_dir (Path): Location of raw data
            dst_dir (Path): Desired location for clipped data set
            stream_gen (dict): A dict with
              'data': np.memmap(dat file),
              'relative_path_name': path name of raw data so it can be copied
                to new dir correctly
              'n_chan': number of channels.
            n_frames (int): Number of frames to clip data to
        """
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
        # third: check if videos directory exists and copy it up one level
        # TODO: Is there a cleaner way to do this?
        videos_path = dst_dir / "Videos"
        videos_path_l = dst_dir / "videos"
        if os.path.isdir(videos_path):
            new_videos_path = dst_dir / ".." / "videos"
            shutil.move(videos_path, new_videos_path)
        elif os.path.isdir(videos_path_l):
            new_videos_path = dst_dir / ".." / "videos"
            shutil.move(videos_path_l, new_videos_path)
