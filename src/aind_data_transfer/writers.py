"""This module contains the api to write ephys data.
"""
import logging
import os
import platform
import shutil

import numpy as np

from aind_data_transfer.transformations.compressors import VideoCompressor

# Zarr adds many characters for groups, datasets,
# file names and temporary files.
DEFAULT_MAX_WINDOWS_FILENAME_LEN = 150


class EphysWriters:
    """This class contains the methods to write ephys data."""

    @staticmethod
    def compress_and_write_block(
        read_blocks,
        compressor,
        output_dir,
        job_kwargs,
        output_format="zarr",
        max_windows_filename_len=DEFAULT_MAX_WINDOWS_FILENAME_LEN,
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
            max_windows_filename_len (int): Warn if base file names are larger
              than this.

        Returns:
            Nothing. Writes data to a folder.
        """
        if job_kwargs["n_jobs"] == -1:
            job_kwargs["n_jobs"] = os.cpu_count()

        if max_windows_filename_len is None:
            max_windows_filename_len = DEFAULT_MAX_WINDOWS_FILENAME_LEN

        for read_block in read_blocks:
            if "recording" in read_block:
                rec = read_block["recording"]
            else:
                rec = read_block["scaled_recording"]
            experiment_name = read_block["experiment_name"]
            stream_name = read_block["stream_name"]
            zarr_path = output_dir / f"{experiment_name}_{stream_name}.zarr"
            if (
                platform.system() == "Windows"
                and len(str(zarr_path)) > max_windows_filename_len
            ):
                raise Exception(
                    f"File name for zarr path is too long "
                    f"({len(str(zarr_path))})"
                    f" and might lead to errors. Use a shorter destination "
                    f"path."
                )
            _ = rec.save(
                format=output_format,
                zarr_path=zarr_path,
                compressor=compressor,
                **job_kwargs,
            )

    @staticmethod
    def copy_and_clip_data(
        src_dir,
        dst_dir,
        stream_gen,
        behavior_dir=None,
        video_encryption_key=None,
        n_frames=100,
    ):
        """
        Copies the raw data to a new directory with the .dat files clipped to
        just a small number of frames. This allows someone to still use the
        spikeinterface api on the clipped data set. Also zips and encrypts
        video folder.
        Parameters
        ----------
        src_dir : Path
          Location of raw data
        dst_dir : Path
          Desired location for clipped data set
        stream_gen : dict
          A dict with
            'data': np.memmap(dat file),
              'relative_path_name': path name of raw data
                to new dir correctly
              'n_chan': number of channels.
        behavior_dir: Path
          Location of videos files to compress. If None, will check if src_dir
          contains a Video or video folder in its root.
        video_encryption_key : Optional[str]
          Password to use to encrypt video files. Default is None.
        n_frames : int
          Number of frames to clip data to. Default is 100.

        Returns
        -------
        None
          Moves some directories around.

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

        # third: check if videos directory exists
        new_videos_path = dst_dir / ".." / "behavior"
        if behavior_dir is not None:
            videos_path = behavior_dir
            shutil.copytree(videos_path, new_videos_path)
        elif os.path.isdir(dst_dir / "Videos"):
            videos_path = dst_dir / "Videos"
            shutil.move(videos_path, new_videos_path)
        elif os.path.isdir(dst_dir / "videos"):
            videos_path = dst_dir / "videos"
            shutil.move(videos_path, new_videos_path)
        else:
            videos_path = None

        # Log a warning if no videos path found
        if videos_path is None:
            logging.warning("No videos found!")
        else:
            # Compress and optionally encrypt
            video_compressor = VideoCompressor(
                encryption_key=video_encryption_key
            )
            video_compressor.compress_all_videos_in_dir(new_videos_path)
