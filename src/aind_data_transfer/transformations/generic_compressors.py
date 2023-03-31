"""Module for basic compression classes."""
import os
from enum import Enum
from pathlib import Path
from typing import List, Optional

import pyminizip
from tqdm import tqdm


class ZipCompressor:
    """Class to handle basic zip compression."""

    def __init__(
        self,
        compression_level: int = 5,
        encryption_key: Optional[str] = None,
        display_progress_bar: bool = False,
    ) -> None:
        """
        Creates a video compressor with compression level and encryption key
        Parameters
        ----------
        compression_level : int
          Integer between 1 and 9. Default is 5.
        encryption_key : Optional[str]
          Optional password to use. Default is None.
        display_progress_bar : bool
          Display the progress bar for the compression. Default is False.
        """
        self.compression_level = compression_level
        self.encryption_key = encryption_key
        self.display_progress_bar = display_progress_bar

    def compress_dir(
        self,
        input_dir: Path,
        output_dir: Path,
        skip_dirs: Optional[List[Path]] = None,
    ):
        """
        Compress the contents of the input folder and save it as a zipped
        folder in an output directory. Can optionally provide a list of
        subdirectories and files to ignore (no wildcard support yet).
        Parameters
        ----------
        input_dir : Path
          The location of the folder to compress
        output_dir : Path
          The location where to save the zipped folder
        skip_dirs : Optional[List[Path]]
          An optional list of directories or files that will be ignored

        Returns
        -------
        None

        """

        # TODO: There's probably a more efficient way than this
        def skip_path(path_to_check, list_of_paths) -> bool:
            """Utility method to check whether to skip a path"""
            if list_of_paths is None:
                return False
            else:
                for path in list_of_paths:
                    if str(path_to_check).startswith(str(path)):
                        return True
                return False

        file_names = []
        file_prefixes = []
        for root, dirs, files in os.walk(input_dir):
            for file in files:
                raw_file_path = os.path.join(root, file)
                if not skip_path(raw_file_path, list_of_paths=skip_dirs):
                    prefix = str(root).replace(str(input_dir.parent), "", 1)[
                        1:
                    ]
                    file_names.append(raw_file_path)
                    file_prefixes.append(prefix)
        total_file_count = len(file_names)
        pbar = tqdm(
            total=total_file_count, disable=(not self.display_progress_bar)
        )
        pyminizip.compress_multiple(
            file_names,
            file_prefixes,
            str(output_dir),
            self.encryption_key,
            self.compression_level,
            lambda x: pbar.update(x),
        )
        pbar.close()
        return None


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
