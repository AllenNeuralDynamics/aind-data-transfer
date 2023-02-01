import os
from enum import Enum
from pathlib import Path
from typing import Optional

import pyminizip


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
