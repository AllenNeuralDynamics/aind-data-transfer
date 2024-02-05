import re
from enum import Enum
from typing import Union, List

import dask.array as da
import numpy as np


class Deinterleave:
    @staticmethod
    def deinterleave(
        a: Union[da.Array, np.ndarray],
        num_channels: int,
        axis: int,
    ) -> List[Union[da.Array, np.ndarray]]:
        """
        Deinterleave the channels of a dask or numpy array.

        Parameters
        ----------
        a : Union[da.Array, np.ndarray]
          The dask or numpy array.
        num_channels : int
          The number of arrays to extract
        axis: int
          The axis to deinterleave

        Returns
        -------
        List of dask or numpy arrays representing each deinterleaved channel
        """
        channels = []
        for offset in range(num_channels):
            s = [slice(None)] * a.ndim
            s[axis] = slice(offset, None, num_channels)
            channels.append(a[tuple(s)])
        return channels


class ChannelParser:
    class RegexPatterns(Enum):
        channel_pattern = r"ch_([0-9_]{3,})(_cam(0|1))?\."
        tile_xyz_pattern = r"([a-zA-Z0-9_]+)_[xX]_\d{4}_[yY]_\d{4}_[zZ]_\d{4}"

    @staticmethod
    def parse_channel_names(filepath: str):
        """
        Parse the channel wavelengths from a filepath

        Parameters
        ----------
        filepath: str
          the path to the interleaved image

        Returns
        -------
        List of channel wavelength strings, e.g., ["488", "561"]
        """
        filepath = str(filepath)
        m = re.search(
            ChannelParser.RegexPatterns.channel_pattern.value, filepath
        )
        if m is None:
            raise ValueError(
                f"file name does not match channel pattern: {filepath}"
            )
        wavelengths = m.group(1).strip().split("_")
        return wavelengths

    @staticmethod
    def parse_tile_xyz_loc(filepath: str):
        """
        Parse the tile XYZ prefix from the filepath

        Parameters
        ----------
        filepath: str
          the path to the interleaved image

        Returns
        -------
        the tile coordinate prefix, e.g., "tile_X_0000_Y_0001_Z_0002"
        """
        filepath = str(filepath)
        m = re.search(
            ChannelParser.RegexPatterns.tile_xyz_pattern.value, filepath
        )
        if m is None:
            raise ValueError(
                f"file name does not match tile pattern: {filepath}"
            )
        return m.group(0)


if __name__ == "__main__":
    path = "/home/cameron.arshadi/data/tile_X_0002_Y_0000_Z_0000_ch_488_561_688_cam1.tiff"
    names = ChannelParser.parse_channel_names(path)
    xyz_loc = ChannelParser.parse_tile_xyz_loc(path)
    print(names)
    print(xyz_loc)

    a = da.zeros(shape=(384, 128, 128), dtype=int)
    num_channels = 3
    a[1::num_channels, ...] = 1
    a[2::num_channels, ...] = 2
    channels = Deinterleave.deinterleave(a, num_channels, axis=0)
    for c in channels:
        print(c.sum().compute())
        print(c.shape)

    print(dict(zip(names, channels)))
