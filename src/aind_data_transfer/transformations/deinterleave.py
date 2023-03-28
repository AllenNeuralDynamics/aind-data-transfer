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
        if not a.shape[axis] % num_channels == 0:
            raise Exception(
                f"axis {axis} with shape {a.shape[axis]} not divisible by {num_channels}"
            )
        channels = []
        for offset in range(num_channels):
            s = [slice(None)] * a.ndim
            s[axis] = slice(offset, None, num_channels)
            channels.append(a[tuple(s)])
        return channels


if __name__ == "__main__":
    a = da.zeros(shape=(384, 128, 128), dtype=int)
    num_channels = 3
    a[1::num_channels, ...] = 1
    a[2::num_channels, ...] = 2
    channels = Deinterleave.deinterleave(a, num_channels, axis=0)
    for c in channels:
        print(c.sum().compute())
        print(c.shape)
