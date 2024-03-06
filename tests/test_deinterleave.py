import unittest

import dask.array as da
import numpy as np

from aind_data_transfer.transformations.deinterleave import Deinterleave


def _create_interleaved_array(shape, num_channels, array_type):
    if array_type == "numpy":
        a = np.zeros(shape, dtype=int)
    elif array_type == "dask":
        a = da.zeros(shape, dtype=int)
    else:
        raise ValueError(f"Invalid type {array_type}")
    for offset in range(num_channels):
        a[offset::num_channels, ...] = offset
    return a


class TestDeinterleave(unittest.TestCase):
    def test_deinterleave_dask(self):
        num_channels = 3
        a = _create_interleaved_array((384, 32, 32), num_channels, "dask")
        channels = Deinterleave.deinterleave(a, num_channels, axis=0)
        self.assertEqual(len(channels), 3)
        expected_shape = (128, 32, 32)
        for i, c in enumerate(channels):
            self.assertEqual(c.shape, expected_shape)
            self.assertEqual(c.sum().compute(), np.product(c.shape) * i)

    def test_deinterleave_numpy(self):
        num_channels = 3
        a = _create_interleaved_array((384, 32, 32), num_channels, "numpy")
        channels = Deinterleave.deinterleave(a, num_channels, axis=0)
        self.assertEqual(len(channels), 3)
        expected_shape = (128, 32, 32)
        for i, c in enumerate(channels):
            self.assertEqual(c.shape, expected_shape)
            self.assertEqual(c.sum(), np.product(c.shape) * i)

    def test_bad_shape(self):
        num_channels = 2
        a = _create_interleaved_array((257, 32, 32), num_channels, "numpy")
        channels = Deinterleave.deinterleave(a, num_channels, axis=0)
        self.assertEqual(len(channels), 2)
        expected_shape_ch0 = (129, 32, 32)
        expected_shape_ch1 = (128, 32, 32)
        self.assertEqual(channels[0].shape, expected_shape_ch0)
        self.assertEqual(channels[1].shape, expected_shape_ch1)
