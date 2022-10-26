import unittest

import numpy as np
from aind_data_transfer.util import chunk_utils
from aind_data_transfer.util.chunk_utils import DimensionsError


class TestChunkUtils(unittest.TestCase):
    """Tests methods defined in chunk_utils class"""

    def test_ensure_array_5d(self):
        # Test padding if <5D
        a = np.zeros(shape=(1,) * 3, dtype=int)
        expected_arr = np.array([[[[[0]]]]])
        actual_arr = chunk_utils.ensure_array_5d(a)
        self.assertTrue(np.array_equal(expected_arr, actual_arr))

        # Test exception if >5D
        a = np.zeros(shape=(1,) * 6, dtype=int)
        with self.assertRaises(DimensionsError):
            chunk_utils.ensure_array_5d(a)

    def test_ensure_shape_5d(self):
        # Test padding if <5D
        s = (1,) * 3
        expected_shape = (1,) * 5
        actual_shape = chunk_utils.ensure_shape_5d(s)
        self.assertEqual(expected_shape, actual_shape)

        # Test exception if >5D
        s = (1,) * 6
        with self.assertRaises(DimensionsError):
            chunk_utils.ensure_shape_5d(s)

    def test_guess_chunks(self):
        data_shape = (2048, 2048, 2048)
        target_size = 128 * (1024**2)  # 128MB
        itemsize = 2

        expected_chunks = (16, 2048, 2048)
        actual_chunks = chunk_utils.guess_chunks(
            data_shape, target_size, itemsize, mode="z"
        )
        self.assertEqual(expected_chunks, actual_chunks)

        actual_chunks = chunk_utils.guess_chunks(
            data_shape, target_size, itemsize, mode="cycle"
        )
        expected_chunks = (256, 512, 512)
        self.assertEqual(expected_chunks, actual_chunks)

        expected_chunks = (407, 407, 407)
        actual_chunks = chunk_utils.guess_chunks(
            data_shape, target_size, itemsize, mode="iso"
        )
        self.assertEqual(expected_chunks, actual_chunks)

        # Test expected exceptions
        with self.assertRaises(ValueError):
            # shape dims == 0
            chunk_utils.guess_chunks((256, 256, 0), target_size, itemsize)

        with self.assertRaises(ValueError):
            # 0 target_size
            chunk_utils.guess_chunks(data_shape, 0, itemsize)

        with self.assertRaises(ValueError):
            # 0 item_size
            chunk_utils.guess_chunks(data_shape, target_size, 0)

    def test_expand_chunks(self):
        data_shape = (2048, 2048, 2048)
        chunks = (32, 32, 32)
        target_size = 128 * (1024**2)  # 128MB
        itemsize = 2

        expected_chunks = (416, 416, 416)
        actual_chunks = chunk_utils.expand_chunks(
            chunks, data_shape, target_size, itemsize, mode="iso"
        )
        self.assertEqual(expected_chunks, actual_chunks)

        expected_chunks = (512, 512, 256)
        actual_chunks = chunk_utils.expand_chunks(
            chunks, data_shape, target_size, itemsize, mode="cycle"
        )
        self.assertEqual(expected_chunks, actual_chunks)

        # Test expected exceptions
        with self.assertRaises(ValueError):
            # 0 chunk dims
            chunk_utils.expand_chunks(
                (256, 256, 0), data_shape, target_size, itemsize
            )

        with self.assertRaises(ValueError):
            # 0 shape dims
            chunk_utils.expand_chunks(
                chunks, (256, 256, 0), target_size, itemsize
            )

        with self.assertRaises(ValueError):
            # chunks larger than shape
            chunk_utils.expand_chunks(
                (128, 128, 129), (128, 128, 128), target_size, itemsize
            )

        with self.assertRaises(ValueError):
            # 0 target size
            chunk_utils.expand_chunks(chunks, data_shape, 0, itemsize)

        with self.assertRaises(ValueError):
            # 0 itemsize
            chunk_utils.expand_chunks(chunks, data_shape, target_size, 0)

    def test_closer_to_target(self):
        target_bytes = 128 * (1024**2)
        itemsize = 2

        s1 = (1024, 256, 256)
        s2 = (256, 256, 1024)
        closer = chunk_utils._closer_to_target(s1, s2, target_bytes, itemsize)
        self.assertEqual(closer, s2)

        s1 = (1024, 256, 256)
        s2 = (1024, 256, 257)
        closer = chunk_utils._closer_to_target(s1, s2, target_bytes, itemsize)
        self.assertEqual(closer, s1)

        s1 = (1024, 256, 255)
        s2 = (1024, 256, 256)
        closer = chunk_utils._closer_to_target(s1, s2, target_bytes, itemsize)
        self.assertEqual(closer, s2)

    def test_get_size(self):
        shape = (32, 256, 512)
        itemsize = 2

        expected_size = 8388608
        actual_size = chunk_utils._get_size(shape, itemsize)
        self.assertEqual(expected_size, actual_size)


if __name__ == "__main__":
    unittest.main()
