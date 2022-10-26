import unittest

from aind_data_transfer.s3 import S3Uploader


class TestS3Uploader(unittest.TestCase):
    """Tests methods defined in S3Uploader class"""

    def test_s3_uploader_init(self):
        """Tests that the S3Uploader can be instantiated."""
        uploader = S3Uploader()
        self.assertIsNotNone(uploader)


if __name__ == "__main__":
    unittest.main()
