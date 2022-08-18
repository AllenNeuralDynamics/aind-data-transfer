import unittest

from transfer.gcs import GCSUploader


class TestGCSUploader(unittest.TestCase):
    """Tests methods defined in GCSUploader class"""

    def test_gcs_uploader_init(self):
        """Checks that the GCSUploader can be instantiated. Since GCSUploader
        initializes a cloud storage client, we'll test that it throws an
        error if the bucket_name is None.
        """

        with self.assertRaises(ValueError):
            GCSUploader(bucket_name=None)


if __name__ == "__main__":
    unittest.main()
