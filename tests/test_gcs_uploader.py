import os
import unittest
from unittest import mock

from google.auth.exceptions import DefaultCredentialsError

from aind_data_transfer.gcs import GCSUploader


class TestGCSUploader(unittest.TestCase):
    """Tests methods defined in GCSUploader class"""

    @mock.patch.dict(os.environ, {"GOOGLE_APPLICATION_CREDENTIALS": "NONE"})
    def test_gcs_uploader_init(self):
        """Checks that the GCSUploader can be instantiated. Since GCSUploader
        initializes a cloud storage client, we'll test that it throws an
        error when the credentials don't exist.
        """

        with self.assertRaises(DefaultCredentialsError):
            GCSUploader(bucket_name=None)


if __name__ == "__main__":
    unittest.main()
