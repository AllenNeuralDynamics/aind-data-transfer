import unittest
from aind_data_transfer.jobs.s3_upload_job import GenericS3UploadJob


class TestProcessingMetadata(unittest.TestCase):

    def test_create_s3_prefix(self):
        modality = "ecephys"
        subject_id = "12345"
        acq_date = "2020-10-10"
        acq_time = "13-24-01"

        expected_s3_prefix = "ecephys_12345_2020-10-10_13-24-01"
        actual_s3_prefix = GenericS3UploadJob.create_s3_prefix(modality,
                                                               subject_id,
                                                               acq_date,
                                                               acq_time)
        self.assertEqual(expected_s3_prefix, actual_s3_prefix)

    def test_create_s3_prefix_error(self):
        modality = "ecephys"
        subject_id = "12345"

        with self.assertRaises(ValueError):
            GenericS3UploadJob.create_s3_prefix(modality,
                                                subject_id,
                                              "2020-13-10",  # Bad month
                                              "12-01-01")
        with self.assertRaises(ValueError):
            GenericS3UploadJob.create_s3_prefix(modality,
                                                subject_id,
                                              "2020-12-32",  # Bad day
                                              "12-01-01")
        with self.assertRaises(ValueError):
            GenericS3UploadJob.create_s3_prefix(modality,
                                                subject_id,
                                              "2020-12-31",
                                              "24-60-01")  # Bad hour

        with self.assertRaises(ValueError):
            GenericS3UploadJob.create_s3_prefix(modality,
                                                subject_id,
                                              "2020-12-31",
                                              "12-60-01")  # Bad minute
        with self.assertRaises(ValueError):
            GenericS3UploadJob.create_s3_prefix(modality,
                                                subject_id,
                                              "2020-12-31",
                                              "12-59-60")  # Bad second


