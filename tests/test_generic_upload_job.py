import unittest
from io import StringIO
from unittest.mock import patch, Mock
import json
from botocore.exceptions import ClientError
from aind_data_transfer.jobs.s3_upload_job import GenericS3UploadJob


class TestProcessingMetadata(unittest.TestCase):

    fake_endpoints_str = (
            '{"metadata_service_url": "http://metada_service_url",'
            '"codeocean_domain": "https://codeocean.acme.org",'
            '"codeocean_trigger_capsule": "abc-123",'
            '"codeocean-api-token": "some_token"}'
        )
    args1 = (
        ["-d", "some_dir", "-b", "some_s3_bucket", "-s", "12345",
         "-m", "ecephys", "-a", "2022-10-10", "-t", "13-24-01",
         "-e", fake_endpoints_str, "--dry-run"]
    )

    def test_create_s3_prefix(self):
        job = GenericS3UploadJob(self.args1)

        expected_s3_prefix = "ecephys_12345_2022-10-10_13-24-01"
        actual_s3_prefix = job.s3_prefix
        self.assertEqual(expected_s3_prefix, actual_s3_prefix)

    def test_create_s3_prefix_error(self):
        bad_args = self.args1.copy()

        with self.assertRaises(ValueError):
            bad_args[9] = "2020-13-10"  # Bad Month
            GenericS3UploadJob(bad_args).s3_prefix()

        with self.assertRaises(ValueError):
            bad_args[9] = "2020-12-32"  # Bad Day
            GenericS3UploadJob(bad_args).s3_prefix()

        with self.assertRaises(ValueError):
            bad_args[9] = "2020-12-31"
            bad_args[11] = "24-59-01"  # Bad Hour
            GenericS3UploadJob(bad_args).s3_prefix()

        with self.assertRaises(ValueError):
            bad_args[11] = "12-60-01"  # Bad Minute
            GenericS3UploadJob(bad_args).s3_prefix()

        with self.assertRaises(ValueError):
            bad_args[11] = "12-59-60"  # Bad Second
            GenericS3UploadJob(bad_args).s3_prefix()

    @patch('sys.stderr', new_callable=StringIO)
    def test_load_configs(self, mock_stderr):
        job = GenericS3UploadJob(self.args1)
        expected_configs_vars = (
            {"data_source": "some_dir",
             "s3_bucket": "some_s3_bucket",
             "subject_id": "12345",
             "modality": "ecephys",
             "acq_date": "2022-10-10",
             "acq_time": "13-24-01",
             "s3_region": "us-west-2",
             "service_endpoints": json.loads(self.fake_endpoints_str),
             "dry_run": True}
        )
        self.assertEqual(expected_configs_vars, vars(job.configs))

        missing_arg = (
            ["-d", "some_dir", "-b", "some_s3_bucket", "-s", "12345",
             "-m", "ecephys", "-a", "2022-10-10"]
        )

        with self.assertRaises(SystemExit):
            GenericS3UploadJob(missing_arg)
        self.assertRegexpMatches(
            mock_stderr.getvalue(),
            r"the following arguments are required: -t/--acq-time"
        )

    @patch('logging.Logger.warning')
    @patch("boto3.session.Session")
    def test_get_endpoints(self, mock_session, mock_log):
        job = GenericS3UploadJob(self.args1)
        expected_endpoints1 = json.loads(self.fake_endpoints_str)
        self.assertEqual(expected_endpoints1, job.configs.service_endpoints)

        # Check job loads endpoints from s3 secrets
        args_without_endpoints = (
            ["-d", "some_dir", "-b", "some_s3_bucket", "-s", "12345",
             "-m", "ecephys", "-a", "2022-10-10", "-t", "10-10-00",
             "--dry-run"]
        )
        mock_session_object = Mock()
        mock_client = Mock()
        mock_client.get_secret_value.return_value = ({
            'SecretString': self.fake_endpoints_str
        })
        mock_session_object.client.return_value = mock_client
        mock_session.return_value = mock_session_object
        job2 = GenericS3UploadJob(args_without_endpoints)
        self.assertEqual(expected_endpoints1, job2.configs.service_endpoints)

        # Check endpoints are empty if not in sys args or in aws
        mock_client.get_secret_value.side_effect = Mock(
            side_effect=ClientError(
                error_response=({
                    'Error': {'Code': '500', 'Message': 'Error'}
                }),
                operation_name=None)
        )
        job3 = GenericS3UploadJob(args_without_endpoints)
        self.assertEqual({}, job3.configs.service_endpoints)
        mock_log.assert_called_with(
            f"Unable to retrieve aws secret: {job3.SERVICE_ENDPOINT_KEY}"
        )

    @patch('aind_data_transfer.jobs.s3_upload_job.copy_to_s3')
    @patch('logging.Logger.warning')
    @patch(
        'aind_data_transfer.transformations.metadata_creation.SubjectMetadata.'
        'ephys_job_to_subject'
    )
    @patch('tempfile.NamedTemporaryFile')
    def test_upload_subject_metadata(self,
                                     mocked_tempfile,
                                     mocked_ephys_job_to_subject,
                                     mock_log,
                                     mock_copy_to_s3):
        mocked_ephys_job_to_subject.return_value = {}
        job = GenericS3UploadJob(self.args1)
        job._upload_subject_metadata()
        mock_copy_to_s3.assert_called_once()
        mocked_tempfile.assert_called_once()

        # Check warning message if not metadata url is found
        empty_args = self.args1.copy()
        empty_args[13] = '{}'
        job2 = GenericS3UploadJob(empty_args)
        job2._upload_subject_metadata()
        mock_log.assert_called_with(
            "No metadata service url given. Not able to get subject metadata."
        )


