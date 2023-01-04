"""Test module for generic upload job."""

import json
import os
import unittest
from io import StringIO
from unittest.mock import MagicMock, Mock, call, mock_open, patch

from aind_codeocean_api.codeocean import CodeOceanClient
from botocore.exceptions import ClientError

from aind_data_transfer.jobs.s3_upload_job import GenericS3UploadJob


class TestGenericS3UploadJob(unittest.TestCase):
    """Unit tests for methods in GenericS3UploadJob class."""

    # Some fake args that can be used to construct a basic job
    fake_endpoints_str = (
        '{"metadata_service_url": "http://metada_service_url",'
        '"codeocean_domain": "https://codeocean.acme.org",'
        '"codeocean_trigger_capsule": "abc-123",'
        '"codeocean-api-token": "some_token"}'
    )
    args1 = [
        "-d",
        "some_dir",
        "-b",
        "some_s3_bucket",
        "-s",
        "12345",
        "-m",
        "ecephys",
        "-a",
        "2022-10-10",
        "-t",
        "13-24-01",
        "-e",
        fake_endpoints_str,
        "--dry-run",
    ]

    @staticmethod
    def _mock_boto_get_secret_session(secret_string: str) -> MagicMock:
        """
        Utility method to return a mocked boto session. A call to client method
         get_secret_value will return {'SecretString': secret_string}
        Parameters
        ----------
        secret_string : A mock string attached to get_secret_value

        Returns
        -------
        MagicMock
          A mocked boto session object

        """
        mock_session_object = Mock()
        mock_client = Mock()
        mock_client.get_secret_value.return_value = {
            "SecretString": secret_string
        }
        mock_session_object.client.return_value = mock_client
        return mock_session_object

    @staticmethod
    def _mock_boto_get_secret_session_error() -> MagicMock:
        """
        Utility method to return a mocked boto session. A call to client method
         get_secret_value will raise a ClientError exception

        Returns
        -------
        MagicMock
          A mocked boto session object

        """
        mock_session_object = Mock()
        mock_client = Mock()
        mock_client.get_secret_value.side_effect = Mock(
            side_effect=ClientError(
                error_response=(
                    {"Error": {"Code": "500", "Message": "Error"}}
                ),
                operation_name=None,
            )
        )
        mock_session_object.client.return_value = mock_client
        return mock_session_object

    def test_create_s3_prefix(self) -> None:
        """Tests that a s3 prefix is created correctly from job configs."""
        job = GenericS3UploadJob(self.args1)

        expected_s3_prefix = "ecephys_12345_2022-10-10_13-24-01"
        actual_s3_prefix = job.s3_prefix
        self.assertEqual(expected_s3_prefix, actual_s3_prefix)

    def test_create_s3_prefix_error(self) -> None:
        """Tests that errors are raised if the data/time strings are
        malformed."""
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

    @patch("sys.stderr", new_callable=StringIO)
    def test_load_configs(self, mock_stderr: MagicMock) -> None:
        """Tests that the sysargs are parsed correctly."""
        job = GenericS3UploadJob(self.args1)
        expected_configs_vars = {
            "data_source": "some_dir",
            "s3_bucket": "some_s3_bucket",
            "subject_id": "12345",
            "modality": "ecephys",
            "acq_date": "2022-10-10",
            "acq_time": "13-24-01",
            "s3_region": "us-west-2",
            "service_endpoints": json.loads(self.fake_endpoints_str),
            "dry_run": True,
        }
        self.assertEqual(expected_configs_vars, vars(job.configs))

        missing_arg = [
            "-d",
            "some_dir",
            "-b",
            "some_s3_bucket",
            "-s",
            "12345",
            "-m",
            "ecephys",
            "-a",
            "2022-10-10",
        ]

        with self.assertRaises(SystemExit):
            GenericS3UploadJob(missing_arg)
        self.assertRegexpMatches(
            mock_stderr.getvalue(),
            r"the following arguments are required: -t/--acq-time",
        )

    @patch("logging.Logger.warning")
    @patch("boto3.session.Session")
    def test_get_endpoints(
        self, mock_session: MagicMock, mock_log: MagicMock
    ) -> None:
        """Tests that the service endpoints are loaded correctly either from
        being set in the sys args or pulled from aws Secrets Manager"""
        job = GenericS3UploadJob(self.args1)
        expected_endpoints1 = json.loads(self.fake_endpoints_str)
        self.assertEqual(expected_endpoints1, job.configs.service_endpoints)

        # Check job loads endpoints from s3 secrets
        args_without_endpoints = [
            "-d",
            "some_dir",
            "-b",
            "some_s3_bucket",
            "-s",
            "12345",
            "-m",
            "ecephys",
            "-a",
            "2022-10-10",
            "-t",
            "10-10-00",
            "--dry-run",
        ]
        mock_session.return_value = self._mock_boto_get_secret_session(
            self.fake_endpoints_str
        )
        job2 = GenericS3UploadJob(args_without_endpoints)
        self.assertEqual(expected_endpoints1, job2.configs.service_endpoints)

        # Check endpoints are empty if not in sys args or in aws
        mock_session.return_value = self._mock_boto_get_secret_session_error()
        job3 = GenericS3UploadJob(args_without_endpoints)
        self.assertEqual({}, job3.configs.service_endpoints)
        mock_log.assert_called_with(
            f"Unable to retrieve aws secret: {job3.SERVICE_ENDPOINT_KEY}"
        )

    @patch("aind_data_transfer.jobs.s3_upload_job.copy_to_s3")
    @patch("logging.Logger.warning")
    @patch(
        "aind_data_transfer.transformations.metadata_creation.SubjectMetadata."
        "ephys_job_to_subject"
    )
    @patch("tempfile.NamedTemporaryFile", new_callable=mock_open())
    @patch("boto3.session.Session")
    def test_upload_subject_metadata(
        self,
        mock_session: MagicMock,
        mocked_tempfile: MagicMock,
        mocked_ephys_job_to_subject: MagicMock,
        mock_log: MagicMock,
        mock_copy_to_s3: MagicMock,
    ) -> None:
        """Tests that subject data is uploaded correctly."""

        # Check that tempfile is called and copy to s3 is called
        mock_session.return_value = self._mock_boto_get_secret_session_error()
        mocked_ephys_job_to_subject.return_value = {}
        job = GenericS3UploadJob(self.args1)
        job.upload_subject_metadata()
        tmp_file_handle = mocked_tempfile.return_value.__enter__()
        tmp_file_handle.write.assert_called_once()
        mock_copy_to_s3.assert_called_once_with(
            file_to_upload=tmp_file_handle.name,
            s3_bucket="some_s3_bucket",
            s3_prefix="ecephys_12345_2022-10-10_13-24-01/subject.json",
            dryrun=True,
        )

        # Check warning message if not metadata url is found
        empty_args = self.args1.copy()
        empty_args[13] = "{}"
        job2 = GenericS3UploadJob(empty_args)
        job2.upload_subject_metadata()
        mock_log.assert_called_once_with(
            "No metadata service url given. "
            "Not able to get subject metadata."
        )

    @patch("aind_data_transfer.jobs.s3_upload_job.copy_to_s3")
    @patch("tempfile.NamedTemporaryFile", new_callable=mock_open())
    @patch("boto3.session.Session")
    def test_upload_data_description_metadata(
        self,
        mock_session: MagicMock,
        mocked_tempfile: MagicMock,
        mock_copy_to_s3: MagicMock,
    ) -> None:
        """Tests data description is uploaded correctly."""

        # Check that tempfile is called and copy to s3 is called
        mock_session.return_value = self._mock_boto_get_secret_session_error()
        job = GenericS3UploadJob(self.args1)
        job.upload_data_description_metadata()
        tmp_file_handle = mocked_tempfile.return_value.__enter__()
        mock_copy_to_s3.assert_called_once_with(
            file_to_upload=tmp_file_handle.name,
            s3_bucket="some_s3_bucket",
            s3_prefix=(
                "ecephys_12345_2022-10-10_13-24-01/data_description.json"
            ),
            dryrun=True,
        )
        tmp_file_handle.write.assert_called_once()

    @patch.dict(
        os.environ,
        ({f"{GenericS3UploadJob.CODEOCEAN_TOKEN_KEY_ENV}": "abc-12345"}),
    )
    @patch("boto3.session.Session")
    @patch("logging.Logger.warning")
    def test_get_codeocean_client(
        self, mock_log: MagicMock, mock_session: MagicMock
    ) -> None:
        """Tests that the codeocean client is constructed correctly."""

        # Check api token pulled from env var
        job = GenericS3UploadJob(self.args1)
        co_client = job._get_codeocean_client()
        expected_domain = job.configs.service_endpoints.get(
            job.CODEOCEAN_DOMAIN_KEY
        )
        expected_co_client = CodeOceanClient(
            domain=expected_domain, token="abc-12345"
        )
        self.assertEqual(expected_co_client.domain, co_client.domain)
        self.assertEqual(expected_co_client.token, co_client.token)

        # Check api token pulled from aws secrets
        del os.environ[job.CODEOCEAN_TOKEN_KEY_ENV]
        mock_session.return_value = self._mock_boto_get_secret_session(
            f'{{"{job.CODEOCEAN_READ_WRITE_KEY}": "abc-12345"}}'
        )
        job2 = GenericS3UploadJob(self.args1)
        co_client2 = job2._get_codeocean_client()
        self.assertEqual(expected_co_client.domain, co_client2.domain)
        self.assertEqual(expected_co_client.token, co_client2.token)

        # Check warnings if api token not found
        mock_session.return_value = self._mock_boto_get_secret_session_error()
        job3 = GenericS3UploadJob(self.args1)
        co_client3 = job3._get_codeocean_client()
        mock_log.assert_called_with(
            f"Unable to retrieve aws secret: {job.CODEOCEAN_TOKEN_KEY}"
        )
        self.assertIsNone(co_client3)

    def test_codeocean_trigger_capsule_parameters(self):
        """Tests capsule parameters are created correctly."""

        job = GenericS3UploadJob(self.args1)
        expected_capsule_parameters = {
            "trigger_codeocean_job": {
                "job_type": job.CODEOCEAN_JOB_TYPE,
                "capsule_id": "abc-123",
                "bucket": job.configs.s3_bucket,
                "prefix": job.s3_prefix,
            }
        }
        capsule_parameters = job._codeocean_trigger_capsule_parameters()
        self.assertEqual(expected_capsule_parameters, capsule_parameters)

    @patch.dict(
        os.environ,
        ({f"{GenericS3UploadJob.CODEOCEAN_TOKEN_KEY_ENV}": "abc-12345"}),
    )
    @patch.object(CodeOceanClient, "get_capsule")
    @patch.object(CodeOceanClient, "run_capsule")
    @patch("logging.Logger.info")
    @patch("logging.Logger.debug")
    @patch("logging.Logger.warning")
    def test_trigger_capsule(
        self,
        mock_log_warning: MagicMock,
        mock_log_debug: MagicMock,
        mock_log_info: MagicMock,
        mock_run_capsule: MagicMock,
        mock_get_capsule: MagicMock,
    ) -> None:
        """Tests that the codeocean capsule is triggered correctly."""

        # Test dry-run
        mock_get_capsule.return_value = "Ran a capsule!"
        job = GenericS3UploadJob(self.args1)
        capsule_parameters = job._codeocean_trigger_capsule_parameters()
        job.trigger_codeocean_capsule()
        mock_get_capsule.assert_called_once()
        mock_log_info.assert_has_calls(
            [
                call("Triggering capsule run."),
                call(
                    f"Would have ran capsule abc-123 at "
                    f"https://codeocean.acme.org with parameters: "
                    f"{[json.dumps(capsule_parameters)]}"
                    f"."
                ),
            ]
        )

        # Test non dry-run
        mock_run_capsule.return_value.json = lambda: "Success!"
        job.configs.dry_run = False
        job.trigger_codeocean_capsule()
        mock_log_debug.assert_called_once_with("Run response: Success!")

        # Check warning message if codeocean endpoints not configured
        empty_args = self.args1.copy()
        empty_args[13] = "{}"
        job2 = GenericS3UploadJob(empty_args)
        job2.trigger_codeocean_capsule()
        mock_log_warning.assert_called_once_with(
            "CodeOcean endpoints are required to trigger capsule."
        )

    @patch.dict(
        os.environ,
        ({f"{GenericS3UploadJob.CODEOCEAN_TOKEN_KEY_ENV}": "abc-12345"}),
    )
    @patch("aind_data_transfer.jobs.s3_upload_job.upload_to_s3")
    @patch.object(GenericS3UploadJob, "upload_subject_metadata")
    @patch.object(GenericS3UploadJob, "upload_data_description_metadata")
    @patch.object(GenericS3UploadJob, "trigger_codeocean_capsule")
    def test_run_job(
        self,
        mock_trigger_codeocean_capsule: MagicMock,
        mock_upload_data_description_metadata: MagicMock,
        mock_upload_subject_metadata: MagicMock,
        mock_upload_to_s3: MagicMock,
    ) -> None:
        """Tests that the run_job method triggers all the sub jobs."""

        job = GenericS3UploadJob(self.args1)
        job.run_job()
        data_prefix = "/".join([job.s3_prefix, job.configs.modality])

        mock_trigger_codeocean_capsule.assert_called_once()
        mock_upload_data_description_metadata.assert_called_once()
        mock_upload_subject_metadata.assert_called_once()
        mock_upload_to_s3.assert_called_once_with(
            directory_to_upload=job.configs.data_source,
            s3_bucket=job.configs.s3_bucket,
            s3_prefix=data_prefix,
            dryrun=job.configs.dry_run,
        )
