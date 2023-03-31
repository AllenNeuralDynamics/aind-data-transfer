"""Test module for generic upload job."""

import json
import os
import unittest
from io import StringIO
from pathlib import Path
from unittest.mock import MagicMock, Mock, call, mock_open, patch

from aind_codeocean_api.codeocean import CodeOceanClient
from botocore.exceptions import ClientError

from aind_data_transfer.jobs.s3_upload_job import (
    GenericS3UploadJob,
    GenericS3UploadJobList,
)
from aind_data_transfer.transformations.metadata_creation import (
    ProceduresMetadata,
    SubjectMetadata,
)

TEST_DIR = Path(os.path.dirname(os.path.realpath(__file__)))
CONFIGS_DIR = TEST_DIR / "resources" / "test_configs"
METADATA_DIR = TEST_DIR / "resources" / "test_metadata"


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

    def test_parse_date(self) -> None:
        """Tests the extra date parsing method"""
        date1 = "12/10/2021"
        date2 = "2021-10-12"
        date3 = "2021.10.12"
        date4 = "1/3/2023"
        date5 = "14/12/2023"
        parsed_date1 = GenericS3UploadJob._parse_date(date1)
        self.assertEqual(parsed_date1, "2021-12-10")
        parsed_date2 = GenericS3UploadJob._parse_date(date2)
        self.assertEqual(parsed_date2, "2021-10-12")
        with self.assertRaises(ValueError):
            GenericS3UploadJob._parse_date(date3)
        parsed_date4 = GenericS3UploadJob._parse_date(date4)
        self.assertEqual(parsed_date4, "2023-01-03")
        with self.assertRaises(ValueError):
            GenericS3UploadJob._parse_date(date5)

    def test_parse_time(self) -> None:
        """Tests the extra time parsing method"""
        time1 = "5:26:59"
        time2 = "05-03-59"
        time3 = "5.26.59"
        time4 = "5:91:40"
        parsed_time1 = GenericS3UploadJob._parse_time(time1)
        self.assertEqual(parsed_time1, "05-26-59")
        parsed_time2 = GenericS3UploadJob._parse_time(time2)
        self.assertEqual(parsed_time2, "05-03-59")
        with self.assertRaises(ValueError):
            GenericS3UploadJob._parse_time(time3)
        with self.assertRaises(ValueError):
            GenericS3UploadJob._parse_time(time4)

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
            "compress_raw_data": False,
            "behavior_dir": None,
            "metadata_dir": None,
            "metadata_dir_force": False,
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

        # Job where endpoints are defined in args
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
        "from_service"
    )
    @patch("tempfile.TemporaryDirectory")
    @patch("boto3.session.Session")
    @patch("builtins.open", new_callable=mock_open())
    def test_upload_subject_metadata(
        self,
        mock_open_file: MagicMock,
        mock_session: MagicMock,
        mocked_tempdir: MagicMock,
        mocked_ephys_job_to_subject: MagicMock,
        mock_log: MagicMock,
        mock_copy_to_s3: MagicMock,
    ) -> None:
        """Tests that subject data is uploaded correctly."""

        # Check that tempfile is called and copy to s3 is called
        mock_session.return_value = self._mock_boto_get_secret_session_error()
        mocked_ephys_job_to_subject.return_value = SubjectMetadata(
            model_obj={}
        )
        mocked_tempdir.return_value.__enter__ = lambda _: "tmp_dir"
        tmp_file_name = os.path.join("tmp_dir", "subject.json")
        job = GenericS3UploadJob(self.args1)
        job.upload_subject_metadata()
        mock_open_file.assert_called_once_with(tmp_file_name, "w")
        mocked_tempdir.assert_called_once()
        mock_copy_to_s3.assert_called_once_with(
            file_to_upload=tmp_file_name,
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
    @patch("logging.Logger.warning")
    @patch(
        "aind_data_transfer.transformations.metadata_creation."
        "ProceduresMetadata.from_service"
    )
    @patch("tempfile.TemporaryDirectory")
    @patch("boto3.session.Session")
    @patch("builtins.open", new_callable=mock_open())
    def test_upload_procedures_metadata(
        self,
        mock_open_file: MagicMock,
        mock_session: MagicMock,
        mocked_tempdir: MagicMock,
        mocked_ephys_job_to_subject: MagicMock,
        mock_log: MagicMock,
        mock_copy_to_s3: MagicMock,
    ) -> None:
        """Tests that procedures data is uploaded correctly."""

        # Check that tempfile is called and copy to s3 is called
        mock_session.return_value = self._mock_boto_get_secret_session_error()
        mocked_ephys_job_to_subject.return_value = ProceduresMetadata(
            model_obj={}
        )
        mocked_tempdir.return_value.__enter__ = lambda _: "tmp_dir"
        tmp_file_name = os.path.join("tmp_dir", "procedures.json")
        job = GenericS3UploadJob(self.args1)
        job.upload_procedures_metadata()
        mock_open_file.assert_called_once_with(tmp_file_name, "w")
        mocked_tempdir.assert_called_once()
        mock_copy_to_s3.assert_called_once_with(
            file_to_upload=tmp_file_name,
            s3_bucket="some_s3_bucket",
            s3_prefix="ecephys_12345_2022-10-10_13-24-01/procedures.json",
            dryrun=True,
        )

        # Check warning message if not metadata url is found
        empty_args = self.args1.copy()
        empty_args[13] = "{}"
        job2 = GenericS3UploadJob(empty_args)
        job2.upload_procedures_metadata()
        mock_log.assert_called_once_with(
            "No metadata service url given. "
            "Not able to get procedures metadata."
        )

    @patch("aind_data_transfer.jobs.s3_upload_job.copy_to_s3")
    @patch("tempfile.TemporaryDirectory")
    @patch("boto3.session.Session")
    @patch("builtins.open", new_callable=mock_open())
    def test_upload_data_description_metadata(
        self,
        mock_open_file: MagicMock,
        mock_session: MagicMock,
        mocked_tempdir: MagicMock,
        mock_copy_to_s3: MagicMock,
    ) -> None:
        """Tests data description is uploaded correctly."""

        # Check that tempfile is called and copy to s3 is called
        mock_session.return_value = self._mock_boto_get_secret_session_error()
        mocked_tempdir.return_value.__enter__ = lambda _: "tmp_dir"
        tmp_file_name = os.path.join("tmp_dir", "data_description.json")
        job = GenericS3UploadJob(self.args1)
        job.upload_data_description_metadata()
        mock_open_file.assert_called_once_with(tmp_file_name, "w")
        mocked_tempdir.assert_called_once()
        mock_copy_to_s3.assert_called_once_with(
            file_to_upload=tmp_file_name,
            s3_bucket="some_s3_bucket",
            s3_prefix=(
                "ecephys_12345_2022-10-10_13-24-01/data_description.json"
            ),
            dryrun=True,
        )

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

    @patch("aind_data_transfer.jobs.s3_upload_job.upload_to_s3")
    def test_upload_metadata_folder(self, mock_upload_to_s3: MagicMock):
        """Tests that the optional metadata folder will be uploaded
        correctly."""
        args_with_metadata = ["-x", str(METADATA_DIR)]
        args_with_metadata.extend(self.args1)
        job = GenericS3UploadJob(args_with_metadata)
        actual_files_found = job.upload_metadata_from_folder()
        expected_files_found = [
            "data_description.json",
            "processing.json",
            "subject.json",
        ]
        mock_upload_to_s3.assert_called_once_with(
            directory_to_upload=str(METADATA_DIR),
            s3_bucket="some_s3_bucket",
            s3_prefix="ecephys_12345_2022-10-10_13-24-01",
            dryrun=True,
            excluded="*",
            included="*.json",
        )
        self.assertEqual(len(set(actual_files_found)), len(actual_files_found))
        self.assertEqual(set(expected_files_found), set(actual_files_found))

    @patch("aind_data_transfer.jobs.s3_upload_job.upload_to_s3")
    def test_upload_raw_data_folder(
        self,
        mock_upload_to_s3: MagicMock,
    ) -> None:
        """Tests raw data directory is uploaded correctly."""

        job = GenericS3UploadJob(self.args1)
        job.upload_raw_data_folder(data_prefix="some_prefix")
        job.upload_raw_data_folder(
            data_prefix="some_prefix2", behavior_dir=Path("behavior_dir")
        )

        mock_upload_to_s3.assert_has_calls(
            [
                call(
                    directory_to_upload="some_dir",
                    s3_bucket="some_s3_bucket",
                    s3_prefix="some_prefix",
                    dryrun=True,
                    excluded=None,
                ),
                call(
                    directory_to_upload="some_dir",
                    s3_bucket="some_s3_bucket",
                    s3_prefix="some_prefix2",
                    dryrun=True,
                    excluded=Path("behavior_dir/*"),
                ),
            ]
        )

    @patch("aind_data_transfer.jobs.s3_upload_job.copy_to_s3")
    @patch("tempfile.TemporaryDirectory")
    @patch(
        "aind_data_transfer.transformations.generic_compressors."
        "ZipCompressor.compress_dir"
    )
    def test_upload_raw_data_folder_with_compression(
        self,
        mock_compress: MagicMock,
        mocked_tempdir: MagicMock,
        mock_copy_to_s3: MagicMock,
    ) -> None:
        """Tests raw data directory is compressed and uploaded correctly."""

        compress_data_args = self.args1 + ["--compress-raw-data"]

        mocked_tempdir.return_value.__enter__ = lambda _: "tmp_dir"

        job = GenericS3UploadJob(compress_data_args)
        job.upload_raw_data_folder(data_prefix="some_prefix")
        mock_compress.assert_called_once_with(
            input_dir=Path("some_dir"),
            output_dir=Path("tmp_dir/some_dir.zip"),
            skip_dirs=None,
        )
        mock_copy_to_s3.assert_called_once_with(
            file_to_upload=str(Path("tmp_dir/some_dir.zip")),
            s3_bucket="some_s3_bucket",
            s3_prefix="some_prefix/some_dir.zip",
            dryrun=True,
        )

    @patch("aind_data_transfer.jobs.s3_upload_job.upload_to_s3")
    @patch("tempfile.TemporaryDirectory")
    @patch(
        "aind_data_transfer.transformations.generic_compressors."
        "VideoCompressor.compress_all_videos_in_dir"
    )
    @patch("shutil.copy")
    @patch("os.walk")
    @patch("boto3.session.Session")
    def test_compress_and_upload_behavior_data(
        self,
        mock_session: MagicMock,
        mock_walk: MagicMock,
        mock_copy: MagicMock,
        mock_compress: MagicMock,
        mocked_tempdir: MagicMock,
        mock_upload_to_s3: MagicMock,
    ) -> None:
        """Tests behavior directory is uploaded correctly."""

        args_with_behavior = ["-v", "some_behave_dir"]
        args_with_behavior.extend(self.args1)
        mock_session.return_value = self._mock_boto_get_secret_session(
            "video_encryption_password"
        )
        mocked_tempdir.return_value.__enter__ = lambda _: "tmp_dir"
        mock_walk.return_value = [
            ("some_behave_dir", "", ["foo1.avi", "foo2.avi"])
        ]
        job = GenericS3UploadJob(args_with_behavior)
        job.compress_and_upload_behavior_data()
        mock_copy.assert_has_calls(
            [
                call("some_behave_dir/foo1.avi", "tmp_dir/foo1.avi"),
                call("some_behave_dir/foo2.avi", "tmp_dir/foo2.avi"),
            ]
        )
        mock_compress.assert_called_once_with("tmp_dir")
        mock_upload_to_s3.assert_called_once_with(
            directory_to_upload="tmp_dir",
            s3_bucket="some_s3_bucket",
            s3_prefix="ecephys_12345_2022-10-10_13-24-01/behavior",
            dryrun=True,
        )

    @patch.dict(
        os.environ,
        ({f"{GenericS3UploadJob.CODEOCEAN_TOKEN_KEY_ENV}": "abc-12345"}),
    )
    @patch("aind_data_transfer.jobs.s3_upload_job.upload_to_s3")
    @patch.object(GenericS3UploadJob, "upload_subject_metadata")
    @patch.object(GenericS3UploadJob, "upload_procedures_metadata")
    @patch.object(GenericS3UploadJob, "upload_data_description_metadata")
    @patch.object(GenericS3UploadJob, "trigger_codeocean_capsule")
    @patch.object(GenericS3UploadJob, "compress_and_upload_behavior_data")
    def test_run_job(
        self,
        mock_compress_and_upload_behavior: MagicMock,
        mock_trigger_codeocean_capsule: MagicMock,
        mock_upload_data_description_metadata: MagicMock,
        mock_upload_procedures_metadata: MagicMock,
        mock_upload_subject_metadata: MagicMock,
        mock_upload_to_s3: MagicMock,
    ) -> None:
        """Tests that the run_job method triggers all the sub jobs."""

        job = GenericS3UploadJob(self.args1)
        job.configs.behavior_dir = "some_behavior_dir"
        job.run_job()
        data_prefix = "/".join([job.s3_prefix, job.configs.modality])

        mock_compress_and_upload_behavior.assert_called_once()
        mock_trigger_codeocean_capsule.assert_called_once()
        mock_upload_data_description_metadata.assert_called_once()
        mock_upload_subject_metadata.assert_called_once()
        mock_upload_procedures_metadata.assert_called_once()
        mock_upload_to_s3.assert_called_once_with(
            directory_to_upload=job.configs.data_source,
            s3_bucket=job.configs.s3_bucket,
            s3_prefix=data_prefix,
            dryrun=job.configs.dry_run,
            excluded=(Path("some_behavior_dir") / "*"),
        )

    @patch.dict(
        os.environ,
        ({f"{GenericS3UploadJob.CODEOCEAN_TOKEN_KEY_ENV}": "abc-12345"}),
    )
    @patch.object(GenericS3UploadJob, "upload_raw_data_folder")
    @patch.object(GenericS3UploadJob, "upload_subject_metadata")
    @patch.object(GenericS3UploadJob, "upload_procedures_metadata")
    @patch.object(GenericS3UploadJob, "upload_data_description_metadata")
    @patch.object(GenericS3UploadJob, "trigger_codeocean_capsule")
    @patch.object(GenericS3UploadJob, "compress_and_upload_behavior_data")
    def test_run_job_with_compress(
        self,
        mock_compress_and_upload_behavior: MagicMock,
        mock_trigger_codeocean_capsule: MagicMock,
        mock_upload_data_description_metadata: MagicMock,
        mock_upload_procedures_metadata: MagicMock,
        mock_upload_subject_metadata: MagicMock,
        mock_upload_raw_data_folder: MagicMock,
    ) -> None:
        """Tests that the run_job method triggers all the sub jobs."""

        job = GenericS3UploadJob(self.args1)
        job.configs.behavior_dir = "some_behavior_dir"
        job.run_job()
        data_prefix = "/".join([job.s3_prefix, job.configs.modality])

        mock_compress_and_upload_behavior.assert_called_once()
        mock_trigger_codeocean_capsule.assert_called_once()
        mock_upload_data_description_metadata.assert_called_once()
        mock_upload_subject_metadata.assert_called_once()
        mock_upload_procedures_metadata.assert_called_once()
        mock_upload_raw_data_folder.assert_called_once_with(
            data_prefix=data_prefix,
            behavior_dir=Path("some_behavior_dir"),
        )


class TestGenericS3UploadJobList(unittest.TestCase):
    """Unit tests for methods in GenericS3UploadJobs class."""

    PATH_TO_EXAMPLE_CSV_FILE = (
        Path(os.path.dirname(os.path.realpath(__file__)))
        / "resources"
        / "test_configs"
        / "jobs_list.csv"
    )

    PATH_TO_EXAMPLE_CSV_FILE2 = (
        Path(os.path.dirname(os.path.realpath(__file__)))
        / "resources"
        / "test_configs"
        / "jobs_list_2.csv"
    )

    def test_load_configs(self) -> None:
        """Tests configs are loaded correctly."""
        expected_param_list = [
            [
                "--data-source",
                "dir/data_set_1",
                "--s3-bucket",
                "some_bucket",
                "--subject-id",
                "123454",
                "--modality",
                "ecephys",
                "--acq-date",
                "2020-10-10",
                "--acq-time",
                "14-10-10",
            ],
            [
                "--data-source",
                "dir/data_set_2",
                "--s3-bucket",
                "some_bucket",
                "--subject-id",
                "123456",
                "--modality",
                "ecephys",
                "--acq-date",
                "2020-10-11",
                "--acq-time",
                "13-10-10",
            ],
        ]
        args = ["-j", str(self.PATH_TO_EXAMPLE_CSV_FILE)]
        jobs = GenericS3UploadJobList(args=args)
        dry_run_args = args + ["--dry-run"]
        dry_run_jobs = GenericS3UploadJobList(args=dry_run_args)
        expected_param_list_dry_run = [
            r + ["--dry-run"] for r in expected_param_list
        ]
        self.assertEqual(expected_param_list, jobs.job_param_list)
        self.assertEqual(
            expected_param_list_dry_run, dry_run_jobs.job_param_list
        )

    @patch("boto3.session.Session")
    @patch("aind_data_transfer.jobs.s3_upload_job.GenericS3UploadJob")
    @patch("logging.Logger.info")
    def test_run_job(
        self, mock_log: MagicMock, mock_job: MagicMock, mock_session: MagicMock
    ) -> None:
        """Tests that the jobs are run correctly."""

        mock_session.return_value = (
            TestGenericS3UploadJob._mock_boto_get_secret_session(
                TestGenericS3UploadJob.fake_endpoints_str
            )
        )

        mock_job.run_job.return_value = lambda: print("Ran Job!")

        args = [
            "-j",
            str(self.PATH_TO_EXAMPLE_CSV_FILE),
            "--dry-run",
            "--compress-raw-data",
        ]
        jobs = GenericS3UploadJobList(args=args)

        params_0 = jobs.job_param_list[0]
        params_1 = jobs.job_param_list[1]

        jobs.run_job()

        mock_log.assert_has_calls(
            [
                call("Starting all jobs..."),
                call(f"Running job 1 of 2 with params: {params_0}"),
                call(f"Finished job 1 of 2 with params: {params_0}"),
                call(f"Running job 2 of 2 with params: {params_1}"),
                call(f"Finished job 2 of 2 with params: {params_1}"),
                call("Finished all jobs!"),
            ]
        )

        # Check that the GenericS3UploadJob constructor is called and
        # GenericS3UploadJob().run_job() is called.
        mock_job.assert_has_calls(
            [
                call(params_0),
                call().run_job(),
                call(params_1),
                call().run_job(),
            ]
        )

    @patch("boto3.session.Session")
    @patch("aind_data_transfer.jobs.s3_upload_job.GenericS3UploadJob")
    @patch("logging.Logger.info")
    def test_run_job2(
        self, mock_log: MagicMock, mock_job: MagicMock, mock_session: MagicMock
    ) -> None:
        """Tests that the jobs with optional args are run correctly."""

        mock_session.return_value = (
            TestGenericS3UploadJob._mock_boto_get_secret_session(
                TestGenericS3UploadJob.fake_endpoints_str
            )
        )

        mock_job.run_job.return_value = lambda: print("Ran Job!")

        args = ["-j", str(self.PATH_TO_EXAMPLE_CSV_FILE2), "--dry-run"]
        jobs = GenericS3UploadJobList(args=args)

        params_0 = jobs.job_param_list[0]
        params_1 = jobs.job_param_list[1]
        params_2 = jobs.job_param_list[2]

        jobs.run_job()

        mock_log.assert_has_calls(
            [
                call("Starting all jobs..."),
                call(f"Running job 1 of 3 with params: {params_0}"),
                call(f"Finished job 1 of 3 with params: {params_0}"),
                call(f"Running job 2 of 3 with params: {params_1}"),
                call(f"Finished job 2 of 3 with params: {params_1}"),
                call(f"Running job 3 of 3 with params: {params_2}"),
                call(f"Finished job 3 of 3 with params: {params_2}"),
                call("Finished all jobs!"),
            ]
        )

        # Check that the GenericS3UploadJob constructor is called and
        # GenericS3UploadJob().run_job() is called.
        mock_job.assert_has_calls(
            [
                call(params_0),
                call().run_job(),
                call(params_1),
                call().run_job(),
                call(params_2),
                call().run_job(),
            ]
        )


if __name__ == "__main__":
    unittest.main()
