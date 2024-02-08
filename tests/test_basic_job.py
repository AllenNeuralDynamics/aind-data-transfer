"""Tests the basic_job module"""

import json
import os
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, call, patch

from aind_codeocean_api.models.computations_requests import RunCapsuleRequest
from aind_data_schema.core.metadata import Metadata, MetadataStatus
from requests import Response

from aind_data_transfer import __version__
from aind_data_transfer.config_loader.base_config import BasicUploadJobConfigs
from aind_data_transfer.jobs.basic_job import BasicJob
from aind_data_transfer.transformations.metadata_creation import (
    ProceduresMetadata,
    SubjectMetadata,
)

TEST_DIR = Path(os.path.dirname(os.path.realpath(__file__))) / "resources"
DATA_DIR = TEST_DIR / "v0.6.x_neuropixels_multiexp_multistream"
CONFIG_FILE = TEST_DIR / "test_configs" / "ephys_upload_job_test_configs.yml"
BEHAVIOR_DIR = TEST_DIR / "v0.6.x_neuropixels_multiexp_multistream" / "Videos"
METADATA_DIR = TEST_DIR / "test_metadata"

with open(METADATA_DIR / "processing.json", "r") as f:
    example_processing_instance_json = json.load(f)

with open(METADATA_DIR / "data_description.json", "r") as f:
    example_data_description_instance_json = json.load(f)

with open(METADATA_DIR / "subject.json", "r") as f:
    example_subject_instance_json = json.load(f)

with open(METADATA_DIR / "procedures.json", "r") as f:
    example_procedures_instance_json = json.load(f)


class TestBasicJob(unittest.TestCase):
    """Tests methods in the BasicJob class"""

    EXAMPLE_ENV_VAR1 = {
        "CODEOCEAN_DOMAIN": "some_domain",
        "CODEOCEAN_TRIGGER_CAPSULE_ID": "some_capsule_id",
        "METADATA_SERVICE_DOMAIN": "some_ms_domain",
        "AIND_DATA_TRANSFER_REPO_LOCATION": "some_dtr_location",
        "VIDEO_ENCRYPTION_PASSWORD": "some_password",
        "CODEOCEAN_API_TOKEN": "some_api_token",
        "S3_BUCKET": "some_bucket",
        "MODALITIES": f'[{{"modality":"MRI",' f'"source":"{str(DATA_DIR)}"}}]',
        "PLATFORM": "confocal",
        "SUBJECT_ID": "643054",
        "ACQ_DATETIME": "2020-10-10 10:10:10",
        "DATA_SOURCE": str(DATA_DIR),
        "DRY_RUN": "true",
    }

    @patch.dict(os.environ, EXAMPLE_ENV_VAR1, clear=True)
    @patch("tempfile.TemporaryDirectory")
    @patch("aind_data_transfer.jobs.basic_job.upload_to_s3")
    def test_aws_creds_check_allowed(
        self,
        mock_upload_to_s3: MagicMock,
        mock_tempfile: MagicMock,
    ):
        """Tests that the aws credentials pass allowed"""
        mock_tempfile.return_value.__enter__.return_value = (
            Path("some_dir") / "tmp"
        )
        basic_job_configs = BasicUploadJobConfigs()
        basic_job = BasicJob(job_configs=basic_job_configs)
        basic_job._test_upload(Path("some_dir"))
        mock_upload_to_s3.assert_called_once_with(
            directory_to_upload=Path("some_dir"),
            s3_bucket="some_bucket",
            s3_prefix="confocal_643054_2020-10-10_10-10-10",
        )

    @patch.dict(os.environ, EXAMPLE_ENV_VAR1, clear=True)
    @patch("os.mkdir")
    @patch(
        "aind_data_transfer.transformations.generic_compressors.ZipCompressor."
        "compress_dir"
    )
    @patch("shutil.copytree")
    def test_compress_raw_data_no_zip(
        self,
        mock_copytree: MagicMock,
        mock_compress: MagicMock,
        mock_make_dir: MagicMock,
    ):
        """Tests that the raw data is copied to temp directory"""
        basic_job_configs = BasicUploadJobConfigs()
        basic_job = BasicJob(job_configs=basic_job_configs)
        basic_job._compress_raw_data(temp_dir=Path("some_path"))

        # The shutil copy takes care of creating the directory
        mock_make_dir.assert_not_called()
        # With a Behavior directory defined
        basic_job.job_configs.behavior_dir = BEHAVIOR_DIR
        basic_job._compress_raw_data(temp_dir=Path("some_path"))
        mock_copytree.assert_has_calls(
            [
                call(DATA_DIR, Path("some_path/MRI"), ignore=None),
                call(
                    DATA_DIR,
                    Path("some_path/MRI"),
                    ignore=(BEHAVIOR_DIR / "*"),
                ),
            ]
        )

        self.assertFalse(mock_compress.called)

    @patch.dict(os.environ, EXAMPLE_ENV_VAR1, clear=True)
    @patch(
        "aind_data_transfer.transformations.generic_compressors.ZipCompressor."
        "compress_dir"
    )
    @patch("os.mkdir")
    def test_compress_raw_data_with_zip(
        self, mock_mkdir: MagicMock, mock_compress: MagicMock
    ):
        """Tests that the raw data is zipped"""
        basic_job_configs = BasicUploadJobConfigs()
        basic_job_configs.modalities[0].compress_raw_data = True
        basic_job = BasicJob(job_configs=basic_job_configs)
        basic_job._compress_raw_data(temp_dir=Path("some_path"))

        # With a behavior directory defined
        basic_job_configs.behavior_dir = BEHAVIOR_DIR
        basic_job = BasicJob(job_configs=basic_job_configs)
        basic_job._compress_raw_data(temp_dir=Path("some_path"))

        mock_mkdir.assert_has_calls(
            [
                call(Path("some_path/MRI")),
                call(Path("some_path/MRI")),
            ]
        )

        mock_compress.assert_has_calls(
            [
                call(
                    input_dir=DATA_DIR,
                    output_dir=Path(
                        "some_path/MRI/"
                        "v0.6.x_neuropixels_multiexp_multistream.zip"
                    ),
                    skip_dirs=None,
                ),
                call(
                    input_dir=DATA_DIR,
                    output_dir=Path(
                        "some_path/MRI/"
                        "v0.6.x_neuropixels_multiexp_multistream.zip"
                    ),
                    skip_dirs=[BEHAVIOR_DIR],
                ),
            ]
        )

    @patch.dict(os.environ, EXAMPLE_ENV_VAR1, clear=True)
    @patch("os.mkdir")
    @patch(
        "aind_data_transfer.transformations.generic_compressors.ZipCompressor."
        "compress_dir"
    )
    @patch("shutil.copytree")
    @patch("aind_data_transfer.jobs.basic_job.upload_to_s3")
    def test_compress_raw_data_no_zip_skip_staging(
        self,
        mock_upload: MagicMock,
        mock_copytree: MagicMock,
        mock_compress: MagicMock,
        mock_make_dir: MagicMock,
    ):
        """Tests that the raw data is uploaded directly to s3"""
        basic_job_configs = BasicUploadJobConfigs()
        basic_job_configs.modalities[0].skip_staging = True
        basic_job = BasicJob(job_configs=basic_job_configs)
        basic_job._compress_raw_data(temp_dir=Path("some_path"))

        # Should upload directly to s3
        mock_make_dir.assert_not_called()
        mock_copytree.assert_not_called()
        mock_upload.assert_called_once_with(
            directory_to_upload=DATA_DIR,
            s3_bucket="some_bucket",
            s3_prefix="confocal_643054_2020-10-10_10-10-10/MRI",
            dryrun=True,
            excluded=None,
        )
        self.assertFalse(mock_compress.called)

    @patch.dict(os.environ, EXAMPLE_ENV_VAR1, clear=True)
    @patch(
        "aind_data_transfer.transformations.metadata_creation."
        "SubjectMetadata.from_service"
    )
    @patch(
        "aind_data_transfer.transformations.metadata_creation."
        "ProceduresMetadata.from_service"
    )
    @patch(
        "aind_data_transfer.transformations.metadata_creation."
        "MetadataCreation.write_to_json"
    )
    @patch("shutil.copyfile")
    @patch("aind_data_transfer.jobs.basic_job.datetime")
    def test_initialize_metadata(
        self,
        mock_datetime: MagicMock,
        mock_copyfile: MagicMock,
        mock_json_write: MagicMock,
        mock_procedures_service: MagicMock,
        mock_subject_service: MagicMock,
    ):
        """Tests that the metadata files are compiled correctly."""

        mock_datetime.now.return_value = datetime(2023, 4, 9)
        mock_subject_service.return_value = SubjectMetadata(
            model_obj=example_subject_instance_json
        )
        mock_procedures_service.return_value = ProceduresMetadata(
            model_obj=example_procedures_instance_json
        )

        basic_job_configs = BasicUploadJobConfigs()
        basic_job = BasicJob(job_configs=basic_job_configs)
        basic_job._initialize_metadata_record(temp_dir=Path("some_dir"))

        expected_write_to_json_calls = [
            call(Path("some_dir")),
            call(Path("some_dir")),
            call(Path("some_dir")),
        ]
        mock_json_write.assert_has_calls(expected_write_to_json_calls)
        mock_copyfile.assert_not_called()
        self.assertEqual(
            "643054", basic_job.metadata_record.subject.subject_id
        )

    @patch.dict(os.environ, EXAMPLE_ENV_VAR1, clear=True)
    @patch("os.makedirs")
    @patch("shutil.copy")
    @patch(
        "aind_data_transfer.transformations.generic_compressors."
        "VideoCompressor.compress_all_videos_in_dir"
    )
    def test_encrypt_behavior_dir(
        self,
        mock_compress: MagicMock,
        mock_copy: MagicMock,
        mock_mkdirs: MagicMock,
    ):
        """Tests that the video files in the behavior dir are encrypted."""
        basic_job_configs = BasicUploadJobConfigs()
        basic_job = BasicJob(job_configs=basic_job_configs)
        basic_job_configs.behavior_dir = BEHAVIOR_DIR
        basic_job._encrypt_behavior_dir(temp_dir=Path("some_dir"))
        mock_copy.assert_has_calls(
            [
                call(
                    str(BEHAVIOR_DIR / "a_video_file.avi"),
                    "some_dir/behavior/a_video_file.avi",
                ),
                call(
                    str(BEHAVIOR_DIR / "another_video_file.avi"),
                    "some_dir/behavior/another_video_file.avi",
                ),
                call(
                    str(BEHAVIOR_DIR / "video_ref.csv"),
                    "some_dir/behavior/video_ref.csv",
                ),
            ],
            any_order=True,
        )
        mock_compress.assert_called_once_with(Path("some_dir/behavior"))
        mock_mkdirs.assert_has_calls(
            [
                call("some_dir/behavior", exist_ok=True),
                call("some_dir/behavior", exist_ok=True),
                call("some_dir/behavior", exist_ok=True),
            ]
        )

    @patch.dict(os.environ, EXAMPLE_ENV_VAR1, clear=True)
    @patch("aind_data_schema.base.AindCoreModel.write_standard_file")
    @patch(
        "aind_data_transfer.transformations.metadata_creation."
        "MetadataCreation.write_to_json"
    )
    @patch("shutil.copyfile")
    @patch("aind_data_transfer.jobs.basic_job.datetime")
    def test_add_processing_to_metadata(
        self,
        mock_datetime: MagicMock,
        mock_copyfile: MagicMock,
        mock_json_write: MagicMock,
        mock_write_standard_file: MagicMock,
    ):
        """Tests that the processing metadata files are compiled correctly."""
        mock_datetime.now.return_value = datetime(2023, 4, 9)

        basic_job_configs = BasicUploadJobConfigs()
        # Change compress_raw_data to true to check process
        # Test that "valid metadata remains valid if valid processing
        basic_job = BasicJob(job_configs=basic_job_configs)
        basic_job_configs.modalities[0].compress_raw_data = True
        basic_job.metadata_record = Metadata(
            name="some_name", location="some_bucket"
        )
        # Test that "valid metadata remains valid if valid processing
        basic_job.metadata_record.metadata_status = MetadataStatus.VALID
        basic_job._add_processing_to_metadata(
            temp_dir=Path("some_dir"), process_start_time=datetime(2023, 4, 8)
        )
        self.assertEqual(
            MetadataStatus.VALID, basic_job.metadata_record.metadata_status
        )
        mock_copyfile.assert_not_called()
        mock_json_write.assert_called_once_with(path=Path("some_dir"))
        mock_write_standard_file.assert_called_once_with(Path("some_dir"))
        # Test that "invalid metadata remains invalid if valid processing
        basic_job = BasicJob(job_configs=basic_job_configs)
        basic_job_configs.modalities[0].compress_raw_data = True
        basic_job.metadata_record = Metadata(
            name="some_name", location="some_bucket"
        )
        # Test that "valid metadata remains valid if valid processing
        basic_job.metadata_record.metadata_status = MetadataStatus.INVALID
        basic_job._add_processing_to_metadata(
            temp_dir=Path("some_dir"), process_start_time=datetime(2023, 4, 8)
        )
        self.assertEqual(
            MetadataStatus.INVALID, basic_job.metadata_record.metadata_status
        )

    @patch.dict(os.environ, EXAMPLE_ENV_VAR1, clear=True)
    @patch("aind_data_transfer.jobs.basic_job.upload_to_s3")
    def test_upload_to_s3(
        self,
        mock_upload: MagicMock,
    ):
        """Tests that the data is uploaded to S3"""
        basic_job_configs = BasicUploadJobConfigs()
        basic_job = BasicJob(job_configs=basic_job_configs)
        basic_job._upload_to_s3(temp_dir=Path("some_dir"))
        mock_upload.assert_called_once_with(
            directory_to_upload=Path("some_dir"),
            s3_bucket="some_bucket",
            s3_prefix="confocal_643054_2020-10-10_10-10-10",
            dryrun=True,
        )

    @patch.dict(os.environ, EXAMPLE_ENV_VAR1, clear=True)
    @patch("aind_codeocean_api.codeocean.CodeOceanClient.run_capsule")
    def test_trigger_codeocean_capsule(
        self,
        mock_run_capsule: MagicMock,
    ):
        """Tests code ocean capsule is triggered"""
        successful_response = Response()
        successful_response.status_code = 200
        successful_response._content = json.dumps(
            {"Message": "triggered a code ocean capsule"}
        ).encode("utf-8")
        mock_run_capsule.return_value = successful_response
        # With dry-run set to True
        basic_job_configs = BasicUploadJobConfigs()
        basic_job = BasicJob(job_configs=basic_job_configs)
        basic_job._trigger_codeocean_pipeline()

        # With dry-run to False
        basic_job.job_configs.dry_run = False
        basic_job = BasicJob(job_configs=basic_job_configs)
        basic_job._trigger_codeocean_pipeline()

        expected_run_capsule_request = RunCapsuleRequest(
            capsule_id="some_capsule_id",
            pipeline_id=None,
            version=None,
            resume_run_id=None,
            data_assets=None,
            parameters=[
                '{"trigger_codeocean_job": '
                '{"job_type": "confocal", '
                '"modalities": ["MRI"], '
                '"capsule_id": "some_capsule_id", '
                '"process_capsule_id": null, '
                '"bucket": "some_bucket", '
                '"prefix": "confocal_643054_2020-10-10_10-10-10", '
                f'"aind_data_transfer_version": "{__version__}"'
                "}}"
            ],
            named_parameters=None,
            processes=None,
        )

        mock_run_capsule.assert_called_once_with(
            request=expected_run_capsule_request
        )

    @patch.dict(os.environ, EXAMPLE_ENV_VAR1, clear=True)
    @patch("aind_codeocean_api.codeocean.CodeOceanClient.run_capsule")
    def test_trigger_custom_codeocean_capsule(
        self,
        mock_run_capsule: MagicMock,
    ):
        """Tests code ocean capsule is triggered"""
        successful_response = Response()
        successful_response.status_code = 200
        successful_response._content = json.dumps(
            {"Message": "triggered a code ocean capsule"}
        ).encode("utf-8")
        mock_run_capsule.return_value = successful_response
        # With dry-run set to True
        basic_job_configs = BasicUploadJobConfigs()
        basic_job_configs.dry_run = False
        basic_job_configs.codeocean_process_capsule_id = "xyz-456"
        basic_job = BasicJob(job_configs=basic_job_configs)
        basic_job._trigger_codeocean_pipeline()

        expected_run_capsule_request = RunCapsuleRequest(
            capsule_id="some_capsule_id",
            pipeline_id=None,
            version=None,
            resume_run_id=None,
            data_assets=None,
            parameters=[
                '{"trigger_codeocean_job": '
                '{"job_type": "run_generic_pipeline", '
                '"modalities": ["MRI"], '
                '"capsule_id": "some_capsule_id", '
                '"process_capsule_id": "xyz-456", '
                '"bucket": "some_bucket", '
                '"prefix": "confocal_643054_2020-10-10_10-10-10", '
                f'"aind_data_transfer_version": "{__version__}"'
                "}}"
            ],
            named_parameters=None,
            processes=None,
        )

        mock_run_capsule.assert_called_once_with(
            request=expected_run_capsule_request
        )

    @patch.dict(os.environ, EXAMPLE_ENV_VAR1, clear=True)
    @patch("tempfile.TemporaryDirectory")
    @patch(
        "aind_data_transfer.jobs.basic_job.BasicJob."
        "_check_if_s3_location_exists"
    )
    @patch(
        "aind_data_transfer.jobs.basic_job.BasicJob"
        "._initialize_metadata_record"
    )
    @patch("aind_data_transfer.jobs.basic_job.BasicJob._compress_raw_data")
    @patch("aind_data_transfer.jobs.basic_job.BasicJob._encrypt_behavior_dir")
    @patch(
        "aind_data_transfer.jobs.basic_job.BasicJob"
        "._add_processing_to_metadata"
    )
    @patch("aind_data_transfer.jobs.basic_job.BasicJob._upload_to_s3")
    @patch(
        "aind_data_transfer.jobs.basic_job.BasicJob."
        "_trigger_codeocean_pipeline"
    )
    @patch("aind_data_transfer.jobs.basic_job.datetime")
    @patch("aind_data_transfer.jobs.basic_job.BasicJob._test_upload")
    def test_run_job(
        self,
        mock_test_upload: MagicMock,
        mock_datetime: MagicMock,
        mock_trigger_pipeline: MagicMock,
        mock_upload_to_s3: MagicMock,
        mock_add_processing_to_metadata: MagicMock,
        mock_encrypt_behavior: MagicMock,
        mock_compress_raw_data: MagicMock,
        mock_initialize_metadata: MagicMock,
        mock_s3_check: MagicMock,
        mock_tempfile: MagicMock,
    ):
        """Tests that the run_job executes all sub jobs"""

        mock_datetime.now.return_value = datetime(2023, 4, 9)

        mock_tempfile.return_value.__enter__.return_value = (
            Path("some_dir") / "tmp"
        )
        basic_job_configs = BasicUploadJobConfigs()
        # Set the temp directory
        basic_job_configs.temp_directory = "some_dir"
        basic_job = BasicJob(job_configs=basic_job_configs)
        basic_job.run_job()

        mock_test_upload.assert_called_once_with(
            temp_dir=(Path("some_dir") / "tmp")
        )
        mock_tempfile.assert_called_once_with(dir="some_dir")
        mock_s3_check.assert_called_once()
        mock_initialize_metadata.assert_called_once_with(
            temp_dir=(Path("some_dir") / "tmp")
        )
        mock_compress_raw_data.assert_called_once_with(
            temp_dir=(Path("some_dir") / "tmp")
        )
        mock_encrypt_behavior.assert_called_once_with(
            temp_dir=(Path("some_dir") / "tmp")
        )
        mock_add_processing_to_metadata.assert_called_once_with(
            temp_dir=(Path("some_dir") / "tmp"),
            process_start_time=datetime(2023, 4, 9),
        )
        mock_upload_to_s3.assert_called_once_with(
            temp_dir=(Path("some_dir") / "tmp")
        )
        mock_trigger_pipeline.assert_called_once()

        self.assertEqual(1, 1)


if __name__ == "__main__":
    unittest.main()
