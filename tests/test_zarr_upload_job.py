import os
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

from aind_data_schema.models.platforms import Platform
from aind_data_schema.models.modalities import Modality
from numcodecs import blosc

from aind_data_transfer.config_loader.base_config import (
    BasicUploadJobConfigs,
    ModalityConfigs,
)
from aind_data_transfer.jobs.zarr_upload_job import (
    ZarrConversionConfigs,
    ZarrUploadJob,
)

TEST_RESOURCE_DIR = (
    Path(os.path.dirname(os.path.realpath(__file__))) / "resources"
)
DISPIM_DATA_DIR = (
    TEST_RESOURCE_DIR / "imaging/diSPIM_12345_2023-03-16_00-00-00"
)
DISPIM_RAW_IMAGE_DIR = DISPIM_DATA_DIR / "diSPIM"
DISPIM_DERIVATIVES_DIR = DISPIM_DATA_DIR / "derivatives"
EXASPIM_DATA_DIR = (
    TEST_RESOURCE_DIR / "imaging/exaSPIM_125L_2022-08-05_17-25-36"
)
EXASPIM_RAW_IMAGE_DIR = EXASPIM_DATA_DIR / "exaSPIM"
EXASPIM_DERIVATIVES_DIR = EXASPIM_DATA_DIR / "derivatives"


class TestZarrConversionConfigs(unittest.TestCase):
    def test_default_values(self):
        config = ZarrConversionConfigs()
        self.assertEqual(config.n_levels, 1)
        self.assertEqual(config.scale_factor, 2)
        self.assertEqual(config.chunk_shape, [1, 1, 256, 256, 256])
        self.assertIsNone(config.voxel_size)
        self.assertEqual(config.codec, "zstd")
        self.assertEqual(config.clevel, 1)
        self.assertFalse(config.do_bkg_subtraction)
        self.assertIsNone(config.exclude_patterns)

    def test_from_yaml(self):
        # Sample YAML content
        sample_yaml_content = """
        n_levels: 2
        scale_factor: 3
        chunk_shape: [2, 2, 128, 128, 128]
        voxel_size: [1, 1, 1]
        codec: "blosc"
        clevel: 2
        do_bkg_subtraction: True
        exclude_patterns: ["pattern1", "pattern2"]
        """

        m = mock_open(read_data=sample_yaml_content)
        with patch("builtins.open", m):
            config = ZarrConversionConfigs.from_yaml(Path("fake_path.yaml"))
        self.assertEqual(config.n_levels, 2)
        self.assertEqual(config.scale_factor, 3)
        self.assertEqual(config.chunk_shape, [2, 2, 128, 128, 128])
        self.assertEqual(config.voxel_size, [1, 1, 1])
        self.assertEqual(config.codec, "blosc")
        self.assertEqual(config.clevel, 2)
        self.assertTrue(config.do_bkg_subtraction)
        self.assertEqual(config.exclude_patterns, ["pattern1", "pattern2"])


class TestZarrUploadJob(unittest.TestCase):
    EXAMPLE_ENV_VAR1 = {
        "CODEOCEAN_DOMAIN": "some_domain",
        "CODEOCEAN_TRIGGER_CAPSULE_ID": "some_capsule_id",
        "METADATA_SERVICE_DOMAIN": "some_ms_domain",
        "AIND_DATA_TRANSFER_REPO_LOCATION": "some_dtr_location",
        "VIDEO_ENCRYPTION_PASSWORD": "some_password",
        "CODEOCEAN_API_TOKEN": "some_api_token",
        "S3_BUCKET": "some_bucket",
        "SUBJECT_ID": "12345",
        "ACQ_DATETIME": "2020-10-10 10:10:10",
        "DATA_SOURCE": str(DISPIM_DATA_DIR),
        "DRY_RUN": "false",
    }

    @patch.dict(os.environ, EXAMPLE_ENV_VAR1, clear=True)
    def _get_test_configs(
        self, platform: Platform, extra: Path = None
    ) -> BasicUploadJobConfigs:
        if platform == Platform.HCR:
            data_dir = DISPIM_DATA_DIR
        elif platform == Platform.EXASPIM:
            data_dir = EXASPIM_DATA_DIR
        else:
            # TODO: unit tests shouldn't raise errors like this. This can be
            #  a assertion somewhere in the codebase and that verified here.
            raise ValueError(f"Unsupported modality: {platform}")
        config = BasicUploadJobConfigs(
            platform=platform,
            modalities=[
                ModalityConfigs(
                    modality=Modality.SPIM,
                    source=data_dir,
                    extra_configs=extra,
                )
            ],
            log_level="WARNING",
        )
        config.temp_directory = "some_dir"
        return config

    def test_init_dispim(self) -> None:
        test_job_configs = self._get_test_configs(Platform.HCR)
        job = ZarrUploadJob(job_configs=test_job_configs)
        self.assertEqual(job.job_configs.platform, Platform.HCR)
        self.assertEqual(job._data_src_dir, DISPIM_DATA_DIR)
        self.assertEqual(job._raw_image_dir, DISPIM_RAW_IMAGE_DIR)
        self.assertEqual(job._derivatives_dir, DISPIM_DERIVATIVES_DIR)
        self.assertEqual(
            job._zarr_path,
            "s3://some_bucket/HCR_12345_2020-10-10_10-10-10/SPIM.ome.zarr",
        )
        self.assertEqual(job._zarr_configs, ZarrConversionConfigs())

    def test_init_exaspim(
        self,
    ) -> None:
        test_job_configs = self._get_test_configs(Platform.EXASPIM)
        job = ZarrUploadJob(job_configs=test_job_configs)

        self.assertEqual(job.job_configs.platform, Platform.EXASPIM)
        self.assertEqual(job._data_src_dir, EXASPIM_DATA_DIR)
        self.assertEqual(job._raw_image_dir, EXASPIM_RAW_IMAGE_DIR)
        self.assertEqual(job._derivatives_dir, EXASPIM_DERIVATIVES_DIR)
        self.assertEqual(
            job._zarr_path,
            "s3://some_bucket/exaSPIM_12345_2020-10-10_10-10-10/SPIM.ome.zarr",
        )
        self.assertEqual(job._zarr_configs, ZarrConversionConfigs())

    @patch(
        "aind_data_transfer.jobs.zarr_upload_job.ZarrUploadJob._create_neuroglancer_link"
    )
    @patch("aind_data_transfer.jobs.zarr_upload_job.datetime")
    @patch("tempfile.TemporaryDirectory")
    @patch(
        "aind_data_transfer.jobs.zarr_upload_job.ZarrUploadJob._upload_zarr"
    )
    @patch(
        "aind_data_transfer.jobs.zarr_upload_job.ZarrUploadJob._upload_to_s3"
    )
    @patch(
        "aind_data_transfer.jobs.zarr_upload_job.ZarrUploadJob._create_dispim_metadata"
    )
    @patch("aind_data_transfer.jobs.basic_job.BasicJob._initialize_metadata_record")
    @patch("aind_data_transfer.jobs.basic_job.BasicJob._add_processing_to_metadata")
    @patch("aind_data_transfer.jobs.basic_job.BasicJob._test_upload")
    @patch(
        "aind_data_transfer.jobs.basic_job.BasicJob._check_if_s3_location_exists"
    )
    def test_run_job_dispim(
        self,
        mock_s3_check: MagicMock,
        mock_test_upload: MagicMock,
        mock_add_processing_to_metadata: MagicMock,
        mock_initialize_metadata: MagicMock,
        mock_create_dispim_metadata: MagicMock,
        mock_upload_to_s3: MagicMock,
        mock_upload_zarr: MagicMock,
        mock_tempfile: MagicMock,
        mock_datetime: MagicMock,
        mock_create_neuroglancer_link: MagicMock,
    ) -> None:
        mock_datetime.now.return_value = datetime(2023, 4, 9)

        mock_tempfile.return_value.__enter__.return_value = (
            Path("some_dir") / "tmp"
        )

        test_job_configs = self._get_test_configs(Platform.HCR)
        job = ZarrUploadJob(job_configs=test_job_configs)
        job.run_job()

        mock_tempfile.assert_called_once_with(dir="some_dir")

        mock_s3_check.assert_called_once()
        mock_test_upload.assert_called_once_with(
            temp_dir=(Path("some_dir") / "tmp")
        )
        mock_initialize_metadata.assert_called_once_with(
            temp_dir=test_job_configs.modalities[0].source
        )
        mock_add_processing_to_metadata.assert_called_once_with(
            temp_dir=test_job_configs.modalities[0].source,
            process_start_time=datetime(2023, 4, 9),
        )
        mock_create_dispim_metadata.assert_called_once()
        mock_upload_to_s3.assert_called_once_with(
            dir=DISPIM_DATA_DIR,
            excluded=os.path.join(DISPIM_DATA_DIR, "diSPIM", "*"),
        )

        mock_upload_zarr.assert_called_once()
        mock_create_neuroglancer_link.assert_not_called()

    @patch(
        "aind_data_transfer.jobs.zarr_upload_job.ZarrUploadJob._create_neuroglancer_link"
    )
    @patch("aind_data_transfer.jobs.zarr_upload_job.datetime")
    @patch("tempfile.TemporaryDirectory")
    @patch(
        "aind_data_transfer.jobs.zarr_upload_job.ZarrUploadJob._upload_zarr"
    )
    @patch(
        "aind_data_transfer.jobs.zarr_upload_job.ZarrUploadJob._upload_to_s3"
    )
    @patch(
        "aind_data_transfer.jobs.zarr_upload_job.ZarrUploadJob._create_dispim_metadata"
    )
    @patch("aind_data_transfer.jobs.basic_job.BasicJob._initialize_metadata_record")
    @patch("aind_data_transfer.jobs.basic_job.BasicJob._add_processing_to_metadata")
    @patch("aind_data_transfer.jobs.basic_job.BasicJob._test_upload")
    @patch(
        "aind_data_transfer.jobs.basic_job.BasicJob._check_if_s3_location_exists"
    )
    def test_run_job_exaspim(
        self,
        mock_s3_check: MagicMock,
        mock_test_upload: MagicMock,
        mock_add_processing_to_metadata: MagicMock,
        mock_initialize_metadata: MagicMock,
        mock_create_dispim_metadata: MagicMock,
        mock_upload_to_s3: MagicMock,
        mock_upload_zarr: MagicMock,
        mock_tempfile: MagicMock,
        mock_datetime: MagicMock,
        mock_create_neuroglancer_link: MagicMock,
    ) -> None:
        mock_datetime.now.return_value = datetime(2023, 4, 9)

        mock_tempfile.return_value.__enter__.return_value = (
            Path("some_dir") / "tmp"
        )

        test_job_configs = self._get_test_configs(
            Platform.EXASPIM, extra=EXASPIM_DATA_DIR / "zarr_configs.yaml"
        )
        job = ZarrUploadJob(job_configs=test_job_configs)
        job.run_job()

        mock_tempfile.assert_called_once_with(dir="some_dir")

        mock_s3_check.assert_called_once()
        mock_test_upload.assert_called_once_with(
            temp_dir=(Path("some_dir") / "tmp")
        )
        mock_initialize_metadata.assert_called_once_with(
            temp_dir=test_job_configs.modalities[0].source
        )
        mock_add_processing_to_metadata.assert_called_once_with(
            temp_dir=test_job_configs.modalities[0].source,
            process_start_time=datetime(2023, 4, 9),
        )
        mock_create_dispim_metadata.assert_not_called()
        mock_upload_to_s3.assert_called_once_with(
            dir=EXASPIM_DATA_DIR,
            excluded=os.path.join(EXASPIM_DATA_DIR, "exaSPIM", "*"),
        )

        mock_upload_zarr.assert_called_once()

        mock_create_neuroglancer_link.assert_called_once()

    @patch("aind_data_transfer.jobs.zarr_upload_job.read_toml")
    @patch("aind_data_transfer.jobs.zarr_upload_job.read_imaging_log")
    @patch("aind_data_transfer.jobs.zarr_upload_job.log_to_acq_json")
    @patch("aind_data_transfer.jobs.zarr_upload_job.write_acq_json")
    @patch("aind_data_transfer.jobs.zarr_upload_job.acq_json_to_xml")
    @patch("aind_data_transfer.jobs.zarr_upload_job.write_xml")
    def test_create_dispim_metadata(
        self,
        mock_write_xml: MagicMock,
        mock_acq_json_to_xml: MagicMock,
        mock_write_acq_json: MagicMock,
        mock_log_to_acq_json: MagicMock,
        mock_read_imaging_log: MagicMock,
        mock_read_toml: MagicMock,
    ) -> None:
        test_job_configs = self._get_test_configs(Platform.HCR)
        job = ZarrUploadJob(job_configs=test_job_configs)
        job._create_dispim_metadata()

        mock_read_toml.assert_called_once_with(
            DISPIM_DATA_DIR.joinpath("config.toml")
        )
        mock_read_imaging_log.assert_called_once_with(
            DISPIM_DATA_DIR.joinpath("imaging_log.log")
        )
        mock_log_to_acq_json.assert_called_once()
        mock_write_acq_json.assert_called_once()
        mock_acq_json_to_xml.assert_called_once()
        mock_write_xml.assert_called_once()

    @patch("aind_data_transfer.jobs.zarr_upload_job.upload_to_s3")
    def test_upload_to_s3(
        self,
        mock_upload: MagicMock,
    ) -> None:
        """Tests that the data is uploaded to S3"""
        test_job_configs = self._get_test_configs(Platform.HCR)
        job = ZarrUploadJob(job_configs=test_job_configs)
        job._upload_to_s3(dir=Path("some_dir"), excluded=Path("exclude_dir"))
        mock_upload.assert_called_once_with(
            directory_to_upload=Path("some_dir"),
            s3_bucket="some_bucket",
            s3_prefix="HCR_12345_2020-10-10_10-10-10",
            excluded=Path("exclude_dir"),
            dryrun=False,
        )

    @patch("aind_data_transfer.jobs.zarr_upload_job.write_files")
    @patch(
        "aind_data_transfer.jobs.zarr_upload_job.get_images",
        return_value=["/path/to/image1", "/path/to/image2"],
    )
    def test_upload_zarr(
        self,
        mock_get_images: MagicMock,
        mock_write_files: MagicMock,
    ) -> None:
        test_job_configs = self._get_test_configs(Platform.HCR)
        job = ZarrUploadJob(job_configs=test_job_configs)
        job._upload_zarr()
        mock_get_images.assert_called_once()
        mock_write_files.assert_called_once_with(
            {"/path/to/image1", "/path/to/image2"},
            "s3://some_bucket/HCR_12345_2020-10-10_10-10-10/SPIM.ome.zarr",
            1,
            2,
            True,
            None,
            [1, 1, 256, 256, 256],
            None,
            compressor=blosc.Blosc("zstd", 1, shuffle=blosc.SHUFFLE),
            bkg_img_dir=None,
        )

    @patch("aind_data_transfer.jobs.zarr_upload_job.write_files")
    @patch(
        "aind_data_transfer.jobs.zarr_upload_job.get_images",
        return_value=["/path/to/image1", "/path/to/image2"],
    )
    def test_upload_zarr_with_bkg_subtraction(
        self, mock_get_images: MagicMock, mock_write_files: MagicMock
    ) -> None:
        test_job_configs = self._get_test_configs(Platform.HCR)
        job = ZarrUploadJob(job_configs=test_job_configs)
        job._zarr_configs.do_bkg_subtraction = True
        job._upload_zarr()
        mock_get_images.assert_called_once()
        mock_write_files.assert_called_once_with(
            {"/path/to/image1", "/path/to/image2"},
            "s3://some_bucket/HCR_12345_2020-10-10_10-10-10/SPIM.ome.zarr",
            1,
            2,
            True,
            None,
            [1, 1, 256, 256, 256],
            None,
            compressor=blosc.Blosc("zstd", 1, shuffle=blosc.SHUFFLE),
            bkg_img_dir=str(DISPIM_DERIVATIVES_DIR),
        )

    @patch("aind_data_transfer.jobs.zarr_upload_job.write_files")
    @patch(
        "aind_data_transfer.jobs.zarr_upload_job.get_images",
        return_value=[],
    )
    def test_upload_zarr_no_images(
        self, mock_get_images: MagicMock, mock_write_files: MagicMock
    ) -> None:
        test_job_configs = self._get_test_configs(Platform.HCR)
        job = ZarrUploadJob(job_configs=test_job_configs)
        job._upload_zarr()
        mock_get_images.assert_called_once()
        mock_write_files.assert_not_called()

    def test_resolve_zarr_configs(self):
        config_path = EXASPIM_DATA_DIR / "zarr_configs.yaml"
        test_job_configs = self._get_test_configs(
            Platform.EXASPIM, extra=config_path
        )
        job = ZarrUploadJob(job_configs=test_job_configs)
        self.assertEqual(job._zarr_configs.n_levels, 2)
        self.assertEqual(job._zarr_configs.scale_factor, 3)
        self.assertEqual(job._zarr_configs.chunk_shape, [2, 2, 128, 128, 128])
        self.assertEqual(job._zarr_configs.voxel_size, [1, 1, 1])
        self.assertEqual(job._zarr_configs.codec, "zstd")
        self.assertEqual(job._zarr_configs.clevel, 2)
        self.assertTrue(job._zarr_configs.do_bkg_subtraction)
        self.assertEqual(
            job._zarr_configs.exclude_patterns, ["pattern1", "pattern2"]
        )
        self.assertEqual(job._zarr_configs.create_ng_link, True)
        self.assertEqual(job._zarr_configs.ng_vmin, 50)
        self.assertEqual(job._zarr_configs.ng_vmax, 5000)

        # test defaults
        test_job_configs = self._get_test_configs(Platform.EXASPIM, extra=None)
        job = ZarrUploadJob(job_configs=test_job_configs)
        self.assertEqual(job._zarr_configs, ZarrConversionConfigs())

    @patch("aind_data_transfer.jobs.zarr_upload_job.generate_exaspim_link")
    def test_create_neuroglancer_link(
        self, mock_generate_exaspim_link: MagicMock
    ):
        config_path = EXASPIM_DATA_DIR / "zarr_configs.yaml"
        test_job_configs = self._get_test_configs(
            Platform.EXASPIM, extra=config_path
        )
        job = ZarrUploadJob(job_configs=test_job_configs)
        job._create_neuroglancer_link()
        mock_generate_exaspim_link.assert_called_with(
            None,
            s3_path='s3://some_bucket/exaSPIM_12345_2020-10-10_10-10-10/SPIM.ome.zarr',
            output_json_path=str(EXASPIM_DATA_DIR),
            opacity=0.5,
            blend='default',
            vmin=50.0,
            vmax=5000.0,
            dataset_name='exaSPIM_125L_2022-08-05_17-25-36',
        )


if __name__ == "__main__":
    unittest.main()
