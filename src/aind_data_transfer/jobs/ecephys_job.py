import logging
import tempfile

from aind_data_transfer.config_loader.ephys_config_loader import (
    EphysConfigLoader,
)
from aind_data_transfer.jobs.s3_upload_job import GenericS3UploadJob
from aind_data_transfer.readers.ephys_readers import EphysReaders
from aind_data_transfer.transformations.ephys_compressors import (
    EphysCompressors,
)
from aind_data_transfer.util.s3_utils import upload_to_s3
from aind_data_transfer.writers.ephys_writers import EphysWriters


class EcephysJob(GenericS3UploadJob):
    def __init__(self, args: list) -> None:
        """Initializes class with sys args. Convert the sys args to configs."""
        super().__init__(args=args)
        self.args = args
        self.configs = EphysConfigLoader(args).configs

    def upload_raw_data_folder(self, data_prefix, behavior_dir) -> None:
        if not self.configs.compress_data:
            # Upload non-behavior data to s3
            upload_to_s3(
                directory_to_upload=self.configs.data_source,
                s3_bucket=self.configs.s3_bucket,
                s3_prefix=data_prefix,
                dryrun=self.configs.dry_run,
                excluded=behavior_dir,
            )
        else:
            logging.info("Compressing raw data folder: ")
            clip_kwargs = self.configs.compression_configs.configs[
                "clip_data"
            ]["clip_kwargs"]
            streams_to_clip = EphysReaders.get_streams_to_clip(
                self.configs.compression_configs.data_name,
                self.configs.data_source,
            )
            with tempfile.TemporaryDirectory() as td:
                # Clip data
                clipped_data_path = td / "ecephys_clipped"
                EphysWriters.copy_and_clip_data(
                    src_dir=self.configs.data_source,
                    dst_dir=clipped_data_path,
                    stream_gen=streams_to_clip,
                    behavior_dir=None,
                    video_encryption_key=None,
                    **clip_kwargs,
                )

                # Compress data
                compressed_data_path = td / "ecephys_compressed"
                compressor_name = self.configs.compression_configs.configs[
                    "compress_data"
                ]["compressor"]["compressor_name"]
                compressor_kwargs = self.configs.compression_configs.configs[
                    "compress_data"
                ]["compressor"]["kwargs"]
                format_kwargs = self.configs.compression_configs.configs[
                    "compress_data"
                ]["format_kwargs"]
                scale_kwargs = self.configs.compression_configs.configs[
                    "compress_data"
                ]["scale_params"]
                write_kwargs = self.configs.compression_configs.configs[
                    "compress_data"
                ]["write_kwargs"]
                max_filename_length = self.configs.compression_configs.configs[
                    "compress_data"
                ].get("max_windows_filename_len")
                read_blocks = EphysReaders.get_read_blocks(
                    self.configs.compression_configs.data_name,
                    self.configs.data_source,
                )
                compressor = EphysCompressors.get_compressor(
                    compressor_name, **compressor_kwargs
                )
                scaled_read_blocks = EphysCompressors.scale_read_blocks(
                    read_blocks, **scale_kwargs
                )
                EphysWriters.compress_and_write_block(
                    read_blocks=scaled_read_blocks,
                    compressor=compressor,
                    output_dir=compressed_data_path,
                    max_windows_filename_len=max_filename_length,
                    job_kwargs=write_kwargs,
                    **format_kwargs,
                )

                upload_to_s3(
                    directory_to_upload=clipped_data_path,
                    s3_bucket=self.configs.s3_bucket,
                    s3_prefix=data_prefix + "/ecephys_clipped",
                    dryrun=self.configs.dry_run,
                    excluded=behavior_dir,
                )

                upload_to_s3(
                    directory_to_upload=compressed_data_path,
                    s3_bucket=self.configs.s3_bucket,
                    s3_prefix=data_prefix + "/ecephys_compressed",
                    dryrun=self.configs.dry_run,
                    excluded=behavior_dir,
                )

        return None
