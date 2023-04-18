"""Module to define ecephys upload job"""

import shutil
import sys
from pathlib import Path

from numpy import memmap

from aind_data_transfer.config_loader.ecephys_config import (
    EcephysUploadJobConfigs,
)
from aind_data_transfer.jobs.basic_job import BasicJob
from aind_data_transfer.readers.ephys_readers import EphysReaders
from aind_data_transfer.transformations.ephys_compressors import (
    EphysCompressors,
)
from aind_data_transfer.util.npopto_correction import (
    correct_np_opto_electrode_locations,
)
from aind_data_transfer.util.s3_utils import upload_to_s3
from aind_data_transfer.writers.ephys_writers import EphysWriters


class EcephysJob(BasicJob):
    """Class to define methods needed to compress and upload ecephys job"""

    def __init__(self, job_configs: EcephysUploadJobConfigs):
        """Class constructor"""
        super().__init__(job_configs)
        self.job_configs = job_configs

    def _copy_and_clip_data(
        self,
        dst_dir,
        stream_gen,
    ):
        """
        Copies the raw data to a new directory with the .dat files clipped to
        just a small number of frames. This allows someone to still use the
        spikeinterface api on the clipped data set.
        Parameters
        ----------
        dst_dir : Path
          Desired location for clipped data set
        stream_gen : dict
          A dict with
            'data': memmap(dat file),
              'relative_path_name': path name of raw data
                to new dir correctly
              'n_chan': number of channels.
        Returns
        -------
        None
          Moves some directories around.

        """

        # first: copy everything except .dat files and files in behavior dir
        if self.job_configs.behavior_dir is None:
            patterns_to_ignore = ["*.dat"]
        else:
            behavior_glob = self.job_configs.behavior_dir / "*"
            patterns_to_ignore = ["*.dat", str(behavior_glob)]
        shutil.copytree(
            self.job_configs.data_source,
            dst_dir,
            ignore=shutil.ignore_patterns(*patterns_to_ignore),
        )
        # second: copy clipped dat files
        for stream in stream_gen:
            data = stream["data"]
            rel_path_name = stream["relative_path_name"]
            n_chan = stream["n_chan"]
            dst_raw_file = dst_dir / rel_path_name
            dst_data = memmap(
                dst_raw_file,
                dtype="int16",
                shape=(self.job_configs.clip_n_frames, n_chan),
                order="C",
                mode="w+",
            )
            dst_data[:] = data[: self.job_configs.clip_n_frames]

    def _compress_raw_data(self, temp_dir: Path) -> None:
        """If compress data is set to False, the data will be uploaded to s3.
        Otherwise, it will be compressed to zarr, stored in temp_dir, and
        uploaded later."""

        # Correct NP-opto electrode positions:
        # correction is skipped if Neuropix-PXI version > 0.4.0
        # It'd be nice if the original data wasn't modified.
        correct_np_opto_electrode_locations(self.job_configs.data_source)

        if self.job_configs.behavior_dir is not None:
            behavior_dir_excluded = self.job_configs.behavior_dir / "*"
        else:
            behavior_dir_excluded = None

        # If no compression is required, we'll upload the data directly instead
        # of copying it to a temp folder
        if not self.job_configs.compress_raw_data:
            # Upload non-behavior data to s3
            data_prefix = "/".join(
                [
                    self.job_configs.s3_prefix,
                    self.job_configs.experiment_type.value,
                ]
            )
            upload_to_s3(
                directory_to_upload=self.job_configs.data_source,
                s3_bucket=self.job_configs.s3_bucket,
                s3_prefix=data_prefix,
                dryrun=self.job_configs.dry_run,
                excluded=behavior_dir_excluded,
            )
        # Otherwise, we'll store the compressed folder in a temp directory
        # and upload it along with the other files
        else:
            # Clip the data
            self._instance_logger.info(
                "Clipping source data. This may take a minute."
            )
            clipped_data_path = temp_dir / "ecephys_clipped"
            streams_to_clip = EphysReaders.get_streams_to_clip(
                self.job_configs.data_reader.value,
                self.job_configs.data_source,
            )
            self._copy_and_clip_data(
                dst_dir=clipped_data_path,
                stream_gen=streams_to_clip,
            )

            self._instance_logger.info("Finished clipping source data.")

            # Compress the data
            self._instance_logger.info("Compressing source data.")
            compressed_data_path = temp_dir / "ecephys_compressed"
            read_blocks = EphysReaders.get_read_blocks(
                self.job_configs.data_reader.value,
                self.job_configs.data_source,
            )
            compressor = EphysCompressors.get_compressor(
                self.job_configs.compressor_name.value,
                **self.job_configs.compressor_kwargs,
            )
            scaled_read_blocks = EphysCompressors.scale_read_blocks(
                read_blocks=read_blocks,
                num_chunks_per_segment=(
                    self.job_configs.scale_num_chunks_per_segment
                ),
                chunk_size=self.job_configs.scale_chunk_size,
            )
            EphysWriters.compress_and_write_block(
                read_blocks=scaled_read_blocks,
                compressor=compressor,
                output_dir=compressed_data_path,
                max_windows_filename_len=(
                    self.job_configs.compress_max_windows_filename_len
                ),
                output_format=self.job_configs.compress_write_output_format,
                job_kwargs=self.job_configs.compress_job_save_kwargs,
            )
            self._instance_logger.info("Finished compressing source data.")

        return None


if __name__ == "__main__":
    sys_args = sys.argv[1:]
    job_configs_from_main = EcephysUploadJobConfigs.from_args(sys_args)
    job = EcephysJob(job_configs=job_configs_from_main)
    job.run_job()
