"""Job that reads open ephys data, compresses, and writes it."""
import argparse
import subprocess
from pathlib import Path

from transfer.compressors import EphysCompressors
from transfer.configuration_loader import EphysJobConfigurationLoader
from transfer.readers import EphysReaders
from transfer.writers import EphysWriters

if __name__ == "__main__":
    # Location of conf file passed in as command line arg
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--conf-file-location", required=True, type=str)
    args = parser.parse_args()
    job_configs = EphysJobConfigurationLoader().load_configs(
        args.conf_file_location
    )

    # Extract raw data name, (e.g., openephys) and raw data path
    data_name = job_configs["data"]["name"]
    data_src_dir = Path(job_configs["data"]["source_dir"])
    data_dest_dir = Path(job_configs["data"]["dest_dir"])

    # Clip data job
    if job_configs["clip_data_job"]["clip"]:
        clipped_data_path = data_dest_dir / "ecephys_clipped"
        clip_kwargs = job_configs["clip_data_job"]["clip_kwargs"]
        streams_to_clip = EphysReaders.get_streams_to_clip(
            data_name, data_src_dir
        )
        EphysWriters.copy_and_clip_data(
            data_src_dir, clipped_data_path, streams_to_clip, **clip_kwargs
        )

    # Compress data job
    if job_configs["compress_data_job"]["compress"]:
        compressed_data_path = data_dest_dir / "ecephys_compressed"
        compressor_name = job_configs["compress_data_job"]["compressor"][
            "compressor_name"
        ]
        compressor_kwargs = job_configs["compress_data_job"]["compressor"][
            "kwargs"
        ]
        format_kwargs = job_configs["compress_data_job"]["format_kwargs"]
        scale_kwargs = job_configs["compress_data_job"]["scale_params"]
        write_kwargs = job_configs["compress_data_job"]["write_kwargs"]
        read_blocks = EphysReaders.get_read_blocks(data_name, data_src_dir)
        compressor = EphysCompressors.get_compressor(
            compressor_name, compressor_kwargs
        )
        scaled_read_blocks = EphysCompressors.scale_read_blocks(
            read_blocks, **scale_kwargs
        )
        EphysWriters.compress_and_write_block(
            read_blocks=scaled_read_blocks,
            compressor=compressor,
            output_dir=compressed_data_path,
            job_kwargs=write_kwargs,
            **format_kwargs
        )

    # Upload to s3
    if job_configs["upload_data_job"]["upload_to_s3"]:
        aws_dest = job_configs["upload_data_job"]["s3_dest"]
        if job_configs["upload_data_job"]["dryrun"]:
            subprocess.run(
                [
                    "aws",
                    "s3",
                    "sync",
                    data_dest_dir,
                    aws_dest,
                    "--dryrun",
                ]
            )
        else:
            subprocess.run(["aws", "s3", "sync", data_dest_dir, aws_dest])

    # Upload to gcp
    if job_configs["upload_data_job"]["upload_to_gcp"]:
        gcp_dest = job_configs["upload_data_job"]["gcp_dest"]
        if job_configs["upload_data_job"]["dryrun"]:
            subprocess.run(
                [
                    "gsutil",
                    "-m",
                    "rsync",
                    "-n",
                    data_dest_dir,
                    gcp_dest,
                ]
            )
        else:
            subprocess.run(["gsutil", "-m", "rsync", data_dest_dir, gcp_dest])
