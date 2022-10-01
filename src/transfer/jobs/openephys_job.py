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
    data_name = job_configs["raw_data"]["name"]
    data_src_dir = Path(job_configs["raw_data"]["source_dir"])

    # Clip data job
    if job_configs["clip_data_job"]["clip"]:
        clipped_data_path = Path(
            job_configs["clip_data_job"]["clipped_data_dest"]
        )
        n_frames = job_configs["clip_data_job"]["n_frames"]
        streams_to_clip = EphysReaders.get_streams_to_clip(
            data_name, data_src_dir
        )
        EphysWriters.copy_and_clip_data(
            data_src_dir, clipped_data_path, streams_to_clip, n_frames
        )

    # Compress data job
    if job_configs["compress_data_job"]["compress"]:
        compressed_data_path = Path(
            job_configs["compress_data_job"]["compressed_data_dest"]
        )
        compressor_name = job_configs["compress_data_job"]["compressor"][
            "compressor_name"
        ]
        compressor_kwargs = job_configs["compress_data_job"]["compressor"][
            "kwargs"
        ]
        output_format = job_configs["compress_data_job"]["output_format"]
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
            output_format=output_format,
            job_kwargs=write_kwargs,
        )

    # Upload to s3
    if job_configs["upload_data_job"]["upload_to_s3"]:
        aws_compressed_dest = (
            job_configs["upload_data_job"]["s3_dest"] + "/ecephys_compressed"
        )
        aws_clipped_dest = (
            job_configs["upload_data_job"]["s3_dest"] + "/ecephys_clipped"
        )
        clipped_data_path = job_configs["clip_data_job"]["clipped_data_dest"]
        compressed_data_path = job_configs["compress_data_job"][
            "compressed_data_dest"
        ]
        if job_configs["upload_data_job"]["dryrun"]:
            subprocess.run(
                [
                    "aws",
                    "s3",
                    "sync",
                    clipped_data_path,
                    aws_clipped_dest,
                    "--dryrun",
                ]
            )
            subprocess.run(
                [
                    "aws",
                    "s3",
                    "sync",
                    compressed_data_path,
                    aws_compressed_dest,
                    "--dryrun",
                ]
            )
        else:
            subprocess.run(
                ["aws", "s3", "sync", clipped_data_path, aws_clipped_dest]
            )
            subprocess.run(
                [
                    "aws",
                    "s3",
                    "sync",
                    compressed_data_path,
                    aws_compressed_dest,
                ]
            )

    # Upload to gcp
    if job_configs["upload_data_job"]["upload_to_gcp"]:
        gcp_compressed_dest = (
            job_configs["upload_data_job"]["gcp_dest"] + "/ecephys_compressed"
        )
        gcp_clipped_dest = (
            job_configs["upload_data_job"]["gcp_dest"] + "/ecephys_clipped"
        )
        clipped_data_path = job_configs["clip_data_job"]["clipped_data_dest"]
        compressed_data_path = job_configs["compress_data_job"][
            "compressed_data_dest"
        ]
        if job_configs["upload_data_job"]["dryrun"]:
            subprocess.run(
                [
                    "gsutil",
                    "-m",
                    "rsync",
                    "-n",
                    clipped_data_path,
                    gcp_clipped_dest,
                ]
            )
            subprocess.run(
                [
                    "gsutil",
                    "-m",
                    "rsync",
                    "-n",
                    compressed_data_path,
                    gcp_compressed_dest,
                ]
            )
        else:
            subprocess.run(
                ["gsutil", "-m", "rsync", clipped_data_path, gcp_clipped_dest]
            )
            subprocess.run(
                [
                    "gsutil",
                    "-m",
                    "rsync",
                    compressed_data_path,
                    gcp_compressed_dest,
                ]
            )
