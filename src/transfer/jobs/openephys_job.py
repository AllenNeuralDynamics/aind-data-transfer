"""Job that reads open ephys data, compresses, and writes it."""
import subprocess
import sys
from pathlib import Path

from botocore.session import get_session

from transfer.codeocean import CodeOceanDataAssetRequests
from transfer.compressors import EphysCompressors
from transfer.configuration_loader import EphysJobConfigurationLoader
from transfer.readers import EphysReaders
from transfer.writers import EphysWriters

if __name__ == "__main__":
    # Location of conf file passed in as command line arg
    job_configs = EphysJobConfigurationLoader().load_configs(sys.argv[1:])

    # Extract raw data name, (e.g., openephys) and raw data path
    data_name = job_configs["data"]["name"]
    data_src_dir = Path(job_configs["endpoints"]["raw_data_dir"])
    dest_data_dir = Path(job_configs["endpoints"]["dest_data_dir"])

    # Clip data job
    if job_configs["jobs"]["clip"]:
        clipped_data_path = dest_data_dir / "ecephys_clipped"
        clip_kwargs = job_configs["clip_data_job"]["clip_kwargs"]
        streams_to_clip = EphysReaders.get_streams_to_clip(
            data_name, data_src_dir
        )
        EphysWriters.copy_and_clip_data(
            data_src_dir, clipped_data_path, streams_to_clip, **clip_kwargs
        )

    # Compress data job
    if job_configs["jobs"]["compress"]:
        compressed_data_path = dest_data_dir / "ecephys_compressed"
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
            **format_kwargs,
        )

    # Upload to s3
    if job_configs["jobs"]["upload_to_s3"]:
        # TODO: Use s3transfer library instead of subprocess?
        s3_bucket = job_configs["endpoints"]["s3_bucket"]
        s3_prefix = job_configs["endpoints"]["s3_prefix"]
        aws_dest = f"s3://{s3_bucket}/{s3_prefix}"
        if job_configs["upload_data_job"]["dryrun"]:
            subprocess.run(
                [
                    "aws",
                    "s3",
                    "sync",
                    dest_data_dir,
                    aws_dest,
                    "--dryrun",
                ]
            )
        else:
            subprocess.run(["aws", "s3", "sync", dest_data_dir, aws_dest])

    # Upload to gcp
    if job_configs["jobs"]["upload_to_gcp"]:
        gcp_bucket = job_configs["endpoints"]["gcp_bucket"]
        gcp_prefix = job_configs["endpoints"]["gcp_prefix"]
        gcp_dest = f"gs://{gcp_bucket}/{gcp_prefix}"
        if job_configs["upload_data_job"]["dryrun"]:
            subprocess.run(
                [
                    "gsutil",
                    "-m",
                    "rsync",
                    "-n",
                    dest_data_dir,
                    gcp_dest,
                ]
            )
        else:
            subprocess.run(
                ["gsutil", "-m", "rsync", dest_data_dir, gcp_dest]
            )

    # Register Asset on CodeOcean
    if job_configs["jobs"]["register_to_codeocean"]:
        # Use botocore to retrieve aws access tokens
        aws_session = get_session()
        aws_credentials = aws_session.get_credentials()
        aws_key = aws_credentials.access_key
        aws_secret = aws_credentials.secret_key

        co_api_token = job_configs["register_on_codeocean_job"]["api_token"]
        co_domain = job_configs["endpoints"]["codeocean_domain"]
        co_client = CodeOceanDataAssetRequests(
            domain=co_domain, token=co_api_token
        )

        co_tags = job_configs["register_on_codeocean_job"]["tags"]

        asset_name = job_configs["register_on_codeocean_job"]["asset_name"]
        mount = job_configs["register_on_codeocean_job"]["mount"]

        bucket = job_configs["endpoints"]["s3_bucket"]
        prefix = job_configs["endpoints"]["s3_prefix"]

        json_data = CodeOceanDataAssetRequests.create_post_json_data(
            asset_name=asset_name,
            mount=mount,
            bucket=bucket,
            prefix=prefix,
            access_key_id=aws_key,
            secret_access_key=aws_secret,
            tags=co_tags,
        )

        co_client.register_data_asset(json_data=json_data)