"""Job that reads open ephys data, compresses, and writes it."""
import logging
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
import warnings

from botocore.session import get_session

from aind_data_transfer.codeocean import CodeOceanClient
from aind_data_transfer.configuration_loader import EphysJobConfigurationLoader
from aind_data_transfer.readers import EphysReaders
from aind_data_transfer.transformations.compressors import EphysCompressors
from aind_data_transfer.transformations.metadata_creation import ProcessingMetadata
from aind_data_transfer.util.npopto_correction import correct_np_opto_electrode_locations
from aind_data_transfer.writers import EphysWriters

root_logger = logging.getLogger()

# Suppress a warning from one of the third-party libraries
deprecation_msg = ("Creating a LegacyVersion has been deprecated and will be "
                   "removed in the next major release")
warnings.filterwarnings("ignore",
                        category=DeprecationWarning,
                        message=deprecation_msg)

# TODO: Break these up into importable jobs to fix the flake8 warning?
if __name__ == "__main__":  # noqa: C901
    # Location of conf file passed in as command line arg
    job_start_time = datetime.now(timezone.utc)
    job_configs = EphysJobConfigurationLoader().load_configs(sys.argv[1:])

    root_logger.setLevel(job_configs["logging"]["level"])
    if job_configs["logging"].get("file") is not None:
        fh = logging.FileHandler(job_configs["logging"]["file"])
        fh.setLevel(job_configs["logging"]["level"])
        root_logger.addHandler(fh)

    # Extract raw data name, (e.g., openephys) and raw data path
    data_name = job_configs["data"]["name"]
    data_src_dir = Path(job_configs["endpoints"]["raw_data_dir"])
    dest_data_dir = Path(job_configs["endpoints"]["dest_data_dir"])

    # Correct NP-opto electrode positions:
    # correction is skipped if Neuropix-PXI version > 0.4.0
    correct_np_opto_electrode_locations(data_src_dir)

    logging.info("Finished loading configs.")

    # Clip data job
    if job_configs["jobs"]["clip"]:
        logging.info("Clipping source data. This may take a minute.")
        clipped_data_path = dest_data_dir / "ecephys_clipped"
        clip_kwargs = job_configs["clip_data_job"]["clip_kwargs"]
        streams_to_clip = EphysReaders.get_streams_to_clip(
            data_name, data_src_dir
        )
        EphysWriters.copy_and_clip_data(
            data_src_dir, clipped_data_path, streams_to_clip, **clip_kwargs
        )
        logging.info("Finished clipping source data.")

    # Compress data job
    if job_configs["jobs"]["compress"]:
        logging.info("Compressing source data.")
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
        logging.info("Finished compressing source data.")

    job_end_time = datetime.now(timezone.utc)
    if job_configs["jobs"]["attach_metadata"]:
        logging.info("Creating processing.json file.")
        start_date_time = job_start_time
        end_date_time = job_end_time
        input_location = data_src_dir
        if job_configs["jobs"]["upload_to_s3"]:
            s3_bucket = job_configs["endpoints"]["s3_bucket"]
            s3_prefix = job_configs["endpoints"]["s3_prefix"]
            aws_dest = f"s3://{s3_bucket}/{s3_prefix}"
            output_location = aws_dest
        elif job_configs["jobs"]["upload_to_gcp"]:
            gcp_bucket = job_configs["endpoints"]["gcp_bucket"]
            gcp_prefix = job_configs["endpoints"]["gcp_prefix"]
            gcp_dest = f"gs://{gcp_bucket}/{gcp_prefix}"
            output_location = gcp_dest
        else:
            output_location = dest_data_dir

        code_url = job_configs["endpoints"]["code_repo_location"]
        schema_url = job_configs["endpoints"]["metadata_schemas"]
        parameters = job_configs
        processing_metadata = ProcessingMetadata(schema_url=schema_url)
        processing_instance = processing_metadata.ephys_job_to_processing(
            start_date_time=start_date_time,
            end_date_time=end_date_time,
            input_location=str(input_location),
            output_location=output_location,
            code_url=code_url,
            parameters=parameters,
            notes=None,
        )

        processing_metadata.write_metadata(
            schema_instance=processing_instance, output_dir=dest_data_dir
        )
        logging.info("Finished creating processing.json file.")

    # Upload to s3
    if job_configs["jobs"]["upload_to_s3"]:
        # TODO: Use s3transfer library instead of subprocess?
        logging.info("Uploading to s3.")
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
        logging.info("Finished uploading to s3.")

    # Upload to gcp
    if job_configs["jobs"]["upload_to_gcp"]:
        logging.info("Uploading to gcp.")
        gcp_bucket = job_configs["endpoints"]["gcp_bucket"]
        gcp_prefix = job_configs["endpoints"]["gcp_prefix"]
        gcp_dest = f"gs://{gcp_bucket}/{gcp_prefix}"
        if job_configs["upload_data_job"]["dryrun"]:
            subprocess.run(
                [
                    "gsutil",
                    "-m",
                    "rsync",
                    "-r",
                    "-n",
                    dest_data_dir,
                    gcp_dest,
                ]
            )
        else:
            subprocess.run(
                ["gsutil", "-m", "rsync", "-r", dest_data_dir, gcp_dest]
            )
        logging.info("Finished uploading to gcp.")

    # TODO: Combine these steps into a capsule?
    # Register Asset on CodeOcean
    if (job_configs["jobs"]["register_to_codeocean"] or
            job_configs["jobs"]["trigger_codeocean_spike_sorting"]):
        data_asset_response = None
        co_api_token = os.getenv("CODEOCEAN_API_TOKEN")
        co_domain = job_configs["endpoints"]["codeocean_domain"]
        co_client = CodeOceanClient(domain=co_domain,
                                    token=co_api_token)
        if job_configs["jobs"]["register_to_codeocean"]:
            # Use botocore to retrieve aws access tokens
            logging.info("Registering data asset to code ocean.")
            aws_session = get_session()
            aws_credentials = aws_session.get_credentials()
            aws_key = aws_credentials.access_key
            aws_secret = aws_credentials.secret_key
            co_tags = job_configs["register_on_codeocean_job"]["tags"]
            asset_name = job_configs["register_on_codeocean_job"]["asset_name"]
            mount = job_configs["register_on_codeocean_job"]["mount"]
            bucket = job_configs["endpoints"]["s3_bucket"]
            prefix = job_configs["endpoints"]["s3_prefix"]
            data_asset_response = co_client.register_data_asset(
                asset_name=asset_name,
                mount=mount,
                bucket=bucket,
                prefix=prefix,
                access_key_id=aws_key,
                secret_access_key=aws_secret,
                tags=co_tags,
            )
            logging.info(f"Finished registering data asset to code ocean. "
                         f"{data_asset_response.json()}")

        # Automatically trigger capsule run
        if job_configs["jobs"]["trigger_codeocean_spike_sorting"]:
            logging.info("Triggering a capsule run.")
            conf_name = "trigger_codeocean_spike_sorting_job"
            if data_asset_response is not None:
                response_contents = data_asset_response.json()
                data_asset_id = response_contents["id"]
            else:
                data_asset_id = (
                    job_configs[conf_name]["asset_id"])
            capsule_id = (
                job_configs[conf_name]["capsule_id"])
            mount = (
                job_configs[conf_name]["mount"])
            data_assets = [{"id": data_asset_id, "mount": mount}]
            capsule_run_response = (
                co_client.run_capsule(capsule_id=capsule_id,
                                      data_assets=data_assets))

            logging.info(f"Finished triggering a capsule run. Please check "
                         f"CodeOcean for status of capsule. "
                         f"{capsule_run_response}")
