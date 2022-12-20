"""Job that reads open ephys data, compresses, and writes it."""
import json
import logging
import os
import platform
import subprocess
import sys
import warnings
from datetime import datetime, timezone
from pathlib import Path

from aind_codeocean_api.codeocean import CodeOceanClient

from aind_data_transfer.config_loader.ephys_configuration_loader import (
    EphysJobConfigurationLoader,
)
from aind_data_transfer.readers import EphysReaders
from aind_data_transfer.transformations.compressors import EphysCompressors
from aind_data_transfer.transformations.metadata_creation import (
    ProcessingMetadata,
    SubjectMetadata,
)
from aind_data_transfer.util.npopto_correction import (
    correct_np_opto_electrode_locations,
)
from aind_data_transfer.util.s3_utils import get_secret
from aind_data_transfer.writers import EphysWriters

root_logger = logging.getLogger()

# Suppress a warning from one of the third-party libraries
deprecation_msg = (
    "Creating a LegacyVersion has been deprecated and will be "
    "removed in the next major release"
)
warnings.filterwarnings(
    "ignore", category=DeprecationWarning, message=deprecation_msg
)


# TODO: Break these up into importable jobs to fix the flake8 warning?
def run_job(args):  # noqa: C901
    # Location of conf file passed in as command line arg
    job_start_time = datetime.now(timezone.utc)
    job_configs = EphysJobConfigurationLoader().load_configs(args)

    root_logger.setLevel(job_configs["logging"]["level"])
    if job_configs["logging"].get("file") is not None:
        fh = logging.FileHandler(job_configs["logging"]["file"])
        fh.setLevel(job_configs["logging"]["level"])
        root_logger.addHandler(fh)

    # Extract raw data name, (e.g., openephys) and raw data path
    data_name = job_configs["data"]["name"]
    data_src_dir = Path(job_configs["endpoints"]["raw_data_dir"])
    dest_data_dir = Path(job_configs["endpoints"]["dest_data_dir"])

    logging.info("Finished loading configs.")

    # Clip data job
    if job_configs["jobs"]["clip"]:
        logging.info("Clipping source data. This may take a minute.")
        clipped_data_path = dest_data_dir / "ecephys_clipped"
        clip_kwargs = job_configs["clip_data_job"]["clip_kwargs"]
        streams_to_clip = EphysReaders.get_streams_to_clip(
            data_name, data_src_dir
        )
        aws_secret_names = job_configs["aws_secret_names"]
        secret_name = aws_secret_names.get("video_encryption_password")
        secret_region = aws_secret_names.get("region")
        video_encryption_key_val = {"password": None}
        if secret_name:
            video_encryption_key_val = json.loads(
                get_secret(secret_name, secret_region)
            )

        behavior_directory = job_configs["endpoints"].get("behavior_directory")
        EphysWriters.copy_and_clip_data(
            src_dir=data_src_dir,
            dst_dir=clipped_data_path,
            stream_gen=streams_to_clip,
            behavior_dir=behavior_directory,
            video_encryption_key=video_encryption_key_val["password"],
            **clip_kwargs,
        )

        # Correct NP-opto electrode positions:
        # correction is skipped if Neuropix-PXI version > 0.4.0
        correct_np_opto_electrode_locations(clipped_data_path)

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
        max_filename_length = job_configs["compress_data_job"].get(
            "max_windows_filename_len"
        )
        read_blocks = EphysReaders.get_read_blocks(data_name, data_src_dir)
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
        logging.info("Finished compressing source data.")

    job_end_time = datetime.now(timezone.utc)
    if job_configs["jobs"]["attach_metadata"]:
        # Processing metadata
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
        parameters = job_configs
        processing_instance = ProcessingMetadata.ephys_job_to_processing(
            start_date_time=start_date_time,
            end_date_time=end_date_time,
            input_location=str(input_location),
            output_location=output_location,
            code_url=code_url,
            parameters=parameters,
            notes=None,
        )

        file_path = dest_data_dir / ProcessingMetadata.output_file_name
        with open(file_path, "w") as f:
            contents = processing_instance.json(**{"indent": 4})
            f.write(contents)
        logging.info("Finished creating processing.json file.")

        # Subject metadata
        logging.info("Creating subject.json file.")
        metadata_url = job_configs["endpoints"]["metadata_service_url"]
        subject_id = job_configs["data"].get("subject_id")
        subject_instance = SubjectMetadata.ephys_job_to_subject(
            metadata_url, subject_id, dest_data_dir.name
        )
        s_file_path = dest_data_dir / SubjectMetadata.output_file_name
        if subject_instance is not None:
            with open(s_file_path, "w") as f:
                f.write(json.dumps(subject_instance, indent=4))
            logging.info("Finished creating subject.json file.")
        else:
            logging.warning("No subject.json file created!")

    # Upload to s3
    if platform.system() == "Windows":
        shell = True
    else:
        shell = False
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
                ],
                shell=shell,
            )
        else:
            subprocess.run(
                ["aws", "s3", "sync", dest_data_dir, aws_dest], shell=shell
            )
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
                ],
                shell=shell,
            )
        else:
            subprocess.run(
                ["gsutil", "-m", "rsync", "-r", dest_data_dir, gcp_dest],
                shell=shell,
            )
        logging.info("Finished uploading to gcp.")

    if job_configs["jobs"]["trigger_codeocean_job"]:
        logging.info("Triggering capsule run.")
        capsule_id = job_configs["trigger_codeocean_job"]["capsule_id"]
        co_api_token = os.getenv("CODEOCEAN_API_TOKEN")
        if co_api_token is None:
            aws_secret_names = job_configs["aws_secret_names"]
            secret_name = aws_secret_names.get("code_ocean_api_token_name")
            secret_region = aws_secret_names.get("region")
            token_key_val = json.loads(get_secret(secret_name, secret_region))
            co_api_token = token_key_val["CODEOCEAN_READWRITE_TOKEN"]
        co_domain = job_configs["endpoints"]["codeocean_domain"]
        co_client = CodeOceanClient(domain=co_domain, token=co_api_token)
        run_response = co_client.run_capsule(
            capsule_id=capsule_id,
            data_assets=[],
            parameters=[json.dumps(job_configs)],
        )
        logging.debug(f"Run response: {run_response.json()}")

    return dest_data_dir


if __name__ == "__main__":
    sys_args = sys.argv[1:]
    run_job(sys_args)
