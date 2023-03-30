"""Basic job to upload a data directory to s3 with some metadata attached."""

import argparse
import csv
import json
import logging
import os
import re
import shutil
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from aind_codeocean_api.codeocean import CodeOceanClient
from botocore.exceptions import ClientError

from aind_data_transfer.transformations.generic_compressors import (
    VideoCompressor,
    ZipCompressor,
)
from aind_data_transfer.transformations.metadata_creation import (
    ProceduresMetadata,
    RawDataDescriptionMetadata,
    SubjectMetadata,
)
from aind_data_transfer.util.s3_utils import (
    copy_to_s3,
    get_secret,
    upload_to_s3,
)


class GenericS3UploadJob:
    """Class for basic job construction."""

    SERVICE_ENDPOINT_KEY = "service_endpoints"
    METADATA_SERVICE_URL_KEY = "metadata_service_url"
    S3_DEFAULT_REGION = "us-west-2"
    VIDEO_ENCRYPTION_KEY_NAME = "video_encryption_password"

    # TODO: Move the code ocean configs into own class? Or import them?
    CODEOCEAN_DOMAIN_KEY = "codeocean_domain"
    CODEOCEAN_CAPSULE_KEY = "codeocean_trigger_capsule"
    CODEOCEAN_TOKEN_KEY = "codeocean-api-token"
    CODEOCEAN_TOKEN_KEY_ENV = CODEOCEAN_TOKEN_KEY.replace("-", "_").upper()
    CODEOCEAN_READ_WRITE_KEY = "CODEOCEAN_READWRITE_TOKEN"
    CODEOCEAN_JOB_TYPE = "register_data"
    BEHAVIOR_S3_PREFIX = "behavior"

    def __init__(self, args: list) -> None:
        """Initializes class with sys args. Convert the sys args to configs."""
        self.args = args
        self.configs = self._load_configs(args)

    @staticmethod
    def _get_endpoints(s3_region: str) -> dict:
        """
        If the service endpoints aren't set in the sys args, then this method
        will try to pull them from aws secrets manager. It's static since it's
        being called before the job is created.
        Parameters
        ----------
        s3_region : str

        Returns
        -------
        dict
          Will return an empty dictionary if the service endpoints are not set
          in sys args, or if they can't be pulled from aws secrets manager.

        """
        try:
            s3_secret_name = GenericS3UploadJob.SERVICE_ENDPOINT_KEY
            get_secret(s3_secret_name, s3_region)
            endpoints = json.loads(get_secret(s3_secret_name, s3_region))
        except ClientError as e:
            logging.warning(
                f"Unable to retrieve aws secret: "
                f"{GenericS3UploadJob.SERVICE_ENDPOINT_KEY}"
            )
            logging.debug(e.response)
            endpoints = {}
        return endpoints

    def upload_raw_data_folder(
        self, data_prefix: str, behavior_dir: Optional[Path] = None
    ) -> None:
        """
        Uploads the raw data folder to s3. Will compress first if that config
        is set.
        Parameters
        ----------
        data_prefix : str
          base prefix of where the data will be uploaded.
        behavior_dir : Path
          If behavior is not None and is a subdirectory of data_source, then
          it will be ignored and handled elsewhere.

        Returns
        -------
        None

        """
        # The behavior directory will be handled in a separate process even
        # if it's a subdirectory of the raw data source.
        if behavior_dir is not None:
            behavior_dir_excluded = Path(behavior_dir) / "*"
        else:
            behavior_dir_excluded = None

        if not self.configs.compress_raw_data:
            # Upload non-behavior data to s3
            upload_to_s3(
                directory_to_upload=self.configs.data_source,
                s3_bucket=self.configs.s3_bucket,
                s3_prefix=data_prefix,
                dryrun=self.configs.dry_run,
                excluded=behavior_dir_excluded,
            )
        else:
            logging.info("Compressing raw data folder: ")
            zc = ZipCompressor(display_progress_bar=True)
            compressed_data_folder_name = (
                str(os.path.basename(self.configs.data_source)) + ".zip"
            )
            s3_prefix = "/".join([data_prefix, compressed_data_folder_name])
            skip_dirs = None if behavior_dir is None else [behavior_dir]
            with tempfile.TemporaryDirectory() as td:
                output_zip_folder = os.path.join(
                    td, compressed_data_folder_name
                )
                zc.compress_dir(
                    input_dir=Path(self.configs.data_source),
                    output_dir=Path(output_zip_folder),
                    skip_dirs=skip_dirs,
                )
                copy_to_s3(
                    file_to_upload=output_zip_folder,
                    s3_bucket=self.configs.s3_bucket,
                    s3_prefix=s3_prefix,
                    dryrun=self.configs.dry_run,
                )

    def compress_and_upload_behavior_data(self):
        """Uploads the behavior directory. Attempts to encrypt the video
        files."""
        behavior_dir = self.configs.behavior_dir
        if behavior_dir:
            encryption_key = get_secret(
                self.VIDEO_ENCRYPTION_KEY_NAME, self.configs.s3_region
            )
            video_compressor = VideoCompressor(encryption_key=encryption_key)
            s3_behavior_prefix = "/".join(
                [self.s3_prefix, self.BEHAVIOR_S3_PREFIX]
            )
            # Copy data to a temp directory
            with tempfile.TemporaryDirectory() as td:
                for root, dirs, files in os.walk(behavior_dir):
                    for file in files:
                        raw_file_path = os.path.join(root, file)
                        new_file_path = os.path.join(td, file)
                        shutil.copy(raw_file_path, new_file_path)
                video_compressor.compress_all_videos_in_dir(td)
                upload_to_s3(
                    directory_to_upload=td,
                    s3_bucket=self.configs.s3_bucket,
                    s3_prefix=s3_behavior_prefix,
                    dryrun=self.configs.dry_run,
                )

    def upload_metadata_from_folder(self) -> List[str]:
        """Uploads json files stored in the user defined metadata directory.
        By default, the files generated by aind-metadata-service
        (subject.json etc.) will override the ones found in this directory
        unless the user sets the flag --metadata-dir-force."""
        # Get a list of file names for use later
        file_names = [
            fn
            for fn in os.listdir(self.configs.metadata_dir)
            if fn.endswith(".json")
        ]
        # Exclude all files, then add include so that only json files are
        # uploaded.
        upload_to_s3(
            directory_to_upload=self.configs.metadata_dir,
            s3_bucket=self.configs.s3_bucket,
            s3_prefix=self.s3_prefix,
            dryrun=self.configs.dry_run,
            excluded="*",
            included="*.json",
        )

        return file_names

    def upload_subject_metadata(self) -> None:
        """Retrieves subject metadata from metadata service and copies it to
        s3. Logs warning if unable to retrieve metadata from service."""
        metadata_service_url = self.configs.service_endpoints.get(
            "metadata_service_url"
        )
        if metadata_service_url:
            subject_metadata = SubjectMetadata.from_service(
                domain=metadata_service_url,
                subject_id=self.configs.subject_id,
            )
            file_name = subject_metadata.output_filename
            final_s3_prefix = "/".join([self.s3_prefix, file_name])
            with tempfile.TemporaryDirectory() as td:
                tmp_file_name = os.path.join(td, file_name)
                subject_metadata.write_to_json(tmp_file_name)
                copy_to_s3(
                    file_to_upload=tmp_file_name,
                    s3_bucket=self.configs.s3_bucket,
                    s3_prefix=final_s3_prefix,
                    dryrun=self.configs.dry_run,
                )
        else:
            logging.warning(
                "No metadata service url given. "
                "Not able to get subject metadata."
            )

    def upload_procedures_metadata(self) -> None:
        """Retrieves procedures metadata from metadata service and copies it to
        s3. Logs warning if unable to retrieve metadata from service."""
        metadata_service_url = self.configs.service_endpoints.get(
            "metadata_service_url"
        )
        if metadata_service_url:
            procedures_metadata = ProceduresMetadata.from_service(
                domain=metadata_service_url,
                subject_id=self.configs.subject_id,
            )
            file_name = procedures_metadata.output_filename
            final_s3_prefix = "/".join([self.s3_prefix, file_name])
            with tempfile.TemporaryDirectory() as td:
                tmp_file_name = os.path.join(td, file_name)
                procedures_metadata.write_to_json(tmp_file_name)
                copy_to_s3(
                    file_to_upload=tmp_file_name,
                    s3_bucket=self.configs.s3_bucket,
                    s3_prefix=final_s3_prefix,
                    dryrun=self.configs.dry_run,
                )
        else:
            logging.warning(
                "No metadata service url given. "
                "Not able to get procedures metadata."
            )

    def upload_data_description_metadata(self) -> None:
        """Builds basic data description and copies it to s3."""

        data_description_metadata = RawDataDescriptionMetadata.from_inputs(
            name=self.s3_prefix
        )
        file_name = data_description_metadata.output_filename
        final_s3_prefix = "/".join([self.s3_prefix, file_name])
        with tempfile.TemporaryDirectory() as td:
            tmp_file_name = os.path.join(td, file_name)
            data_description_metadata.write_to_json(tmp_file_name)
            copy_to_s3(
                file_to_upload=tmp_file_name,
                s3_bucket=self.configs.s3_bucket,
                s3_prefix=final_s3_prefix,
                dryrun=self.configs.dry_run,
            )

    def _get_codeocean_client(self) -> Optional[CodeOceanClient]:
        """Constructs a codeocean client. Will try to check if the api token
        is set as an environment variable. If not set, then tries to retrieve
        it from aws secrets manager. Otherwise, logs a warning and returns
        None."""

        codeocean_domain = self.configs.service_endpoints.get(
            self.CODEOCEAN_DOMAIN_KEY
        )
        # Try to see if api token is set by an env var
        co_api_token = os.getenv(self.CODEOCEAN_TOKEN_KEY_ENV)
        # If not set by an env var, check if it's stored in aws secrets
        if co_api_token is None:
            try:
                s3_secret_name = self.CODEOCEAN_TOKEN_KEY
                get_secret(s3_secret_name, self.configs.s3_region)
                token_key_val = json.loads(
                    get_secret(s3_secret_name, self.configs.s3_region)
                )
                co_api_token = token_key_val.get(self.CODEOCEAN_READ_WRITE_KEY)
            except ClientError as e:
                logging.warning(
                    f"Unable to retrieve aws secret: "
                    f"{self.CODEOCEAN_TOKEN_KEY}"
                )
                logging.debug(e.response)
        if co_api_token and codeocean_domain:
            codeocean_client = CodeOceanClient(
                domain=codeocean_domain, token=co_api_token
            )
        else:
            codeocean_client = None
        return codeocean_client

    def _codeocean_trigger_capsule_parameters(self):
        """Generate parameters to run code ocean capsule."""

        return {
            "trigger_codeocean_job": {
                "job_type": self.CODEOCEAN_JOB_TYPE,
                "capsule_id": self.configs.service_endpoints.get(
                    self.CODEOCEAN_CAPSULE_KEY
                ),
                "bucket": self.configs.s3_bucket,
                "prefix": self.s3_prefix,
            }
        }

    def trigger_codeocean_capsule(self) -> None:
        """Triggers the codeocean capsule. Logs a warning if the endpoints
        are not configured."""

        capsule_id = self.configs.service_endpoints.get(
            self.CODEOCEAN_CAPSULE_KEY
        )

        codeocean_client = self._get_codeocean_client()
        if codeocean_client and capsule_id:
            parameters = self._codeocean_trigger_capsule_parameters()
            parameter_list = [json.dumps(parameters)]
            logging.info("Triggering capsule run.")
            if not self.configs.dry_run:
                run_response = codeocean_client.run_capsule(
                    capsule_id=capsule_id,
                    data_assets=[],
                    parameters=parameter_list,
                )
                logging.debug(f"Run response: {run_response.json()}")
            else:
                codeocean_client.get_capsule(capsule_id=capsule_id)
                logging.info(
                    f"Would have ran capsule {capsule_id} "
                    f"at {codeocean_client.domain} with parameters: "
                    f"{parameter_list}."
                )
        else:
            logging.warning(
                "CodeOcean endpoints are required to trigger capsule."
            )

    @property
    def s3_prefix(self):
        """Constructs the s3_prefix from configs."""

        # The date and time strings are already validated upstream
        datetime.strptime(
            self.configs.acq_date + " " + self.configs.acq_time,
            "%Y-%m-%d %H-%M-%S",
        )

        return "_".join(
            [
                self.configs.modality,
                self.configs.subject_id,
                self.configs.acq_date,
                self.configs.acq_time,
            ]
        )

    @staticmethod
    def _parse_date(date: str) -> str:
        """Parses date string to %YYYY-%MM-%DD format"""
        stripped_date = date.strip()
        pattern = r"^\d{4}-\d{2}-\d{2}$"
        pattern2 = r"^\d{1,2}/\d{1,2}/\d{4}$"
        if re.match(pattern, stripped_date):
            parsed_date = datetime.strptime(stripped_date, "%Y-%m-%d")
            return parsed_date.strftime("%Y-%m-%d")
        elif re.match(pattern2, stripped_date):
            parsed_date = datetime.strptime(stripped_date, "%m/%d/%Y")
            return parsed_date.strftime("%Y-%m-%d")
        else:
            raise ValueError(
                "Incorrect date format, should be YYYY-MM-DD or MM/DD/YYYY"
            )

    @staticmethod
    def _parse_time(time: str) -> str:
        """Parses time string to "%HH-%MM-%SS format"""
        stripped_time = time.strip()
        pattern = r"^\d{1,2}-\d{1,2}-\d{1,2}$"
        pattern2 = r"^\d{1,2}:\d{1,2}:\d{1,2}$"
        if re.match(pattern, stripped_time):
            parsed_time = datetime.strptime(stripped_time, "%H-%M-%S")
            return parsed_time.strftime("%H-%M-%S")
        elif re.match(pattern2, stripped_time):
            parsed_time = datetime.strptime(stripped_time, "%H:%M:%S")
            return parsed_time.strftime("%H-%M-%S")
        else:
            raise ValueError(
                "Incorrect time format, should be HH-MM-SS or HH:MM:SS"
            )

    def _parse_date_time(self, job_args: argparse.Namespace):
        """Parses date and time to Excel default format"""
        args_dict = vars(job_args)
        args_dict["acq_date"] = self._parse_date(args_dict["acq_date"])
        args_dict["acq_time"] = self._parse_time(args_dict["acq_time"])

    def _load_configs(self, args: list) -> argparse.Namespace:
        """Parses sys args using argparse and resolves the service
        endpoints."""

        description = (
            "Uploads a data folder to s3 with a formatted file name. "
            "Also attaches metadata and registers asset to Code Ocean."
        )
        additional_info = (
            "It's possible to define multiple jobs in a csv file. "
            "Run: \n\tpython -m aind_data_transfer.jobs.s3_upload_job --help "
            "--jobs-csv-file\nfor more info."
        )

        parser = argparse.ArgumentParser(
            description=description,
            epilog=additional_info,
            formatter_class=argparse.RawDescriptionHelpFormatter,
        )
        parser.add_argument("-d", "--data-source", required=True, type=str)
        parser.add_argument("-b", "--s3-bucket", required=True, type=str)
        parser.add_argument("-s", "--subject-id", required=True, type=str)
        parser.add_argument("-m", "--modality", required=True, type=str)
        parser.add_argument("-a", "--acq-date", required=True, type=str)
        parser.add_argument("-t", "--acq-time", required=True, type=str)
        parser.add_argument("-v", "--behavior-dir", required=False, type=str)
        parser.add_argument("-x", "--metadata-dir", required=False, type=str)
        parser.add_argument(
            "-e", "--service-endpoints", required=False, type=json.loads
        )
        parser.add_argument("-r", "--s3-region", required=False, type=str)
        parser.add_argument("--dry-run", action="store_true")
        parser.add_argument("--compress-raw-data", action="store_true")
        parser.add_argument("--metadata-dir-force", action="store_true")
        parser.set_defaults(dry_run=False)
        parser.set_defaults(compress_raw_data=False)
        parser.set_defaults(metadata_dir_force=False)
        parser.set_defaults(s3_region=self.S3_DEFAULT_REGION)
        job_args = parser.parse_args(args)
        if job_args.service_endpoints is None:
            job_args.service_endpoints = self._get_endpoints(
                job_args.s3_region
            )
        self._parse_date_time(job_args)
        return job_args

    def run_job(self) -> None:
        """Uploads data folder to s3, then attempts to upload metadata to s3
        and trigger the codeocean capsule."""

        data_prefix = "/".join([self.s3_prefix, self.configs.modality])

        # Optionally upload the data in the behavior directory to s3
        behavior_dir = self.configs.behavior_dir
        if behavior_dir is not None:
            self.compress_and_upload_behavior_data()

        # Upload non-behavior data to s3
        behavior_path = None if behavior_dir is None else Path(behavior_dir)
        self.upload_raw_data_folder(
            data_prefix=data_prefix, behavior_dir=behavior_path
        )

        # Optionally upload the data in the metadata directory to s3
        metadata_dir = self.configs.metadata_dir
        user_defined_metadata_files = None
        if metadata_dir is not None:
            user_defined_metadata_files = self.upload_metadata_from_folder()

        # Will create a data description file if not found in user defined
        # metadata directory. Assumes data description built by this job
        # is the source of truth unless metadata-dir-force is set.
        if (
            (user_defined_metadata_files is None)
            or ("data_description.json" not in user_defined_metadata_files)
            or (self.configs.metadata_dir_force is False)
        ):
            self.upload_data_description_metadata()

        # Create subject.json file if metadata service url is provided
        # Will not upload if subject.json in user defined metadata folder and
        # force command present. Assumes aind-metadata-service is source of
        # truth by default.
        if (
            (user_defined_metadata_files is None)
            or ("subject.json" not in user_defined_metadata_files)
            or (self.configs.metadata_dir_force is False)
        ):
            self.upload_subject_metadata()

        # Create procedures.json file if metadata service url is provided
        # Will not upload if procedures.json in user defined metadata folder
        # and force command present. Assumes aind-metadata-service is source of
        # truth by default.
        if (
            (user_defined_metadata_files is None)
            or ("procedures.json" not in user_defined_metadata_files)
            or (self.configs.metadata_dir_force is False)
        ):
            self.upload_procedures_metadata()

        # Register to code ocean if url is provided
        self.trigger_codeocean_capsule()


class GenericS3UploadJobList:
    """Class to run multiple jobs defined in a csv file."""

    # Expected field names in the csv file
    CSV_FILE_FIELD_NAMES = [
        "data-source",
        "s3-bucket",
        "subject-id",
        "modality",
        "acq-date",
        "acq-time",
        "behavior-dir",  # Optional
        "metadata-dir",  # Optional
        "metadata-dir-force",  # Optional
    ]

    def __init__(self, args: list) -> None:
        """Initializes class with sys args. Convert the sys args to configs."""
        self.args = args
        self.configs = self._load_configs(args)
        self.job_param_list = self._create_job_param_list()

    @staticmethod
    def _load_configs(args: list) -> argparse.Namespace:
        """Parses args into argparse object."""

        help_message_csv_file = (
            "Path to csv file with list of job configs. The csv file needs "
            "to have the headers: data-source, s3-bucket, subject-id, "
            "modality, acq-date, acq-time. Optional headers: behavior-dir, "
            "metadata-dir."
        )
        help_message_dry_run = (
            "Tests the upload without actually uploading the files."
        )
        help_message_compress_raw_data = (
            "Zip raw data folder before uploading."
        )
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "-j",
            "--jobs-csv-file",
            required=True,
            type=str,
            help=help_message_csv_file,
        )
        parser.add_argument(
            "--dry-run", action="store_true", help=help_message_dry_run
        )
        parser.add_argument(
            "--compress-raw-data",
            action="store_true",
            help=help_message_compress_raw_data,
        )
        parser.set_defaults(dry_run=False)
        parser.set_defaults(compress_raw_data=False)
        job_args = parser.parse_args(args)
        return job_args

    def _create_job_param_list(self):
        """Reads in the csv file and outputs a list that can be parsed by
        argparse."""
        job_list = list()
        with open(self.configs.jobs_csv_file, newline="") as csvfile:
            reader = csv.DictReader(csvfile, skipinitialspace=True)
            for row in reader:
                job_list.append(row)
        assert len(job_list) > 0
        # We want to check that the csv header contains the proper info.
        # Must contain relevant keys but does not need to contain the
        # optional keys. There's probably a cleaner way to do this?
        job_list_keys = job_list[0].keys()
        assert len(job_list_keys) == len(set(job_list_keys))
        assert set(self.CSV_FILE_FIELD_NAMES[0:-3]).issubset(
            set(job_list_keys)
        )
        assert set(job_list_keys).issubset(self.CSV_FILE_FIELD_NAMES)
        param_list = list()
        # Loop through rows in job_list csv file. Build the shell command
        # from items in the row. Can ignore the optional settings.
        for job_item in job_list:
            res = [
                x
                for t in [
                    ("--" + keys[0], keys[1])
                    for keys in job_item.items()
                    if (
                        (keys[0] != "metadata-dir-force")
                        and (
                            (keys[0] not in ["behavior-dir", "metadata-dir"])
                            or ((keys[1] is not None) and (keys[1] != ""))
                        )
                    )
                ]
                for x in t
            ]
            # Convert metadata-dir-force to a flag if it's been set in csv file
            if (job_item.get("metadata-dir-force") is not None) and (
                job_item.get("metadata-dir-force")
                in ["True", "true", "t", "T", "Yes", "yes", "y", "1"]
            ):
                res.append("--metadata-dir-force")
            # Add dry-run command if set in command line.
            if self.configs.dry_run:
                res.append("--dry-run")
            if self.configs.compress_raw_data:
                res.append("--compress-raw-data")
            param_list.append(res)
        return param_list

    def run_job(self):
        """Loop through param list and run GenericS3UploadJob"""
        total_jobs = len(self.job_param_list)
        current_job_num = 1
        logging.info("Starting all jobs...")
        for job_params in self.job_param_list:
            one_job = GenericS3UploadJob(job_params)
            logging.info(
                f"Running job {current_job_num} of {total_jobs} "
                f"with params: {job_params}"
            )
            one_job.run_job()
            logging.info(
                f"Finished job {current_job_num} of {total_jobs} "
                f"with params: {job_params}"
            )
            current_job_num += 1
        logging.info("Finished all jobs!")


if __name__ == "__main__":
    sys_args = sys.argv[1:]
    if ("-j" in sys_args) or ("--jobs-csv-file" in sys_args):
        job = GenericS3UploadJobList(sys_args)
    else:
        job = GenericS3UploadJob(sys_args)
    job.run_job()
