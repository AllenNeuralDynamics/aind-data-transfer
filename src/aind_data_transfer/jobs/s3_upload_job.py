"""Basic job to upload a data directory to s3 with some metadata attached."""

import argparse
import csv
import datetime
import json
import logging
import os
import sys
import tempfile
from typing import Optional

from aind_codeocean_api.codeocean import CodeOceanClient
from botocore.exceptions import ClientError

from aind_data_transfer.transformations.metadata_creation import (
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

    # TODO: Move the code ocean configs into own class? Or import them?
    CODEOCEAN_DOMAIN_KEY = "codeocean_domain"
    CODEOCEAN_CAPSULE_KEY = "codeocean_trigger_capsule"
    CODEOCEAN_TOKEN_KEY = "codeocean-api-token"
    CODEOCEAN_TOKEN_KEY_ENV = CODEOCEAN_TOKEN_KEY.replace("-", "_").upper()
    CODEOCEAN_READ_WRITE_KEY = "CODEOCEAN_READWRITE_TOKEN"
    CODEOCEAN_JOB_TYPE = "register_data"

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

    def upload_subject_metadata(self) -> None:
        """Retrieves subject metadata from metadata service and copies it to
        s3. Logs warning if unable to retrieve metadata from service."""
        metadata_service_url = self.configs.service_endpoints.get(
            "metadata_service_url"
        )
        if metadata_service_url:
            subject_metadata = SubjectMetadata.ephys_job_to_subject(
                metadata_service_url=metadata_service_url,
                subject_id=self.configs.subject_id,
            )
            file_name = SubjectMetadata.output_file_name
            final_s3_prefix = "/".join([self.s3_prefix, file_name])
            with tempfile.TemporaryDirectory() as td:
                tmp_file_name = os.path.join(td, file_name)
                with open(tmp_file_name, "w") as fh:
                    fh.write(json.dumps(subject_metadata, indent=4))
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

    def upload_data_description_metadata(self) -> None:
        """Builds basic data description and copies it to s3."""

        data_description_metadata = (
            RawDataDescriptionMetadata.get_data_description(
                name=self.s3_prefix
            )
        )
        file_name = RawDataDescriptionMetadata.output_file_name
        final_s3_prefix = "/".join([self.s3_prefix, file_name])
        with tempfile.TemporaryDirectory() as td:
            tmp_file_name = os.path.join(td, file_name)
            with open(tmp_file_name, "w") as fh:
                fh.write(data_description_metadata.json(**{"indent": 4}))
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

        # Validate date and time strings
        try:
            datetime.datetime.strptime(
                self.configs.acq_date + " " + self.configs.acq_time,
                "%Y-%m-%d %H-%M-%S",
            )
        except ValueError:
            raise ValueError(
                "Incorrect data format, acq_date should be "
                "yyyy-MM-dd and acq_time should be HH-mm-SS"
            )

        return "_".join(
            [
                self.configs.modality,
                self.configs.subject_id,
                self.configs.acq_date,
                self.configs.acq_time,
            ]
        )

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
        parser.add_argument(
            "-e", "--service-endpoints", required=False, type=json.loads
        )
        parser.add_argument("-r", "--s3-region", required=False, type=str)
        parser.add_argument("--dry-run", action="store_true")
        parser.set_defaults(dry_run=False)
        parser.set_defaults(s3_region=self.S3_DEFAULT_REGION)
        job_args = parser.parse_args(args)
        if job_args.service_endpoints is None:
            job_args.service_endpoints = self._get_endpoints(
                job_args.s3_region
            )
        return job_args

    def run_job(self) -> None:
        """Uploads data folder to s3, then attempts to upload metadata to s3
        and trigger the codeocean capsule."""

        data_prefix = "/".join([self.s3_prefix, self.configs.modality])

        upload_to_s3(
            directory_to_upload=self.configs.data_source,
            s3_bucket=self.configs.s3_bucket,
            s3_prefix=data_prefix,
            dryrun=self.configs.dry_run,
        )

        # Create subject.json file if metadata service url is provided
        self.upload_subject_metadata()

        # Create data description file
        self.upload_data_description_metadata()

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
            "modality, acq-date, acq-time"
        )
        help_message_dry_run = (
            "Tests the upload without actually uploading the files."
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
        parser.set_defaults(dry_run=False)
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
        assert sorted(job_list[0].keys()) == sorted(self.CSV_FILE_FIELD_NAMES)
        param_list = list()
        for job_item in job_list:
            res = [
                x
                for t in [
                    ("--" + keys[0], keys[1]) for keys in job_item.items()
                ]
                for x in t
            ]
            if self.configs.dry_run:
                res.append("--dry-run")
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
