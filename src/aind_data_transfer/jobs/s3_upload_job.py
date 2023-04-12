"""Basic job to upload a data directory to s3 with some metadata attached."""

import argparse
import csv
import logging
import sys
from typing import Any, List, Optional

from aind_data_schema.data_description import ExperimentType

from aind_data_transfer.config_loader.base_config import BasicJobEndpoints
from aind_data_transfer.jobs.basic_job import BasicJob, BasicUploadJobConfigs


class GenericS3UploadJobList:
    """Class to run multiple jobs defined in a csv file."""

    def __init__(self, args: list) -> None:
        """Initializes class with sys args. Convert the sys args to configs."""
        self.args = args
        self.configs = self._load_configs(args)
        self.job_list = self._create_job_config_list()

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

    @staticmethod
    def _clean_csv_entry(csv_key: str, csv_value: Optional[str]) -> Any:
        """Tries to set the default value for optional settings if the csv
        entry is blank."""
        if csv_value is None or csv_value == "" or csv_value == " ":
            clean_val = BasicUploadJobConfigs.__fields__[csv_key].default
        else:
            clean_val = csv_value.strip()
        return clean_val

    def _create_job_config_list(self) -> List[BasicJob]:
        """Reads in the csv file and outputs a list of Job Configs."""
        job_list = list()
        param_store_name = None
        job_endpoints = {}
        with open(self.configs.jobs_csv_file, newline="") as csvfile:
            reader = csv.DictReader(csvfile, skipinitialspace=True)
            for row in reader:
                cleaned_row = {
                    k.strip().replace("-", "_"): self._clean_csv_entry(
                        k.strip().replace("-", "_"), v
                    )
                    for k, v in row.items()
                }
                cleaned_row["acq_date"] = BasicUploadJobConfigs.parse_date(
                    cleaned_row["acq_date"]
                )
                cleaned_row["acq_time"] = BasicUploadJobConfigs.parse_time(
                    cleaned_row["acq_time"]
                )
                # Override with flags set in command line
                if self.configs.dry_run is True:
                    cleaned_row["dry_run"] = True
                if self.configs.compress_raw_data is True:
                    cleaned_row["compress_raw_data"] = True
                # Avoid downloading endpoints from aws multiple times
                if cleaned_row.get("aws_param_store_name") is not None:
                    # Check if param store is defined in previous row
                    if cleaned_row["aws_param_store_name"] == param_store_name:
                        cleaned_row.update(job_endpoints)
                    # Otherwise, download it from aws
                    else:
                        job_endpoints = BasicJobEndpoints(
                            aws_param_store_name=(
                                cleaned_row["aws_param_store_name"]
                            )
                        ).dict()
                        cleaned_row.update(job_endpoints)
                        param_store_name = cleaned_row["aws_param_store_name"]
                    del cleaned_row["aws_param_store_name"]

                if (
                    cleaned_row.get("experiment_type")
                    == ExperimentType.ECEPHYS.value
                ):
                    # Conditional imports aren't ideal, but this will avoid
                    # installing unnecessary dependencies for non-ephys uploads

                    ecephys_upload_job_configs_class = getattr(
                        __import__(
                            "aind_data_transfer.config_loader.ecephys_config",
                            fromlist=["EcephysUploadJobConfigs"],
                        ),
                        "EcephysUploadJobConfigs",
                    )

                    ecephys_job_class = getattr(
                        __import__(
                            "aind_data_transfer.jobs.ecephys_job",
                            fromlist=["EcephysJob"],
                        ),
                        "EcephysJob",
                    )

                    configs_from_row = ecephys_upload_job_configs_class(
                        **cleaned_row
                    )
                    new_job = ecephys_job_class(job_configs=configs_from_row)
                else:
                    configs_from_row = BasicUploadJobConfigs(**cleaned_row)
                    new_job = BasicJob(job_configs=configs_from_row)
                job_list.append(new_job)
        return job_list

    def run_job(self):
        """Loop through param list and run GenericS3UploadJob"""
        total_jobs = len(self.job_list)
        current_job_num = 1
        logging.info("Starting all jobs...")
        for one_job in self.job_list:
            # TODO: Add switch on experiment type
            logging.info(
                f"Running job {current_job_num} of {total_jobs} "
                f"with params: {one_job.job_configs.dict()}"
            )
            one_job.run_job()
            logging.info(
                f"Finished job {current_job_num} of {total_jobs} "
                f"with params: {one_job.job_configs.dict()}"
            )
            current_job_num += 1
        logging.info("Finished all jobs!")


if __name__ == "__main__":
    sys_args = sys.argv[1:]
    if ("-j" in sys_args) or ("--jobs-csv-file" in sys_args):
        job = GenericS3UploadJobList(sys_args)
    else:
        configs = BasicUploadJobConfigs.from_args(sys_args)
        # This is here for legacy purposes. In the future, all calls to
        # s3_upload_job will require the jobs to be defined in csv file.
        job = BasicJob(job_configs=configs)
    job.run_job()
