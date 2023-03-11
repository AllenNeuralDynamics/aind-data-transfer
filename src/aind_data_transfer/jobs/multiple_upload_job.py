import argparse
import csv
import logging

from aind_data_transfer.config_loader.generic_config_loader import (
    GenericJobConfigurationLoader,
)
from aind_data_transfer.jobs.s3_upload_job import GenericS3UploadJob


class S3UploadJobList:
    """Class to run multiple jobs defined in a csv file."""

    # Expected field names in the csv file
    CSV_FILE_FIELD_NAMES = [
        "data-source",
        "s3-bucket",
        "subject-id",
        "modality",
        "acq-date",
        "acq-time",
        "service-endpoints",  # Optional
        "secrets",  # Optional
        "extra-config-source",  # Optional
        "behavior-dir",  # Optional
        "metadata-dir",  # Optional
        "metadata-dir-force",  # Optional
        "compress-data",  # Optional
        "dry-run",  # Optional
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
        help_message_compress_data = (
            "Compress the each data source before uploading."
        )
        help_message_metadata_dir_force = (
            "If metadata_dir is non-empty, then that will be considered the "
            "source of truth vs. the metadata json built by the service."
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
        parser.add_argument(
            "--compress-data",
            action="store_true",
            help=help_message_compress_data,
        )
        parser.set_defaults(compress_data=False)
        parser.add_argument(
            "--metadata-dir-force",
            action="store_true",
            help=help_message_metadata_dir_force,
        )
        parser.set_defaults(metadadata_dir_force=False)
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
        assert set(self.CSV_FILE_FIELD_NAMES[0:-8]).issubset(
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

            truth_list = ["True", "true", "t", "T", "Yes", "yes", "y", "1"]
            false_list = ["False", "false", "f", "F", "No", "no", "n", "0"]
            # Add metadata-dir-force flag if set in csv file
            if (job_item.get("metadata-dir-force") is not None) and (
                job_item.get("metadata-dir-force") in truth_list
            ):
                res.append("--metadata-dir-force")
            elif (
                (self.configs.metadata_dir_force is not None)
                and (job_item.get("metadata-dir-force") is None)
                or (job_item.get("metadata-dir-force") not in false_list)
            ):
                res.append("--metadata-dir-force")
            # Add dry-run flag if set in csv file
            if (job_item.get("dry-run") is not None) and (
                job_item.get("dry-run") in truth_list
            ):
                res.append("--dry-run")
            elif (
                (self.configs.dry_run is not None)
                and (job_item.get("dry-run") is None)
                or (job_item.get("dry-run") not in false_list)
            ):
                res.append("--dry-run")

            # Add compress data flag if set in csv file
            if (job_item.get("compress-data") is not None) and (
                job_item.get("compress-data") in truth_list
            ):
                res.append("--compress-data")
            elif (
                (self.configs.compress_data is not None)
                and (job_item.get("compress-data") is None)
                or (job_item.get("compress-data") not in false_list)
            ):
                res.append("--compress-data")
            elif (
                self.configs.modality == "ecephys"
                and (job_item.get("compress-data") is None)
                or (job_item.get("compress-data") not in false_list)
            ):
                res.append("--compress-data")

            param_list.append(res)
        return param_list

    def run_job(self):
        """Loop through param list and run GenericS3UploadJob"""
        total_jobs = len(self.job_param_list)
        current_job_num = 1
        logging.info("Starting all jobs...")
        for job_params in self.job_param_list:
            base_configs = GenericJobConfigurationLoader(job_params)
            if base_configs.configs.modality == "ecephys":
                from aind_data_transfer.jobs.ecephys_job import EcephysJob

                one_job = EcephysJob(job_params)
                # Load ecephys job
            else:
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
