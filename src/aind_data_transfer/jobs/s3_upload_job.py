"""Basic job to upload a data directory to s3 with some metadata attached."""

import argparse
import csv
import logging
import re
import sys
from typing import Any, Dict, List, Optional

from aind_data_transfer.config_loader.base_config import (
    BasicJobEndpoints,
    ModalityConfigs,
)
from aind_data_transfer.jobs.basic_job import BasicJob, BasicUploadJobConfigs


class GenericS3UploadJobList:
    """Class to run multiple jobs defined in a csv file."""

    _MODALITY_ENTRY_PATTERN = re.compile(r"^modality(\d*)$")

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
        help_message_skip_staging = (
            "Skip copying uncompressed data to a staging directory."
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
            "--dry-run",
            action="store_true",
            help=help_message_dry_run,
            default=False,
        )
        parser.add_argument(
            "--compress-raw-data",
            action="store_true",
            help=help_message_compress_raw_data,
            default=None,
        )
        parser.add_argument(
            "--force-cloud-sync",
            action="store_true",
            help="Force syncing of data if s3 location already exists.",
            default=False,
        )
        parser.add_argument(
            "--skip-staging",
            action="store_true",
            help=help_message_skip_staging,
            default=False,
        )
        job_args = parser.parse_args(args)
        return job_args

    @staticmethod
    def _clean_csv_entry(csv_key: str, csv_value: Optional[str]) -> Any:
        """Tries to set the default value for optional settings if the csv
        entry is blank."""
        if (
            csv_value is None or csv_value == "" or csv_value == " "
        ) and BasicUploadJobConfigs.model_fields.get(csv_key) is not None:
            clean_val = BasicUploadJobConfigs.model_fields[csv_key].default
        else:
            clean_val = csv_value.strip()
        return clean_val

    def _map_row_and_key_to_modality_config(  # noqa: C901
        self,
        modality_key: str,
        cleaned_row: Dict[str, Any],
        modality_counts: Dict[str, Optional[int]],
    ) -> Optional[ModalityConfigs]:
        """
        Maps a cleaned csv row and a key for a modality to process into an
        ModalityConfigs object.
        Parameters
        ----------
        modality_key : str
          The column header like modality, modality0, or modality1, etc.
        cleaned_row : Dict[str, Any]
          The csv row that's been cleaned.
        modality_counts : Dict[str, Optional[int]]
          If more than one type of modality is present in the csv row, then
          they will be assigned numerical ids. This will allow multiple of the
          same modalities to be stored under folders like ecephys0, etc.

        Returns
        -------
        Optional[ModalityConfigs]
          None if unable to parse csv row properly.

        """
        modality: str = cleaned_row[modality_key]
        modality_exists = modality is not None and modality != ""
        modality_unindexed = modality_key == "modality" and modality_exists

        # For backwards compatibility, see if configs are set in the old way
        if (
            modality_unindexed
            and cleaned_row.get("modality.source") is not None
        ):
            source = cleaned_row.get("modality.source")
        elif (
            modality_unindexed
            and cleaned_row.get("modality.data_source") is not None
        ):
            source = cleaned_row.get(f"{modality_key}.data_source")
        elif modality_unindexed and cleaned_row.get("source") is not None:
            source = cleaned_row.get("source")
        elif modality_unindexed and cleaned_row.get("data_source") is not None:
            source = cleaned_row.get("data_source")
        elif modality_exists and modality_key != "modality":
            source = cleaned_row.get(f"{modality_key}.source")
        else:
            source = None

        if (
            source is not None
            and modality_unindexed
            and cleaned_row.get("modality.compress_raw_data") is not None
        ):
            compress_raw_data = cleaned_row.get("modality.compress_raw_data")
        elif (
            source is not None
            and modality_unindexed
            and cleaned_row.get("compress_raw_data") is not None
        ):
            compress_raw_data = cleaned_row.get("compress_raw_data")
        elif source is not None and modality_key != "modality":
            compress_raw_data = cleaned_row.get(
                f"{modality_key}.compress_raw_data"
            )
        else:
            compress_raw_data = None

        compress_raw_data = (
            True
            if self.configs.compress_raw_data is True
            else compress_raw_data
        )

        if (
            source is not None
            and modality_unindexed
            and cleaned_row.get("modality.skip_staging") is not None
        ):
            skip_staging = cleaned_row.get("modality.skip_staging")
        elif (
            source is not None
            and modality_unindexed
            and cleaned_row.get("skip_staging") is not None
        ):
            skip_staging = cleaned_row.get("skip_staging")
        elif (
            source is not None
            and modality_key != "modality"
            and cleaned_row.get(f"{modality_key}.skip_staging") is not None
        ):
            skip_staging = cleaned_row.get(f"{modality_key}.skip_staging")
        else:
            skip_staging = cleaned_row.get("skip_staging")

        skip_staging = False if skip_staging is None else skip_staging

        if (
            source is not None
            and modality_unindexed
            and cleaned_row.get("modality.extra_configs") is not None
        ):
            extra_configs = cleaned_row.get("modality.extra_configs")
        elif (
            source is not None
            and modality_unindexed
            and cleaned_row.get("extra_configs") is not None
        ):
            extra_configs = cleaned_row.get("extra_configs")
        elif source is not None and modality_key != "modality":
            extra_configs = cleaned_row.get(f"{modality_key}.extra_configs")
        else:
            extra_configs = None

        if source is None:
            return None
        else:
            modality_configs = ModalityConfigs(
                modality=modality,
                source=source,
                compress_raw_data=compress_raw_data,
                extra_configs=extra_configs,
                skip_staging=skip_staging,
            )
            num_id = modality_counts.get(modality)
            modality_configs._number_id = num_id
            if num_id is None:
                modality_counts[modality] = 1
            else:
                modality_counts[modality] = num_id + 1
            return modality_configs

    def _parse_modality_configs_from_row(self, cleaned_row: dict) -> None:
        """
        Parses csv row into a list of ModalityConfigs. Will then process the
        cleaned_row dictionary by removing the old modality keys and replacing
        them with just modalities: List[ModalityConfigs.]
        Parameters
        ----------
        cleaned_row : dict
          csv row that contains keys like modality0, modality0.source,
          modality1, modality1.source, etc.

        Returns
        -------
        None
          Modifies cleaned_row dict in-place

        """
        modalities = []
        modality_keys = [
            m
            for m in cleaned_row.keys()
            if self._MODALITY_ENTRY_PATTERN.match(m)
        ]
        modality_counts: Dict[str, Optional[int]] = dict()
        # Check uniqueness of keys
        if len(modality_keys) != len(set(modality_keys)):
            raise KeyError(
                f"Modality keys need to be unique in csv "
                f"header: {modality_keys}"
            )
        for modality_key in modality_keys:
            modality_configs = self._map_row_and_key_to_modality_config(
                modality_key=modality_key,
                cleaned_row=cleaned_row,
                modality_counts=modality_counts,
            )
            if modality_configs is not None:
                modalities.append(modality_configs)

        # Del old modality keys and replace them with list of modality_configs
        for row_key in [
            m
            for m in cleaned_row.keys()
            if (
                m.startswith("modality")
                or m
                in {
                    "data_source",
                    "extra_configs",
                    "source",
                    "compress_raw_data",
                    "skip_staging",
                }
            )
        ]:
            del cleaned_row[row_key]
        cleaned_row["modalities"] = modalities

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
                # Override with flags set in command line
                if self.configs.compress_raw_data is True:
                    cleaned_row["compress_raw_data"] = True
                if self.configs.dry_run is True:
                    cleaned_row["dry_run"] = True
                if self.configs.force_cloud_sync is True:
                    cleaned_row["force_cloud_sync"] = True
                if self.configs.skip_staging is True:
                    cleaned_row["skip_staging"] = True
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
                        ).model_dump()
                        cleaned_row.update(job_endpoints)
                        param_store_name = cleaned_row["aws_param_store_name"]
                    del cleaned_row["aws_param_store_name"]
                self._parse_modality_configs_from_row(cleaned_row=cleaned_row)

                configs_from_row = BasicUploadJobConfigs(**cleaned_row)
                new_job = BasicJob(job_configs=configs_from_row)
                job_list.append(new_job)
        return job_list

    def run_job(self):
        """Loop through param list and run GenericS3UploadJob"""
        total_jobs = len(self.job_list)
        current_job_num = 1
        logging.info("Starting all jobs...")
        problem_jobs = []
        for one_job in self.job_list:
            logging.info(
                f"Running job {current_job_num} of {total_jobs} "
                f"with params: {one_job.job_configs.model_dump()}"
            )
            try:
                one_job.run_job()
            except Exception as e:
                logging.error(
                    f"There was a problem processing job {current_job_num} of "
                    f"{total_jobs}. Skipping for now. "
                    f"Error: {e}"
                )
                problem_jobs.append(
                    f"There was a problem processing job {current_job_num} of "
                    f"{total_jobs}. Error: {e}"
                )
            logging.info(
                f"Finished job {current_job_num} of {total_jobs} "
                f"with params: {one_job.job_configs.model_dump()}"
            )
            current_job_num += 1
        logging.info("Finished all jobs!")
        if len(problem_jobs) > 0:
            logging.error("There were errors processing the following jobs: ")
            for problem_job in problem_jobs:
                logging.error(problem_job)


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
