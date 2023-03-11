import argparse
import json

from aind_data_transfer.config_loader.base_config import (
    JobEndpoints,
    JobSecrets,
)


class GenericJobConfigurationLoader:
    def __init__(self, args: list):
        self.configs = self._load_configs(args)

    @staticmethod
    def _load_configs(args: list) -> argparse.Namespace:
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

        help_message_csv_file = (
            "Path to csv file with list of job configs. The csv file needs "
            "to have the headers: data-source, s3-bucket, subject-id, "
            "modality, acq-date, acq-time. Optional headers: behavior-dir, "
            "metadata-dir."
        )

        parser = argparse.ArgumentParser(
            description=description,
            epilog=additional_info,
            formatter_class=argparse.RawDescriptionHelpFormatter,
        )

        # Check if user has selected to use a jobs_csv_file
        parser.add_argument(
            "-j",
            "--jobs-csv-file",
            required=False,
            type=str,
            help=help_message_csv_file,
        )
        parser.add_argument("--dry-run", action="store_true")
        parser.add_argument("--metadata-dir-force", action="store_true")
        parser.add_argument("--compress-data", action="store_true")
        parser.set_defaults(dry_run=False)
        parser.set_defaults(metadata_dir_force=False)
        parser.set_defaults(compress_data=False)
        job_args = parser.parse_args(args)

        if job_args.jobs_csv_file is not None:
            return job_args

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
        parser.add_argument("-z", "--secrets", required=False, type=json.loads)
        parser.add_argument(
            "-c", "--extra-config-source", required=False, type=str
        )
        job_args = parser.parse_args(args)
        return job_args

    def _resolve_endpoints(self):
        if self.configs.service_endpoints is not None:
            self.configs.service_endpoints = JobEndpoints.from_dict(
                self.configs.service_endpoints
            )
        else:
            self.configs.service_endpoints = JobEndpoints()

    def _resolve_secrets(self):
        if self.configs.secrets is not None:
            self.configs.secrets = JobSecrets.from_dict(self.configs.secrets)
        else:
            self.configs.secrets = JobSecrets()
