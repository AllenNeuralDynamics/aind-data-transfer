import argparse
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
    DataDescriptionMetadata,
    SubjectMetadata,
)
from aind_data_transfer.util.s3_utils import (
    copy_to_s3,
    get_secret,
    upload_to_s3,
)


class GenericS3UploadJob:

    SERVICE_ENDPOINT_KEY = "service_endpoints"
    METADATA_SERVICE_URL_KEY = "metadata_service_url"
    CODEOCEAN_DOMAIN_KEY = "codeocean_domain"
    CODEOCEAN_CAPSULE_KEY = "codeocean_trigger_capsule"
    CODEOCEAN_TOKEN_KEY = "codeocean-api-token"
    CODEOCEAN_READ_WRITE_KEY = "CODEOCEAN_READWRITE_TOKEN"
    S3_DEFAULT_REGION = "us-west-2"

    def __init__(self, args: list):
        self.args = args
        self.configs = self._load_configs(args)

    def _get_endpoints(self, job_args, s3_region) -> Optional[dict]:
        endpoints = getattr(job_args, self.SERVICE_ENDPOINT_KEY)
        if not endpoints:
            try:
                s3_secret_name = self.SERVICE_ENDPOINT_KEY
                get_secret(s3_secret_name, s3_region)
                endpoints = json.loads(get_secret(s3_secret_name, s3_region))
            except ClientError as e:
                logging.warning(
                    f"Unable to retrieve aws secret: "
                    f"{self.SERVICE_ENDPOINT_KEY}"
                )
                logging.debug(e.response)
                endpoints = {}
        return endpoints

    @staticmethod
    def _upload_subject_metadata(
        metadata_service_url, subject_id, s3_bucket, s3_prefix, dry_run
    ) -> None:
        if metadata_service_url:
            subject_metadata = SubjectMetadata.ephys_job_to_subject(
                metadata_service_url=metadata_service_url,
                subject_id=subject_id,
            )
            file_name = SubjectMetadata.output_file_name
            final_s3_prefix = "/".join([s3_prefix, file_name])
            with tempfile.NamedTemporaryFile(mode="w") as tmp:
                tmp.write(json.dumps(subject_metadata, indent=4))
                copy_to_s3(
                    file_to_upload=tmp.name,
                    s3_bucket=s3_bucket,
                    s3_prefix=final_s3_prefix,
                    dryrun=dry_run,
                )
        else:
            logging.warning(
                "No metadata service url given. "
                "Not able to get subject metadata."
            )

    @staticmethod
    def _upload_data_description_metadata(
        s3_bucket, s3_prefix, dry_run
    ) -> None:
        data_description_metadata = (
            DataDescriptionMetadata.get_data_description(name=s3_prefix)
        )
        file_name = DataDescriptionMetadata.output_file_name
        final_s3_prefix = "/".join([s3_prefix, file_name])
        with tempfile.NamedTemporaryFile(mode="w") as tmp:
            json_contents = data_description_metadata.json(indent=4)
            tmp.write(json_contents)
            copy_to_s3(
                file_to_upload=tmp.name,
                s3_bucket=s3_bucket,
                s3_prefix=final_s3_prefix,
                dryrun=dry_run,
            )

    def _get_codeocean_client(self, s3_region, codeocean_domain):
        # Try to see if it's been set by an env var
        co_api_token = os.getenv(
            f"{self.CODEOCEAN_TOKEN_KEY.replace('_', '-').upper()}"
        )
        # If not set by an env var, check if it's stored in aws secrets
        if co_api_token is None:
            try:
                s3_secret_name = self.CODEOCEAN_TOKEN_KEY
                get_secret(s3_secret_name, s3_region)
                token_key_val = json.loads(
                    get_secret(s3_secret_name, s3_region)
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

    @staticmethod
    def _trigger_codeocean_capsule(codeocean_client, capsule_id, dry_run):
        if codeocean_client and capsule_id:
            logging.info("Triggering capsule run.")
            if not dry_run:
                run_response = codeocean_client.run_capsule(
                    capsule_id=capsule_id,
                    data_assets=[],
                    parameters=[],
                )
                logging.debug(f"Run response: {run_response.json()}")
            else:
                codeocean_client.get_capsule(capsule_id=capsule_id)
                logging.info(
                    f"Would have ran capsule: {capsule_id} "
                    f"at {codeocean_client.domain}"
                )
        else:
            logging.warning(
                "CodeOcean endpoints are required to trigger capsule."
            )

    @staticmethod
    def _create_s3_prefix(
        modality: str, subject_id: str, acq_date: str, acq_time: str
    ) -> str:
        # Validate date and time strings
        try:
            datetime.datetime.strptime(
                acq_date + " " + acq_time, "%Y-%m-%d %H-%M-%S"
            )
        except ValueError:
            raise ValueError(
                "Incorrect data format, acq_date should be "
                "yyyy-MM-dd and acq_time should be HH-mm-SS"
            )

        return "_".join([modality, subject_id, acq_date, acq_time])

    def _load_configs(self, args: list) -> argparse.Namespace:
        parser = argparse.ArgumentParser()
        parser.add_argument("-d", "--data-source", required=True, type=str)
        parser.add_argument("-b", "--s3-bucket", required=True, type=str)
        parser.add_argument("-s", "--subject-id", required=True, type=str)
        parser.add_argument("-m", "--modality", required=True, type=str)
        parser.add_argument("-a", "--acq-date", required=True, type=str)
        parser.add_argument("-t", "--acq-time", required=True, type=str)
        service_endpoints_name = self.SERVICE_ENDPOINT_KEY.replace("_", "-")
        parser.add_argument(
            "-e", f"--{service_endpoints_name}", required=False, type=str
        )
        parser.add_argument("-r", "--s3-region", required=False, type=str)
        parser.add_argument("--dry-run", action="store_true")
        parser.set_defaults(dry_run=False)
        parser.set_defaults(s3_region=self.S3_DEFAULT_REGION)

        job_args = parser.parse_args(args)
        return job_args

    def run_job(self) -> None:

        job_args = self._load_configs(self.args)
        s3_region = job_args.s3_region

        s3_prefix = self._create_s3_prefix(
            job_args.modality,
            job_args.subject_id,
            job_args.acq_date,
            job_args.acq_time,
        )
        data_prefix = "/".join([s3_prefix, job_args.modality])

        upload_to_s3(
            directory_to_upload=job_args.data_source,
            s3_bucket=job_args.s3_bucket,
            s3_prefix=data_prefix,
            dryrun=job_args.dry_run,
        )

        endpoints = self._get_endpoints(job_args, s3_region)

        # Create subject.json file if metadata service url is provided
        self._upload_subject_metadata(
            metadata_service_url=(
                endpoints.get(self.METADATA_SERVICE_URL_KEY)
            ),
            subject_id=job_args.subject_id,
            s3_bucket=job_args.s3_bucket,
            s3_prefix=s3_prefix,
            dry_run=job_args.dry_run,
        )

        # Create data description file
        self._upload_data_description_metadata(
            s3_bucket=job_args.s3_bucket,
            s3_prefix=s3_prefix,
            dry_run=job_args.dry_run,
        )

        # Register to code ocean if url is provided
        codeocean_client = self._get_codeocean_client(
            s3_region=s3_region,
            codeocean_domain=endpoints.get("codeocean_domain"),
        )
        self._trigger_codeocean_capsule(
            codeocean_client=codeocean_client,
            capsule_id=endpoints.get(self.CODEOCEAN_CAPSULE_KEY),
            dry_run=job_args.dry_run,
        )

if __name__ == "__main__":
    sys_args = sys.argv[1:]
    job = GenericS3UploadJob(sys_args)
    job.run_job()
