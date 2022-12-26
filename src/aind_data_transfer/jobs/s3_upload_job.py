from aind_data_transfer.transformations.metadata_creation import (
    DataDescriptionMetadata,
    SubjectMetadata,
)

import logging
import json
import sys
import argparse
import datetime
import tempfile
import os
from aind_codeocean_api.codeocean import CodeOceanClient
from aind_data_transfer.util.s3_utils import upload_to_s3, get_secret


class GenericS3UploadJob:

    upload_endpoints_keys = {"upload_endpoints": ["metadata_service_url",
                                                  "codeocean_domain",
                                                  "codeocean_trigger_capsule"]}

    def run_job(self,
                args: dict,
                dryrun: bool = False,
                s3_region="us-west-2") -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "-r", "--raw-data-source", required=True, type=str
        )
        parser.add_argument(
            "-b", "--s3-bucket", required=True, type=str
        )
        parser.add_argument(
            "-s", "--subject-id", required=True, type=str
        )
        parser.add_argument(
            "-m", "--modality", required=True, type=str
        )
        parser.add_argument(
            "-d", "--acq-date", required=True, type=str
        )
        parser.add_argument(
            "-t", "--acq-time", required=True, type=str
        )
        parser.add_argument(
            "-e", "--endpoints", required=False, type=str
        )

        job_args = parser.parse_args(args)

        s3_prefix = self.create_s3_prefix(job_args.modality,
                                          job_args.subject_id,
                                          job_args.acq_date,
                                          job_args.acq_time)
        data_prefix = "/".join([s3_prefix, job_args.modality])

        upload_to_s3(directory_to_upload=job_args.raw_data_soucre,
                     s3_bucket=job_args.s3_bucket,
                     s3_prefix=data_prefix,
                     dryrun=dryrun)

        endpoints = job_args.get("endpoints")
        if not endpoints:
            s3_secret_name = list(self.upload_endpoints_keys.keys())[0]
            get_secret(s3_secret_name, s3_region)
            endpoints = json.loads(get_secret(s3_secret_name, s3_region))

        # Create subject.json file if metadata service url is provided
        metadata_service_url = endpoints.get("metadata_service_url")
        if metadata_service_url:
            subject_metadata = (
                SubjectMetadata.ephys_job_to_subject(
                    metadata_service_url=metadata_service_url,
                    subject_id=job_args.subect_id)
            )
            subject_s3_prefix = SubjectMetadata.output_file_name
            with tempfile.NamedTemporaryFile() as tmp:
                tmp.write(json.dumps(subject_metadata, indent=4))
                upload_to_s3(directory_to_upload=tmp.name,
                             s3_bucket=job_args.s3_bucket,
                             s3_prefix=subject_s3_prefix,
                             dryrun=dryrun)

        # Create data description file
        data_description_metadata = (
            DataDescriptionMetadata.get_data_description(name=s3_prefix)
        )
        with tempfile.NamedTemporaryFile() as tmp:
            json_contents = data_description_metadata.json(indent=4)
            tmp.write(json_contents)
            data_desc_s3_prefix = DataDescriptionMetadata.output_file_name
            upload_to_s3(directory_to_upload=tmp.name,
                         s3_bucket=job_args.s3_bucket,
                         s3_prefix=data_desc_s3_prefix,
                         dryrun=dryrun)

        # Register to code ocean if url is provided
        codeocean_url = endpoints.get("codeocean_url")
        if codeocean_url:
            logging.info("Triggering capsule run.")
            capsule_id = endpoints.get("codeocean_trigger_capsule")
            co_api_token = os.getenv("CODEOCEAN_API_TOKEN")
            if co_api_token is None:
                token_key_val = json.loads(
                    get_secret("codeocean-api-token", "us-west-2"))
                co_api_token = token_key_val["CODEOCEAN_READWRITE_TOKEN"]
            co_domain = endpoints.get("codeocean_domain")
            co_client = CodeOceanClient(domain=co_domain, token=co_api_token)
            run_response = co_client.run_capsule(
                capsule_id=capsule_id,
                data_assets=[],
                parameters=[],
            )
            logging.debug(f"Run response: {run_response.json()}")

    @staticmethod
    def create_s3_prefix(modality: str,
                         subject_id: str,
                         acq_date: str,
                         acq_time: str) -> str:
        # Validate date and time strings
        try:
            datetime.datetime.strptime(acq_date + " " + acq_time,
                                       '%Y-%m-%d %H-%M-%S')
        except ValueError:
            raise ValueError("Incorrect data format, acq_date should be "
                             "yyyy-MM-dd and acq_time should be HH-mm-SS")

        return "_".join([modality, subject_id, acq_date, acq_time])


# python -m aind_data_transfer.upload --subject 12345 --modality ecephys
# --acq-date YYYY-MM-DD --acq-time HH-MM-SS <data_folder> <bucket>
# <modality>_<subject_id>_<acq-date>_<acq-time>

if __name__ == "__main__":
    sys_args = sys.argv[1:]
    GenericS3UploadJob.run_job(sys_args)
