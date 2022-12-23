from aind_data_transfer.transformations.metadata_creation import (
    ProcessingMetadata,
    SubjectMetadata,
)

import logging
import json
import sys
import argparse


class GenericUploadJob:

    @staticmethod
    def attach_subject_info(subject_id,
                            metadata_url,
                            dest_data_dir) -> None:
        """Writes subject metadata a destination directory"""
        # Subject metadata
        logging.info("Creating subject.json file.")
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

    @staticmethod
    def attach_processing_info(dest_data_dir,
                               start_date_time,
                               end_date_time,
                               input_location,
                               output_location,
                               code_url,
                               parameters) -> None:
        """Writes processing metadata a destination directory"""

        # Processing metadata
        logging.info("Creating processing.json file.")
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


def run_job(args):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-r", "--raw-data-source", required=False, type=str
    )
    parser.add_argument(
        "-b", "--s3-bucket", required=False, type=str
    )
    parser.add_argument(
        "-p", "--s3-prefix", required=False, type=str
    )
    parser.add_argument(
        "-s", "--subject_id", required=False, type=str
    )
    job_args = parser.parse_args(args)


if __name__ == "__main__":
    sys_args = sys.argv[1:]
    run_job(sys_args)

