import logging
import platform
import subprocess

import boto3
from botocore.exceptions import ClientError


def get_secret(secret_name, region_name):
    """Retrieves a secret"""

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name="secretsmanager", region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/
        #   API_GetSecretValue.html
        raise e

    # Decrypts secret using the associated KMS key.
    # Don't log or print!
    secret = get_secret_value_response["SecretString"]

    return secret


def upload_to_s3(
    directory_to_upload,
    s3_bucket,
    s3_prefix,
    dryrun=False,
    excluded=None,
    included=None,
):
    # Upload to s3
    if platform.system() == "Windows":
        shell = True
    else:
        shell = False

    # TODO: Use s3transfer library instead of subprocess?
    logging.info("Uploading to s3.")
    aws_dest = f"s3://{s3_bucket}/{s3_prefix}"
    base_command = ["aws", "s3", "sync", str(directory_to_upload), aws_dest]

    if excluded:
        base_command.extend(["--exclude", str(excluded)])
    if included:
        base_command.extend(["--include", str(included)])
    if dryrun:
        base_command.append("--dryrun")
    subprocess.run(base_command, shell=shell, check=True)

    logging.info("Finished uploading to s3.")


def copy_to_s3(file_to_upload, s3_bucket, s3_prefix, dryrun):
    # Upload to s3
    if platform.system() == "Windows":
        shell = True
    else:
        shell = False

    # TODO: Use s3transfer library instead of subprocess?
    logging.info("Copying file to s3.")
    aws_dest = f"s3://{s3_bucket}/{s3_prefix}"
    if dryrun:
        subprocess.run(
            [
                "aws",
                "s3",
                "cp",
                str(file_to_upload),
                aws_dest,
                "--dryrun",
            ],
            shell=shell,
            check=True,
        )
    else:
        subprocess.run(
            ["aws", "s3", "cp", str(file_to_upload), aws_dest],
            shell=shell,
            check=True,
        )
    logging.info("Finished copying file to s3.")
