import logging
import os

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv


load_dotenv()


def get_minio_url(url: str, bucket: str, key: str) -> str:
    '''Returns location of given resource in minio once it is saved'''
    return url + "/" + bucket + "/" + key


def list_log_files(prefix: str) -> list:
    client = get_s3_client()
    bucket = os.getenv('MINIO_BUCKET')
    log_keys = [log['Key'] for log in client.list_objects(Bucket=bucket).get('Contents', []) if log['Key'].startswith(prefix)]
    return log_keys


def get_s3_client() -> boto3.client:
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_URL"),
        aws_access_key_id=os.getenv("MINIO_USER"),
        aws_secret_access_key=os.getenv("MINIO_PWD"),
        config=Config(signature_version="s3v4"),
    )


def download_from_minio(key: str, filepath: str) -> None:
    logging.info(f"Downloading from minio: {key} to: {filepath}")
    s3 = get_s3_client()
    try:
        s3.download_file(os.getenv('MINIO_BUCKET'), key, filepath)
        logging.info(
            f"Resource downloaded from minio at {get_minio_url(os.getenv('MINIO_URL'), os.getenv('MINIO_BUCKET'), key)}"
        )
    except ClientError as e:
        logging.error(e)
