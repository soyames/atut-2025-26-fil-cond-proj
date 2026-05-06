from __future__ import annotations

from pathlib import Path

import boto3

from src.etl.config import ETLConfig
from src.etl.logging_utils import get_logger


LOGGER = get_logger(__name__)


def upload_directory_to_minio(config: ETLConfig, local_dir: Path) -> None:
    session = boto3.session.Session()
    s3 = session.client(
        "s3",
        endpoint_url=config.minio_endpoint,
        aws_access_key_id=config.minio_access_key,
        aws_secret_access_key=config.minio_secret_key,
    )

    try:
        s3.head_bucket(Bucket=config.minio_bucket)
    except Exception:
        s3.create_bucket(Bucket=config.minio_bucket)

    for file_path in local_dir.rglob("*"):
        if not file_path.is_file():
            continue
        rel = file_path.relative_to(local_dir).as_posix()
        key = f"{config.minio_prefix}/{rel}"
        s3.upload_file(str(file_path), config.minio_bucket, key)
        LOGGER.info(f"Uploaded {file_path} -> s3://{config.minio_bucket}/{key}")

