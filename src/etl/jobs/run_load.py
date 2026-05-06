from __future__ import annotations

from src.etl.config import ETLConfig
from src.etl.load.minio_loader import upload_directory_to_minio


def run_load_pipeline() -> None:
    config = ETLConfig.from_env()
    upload_directory_to_minio(config, config.silver_dir)


if __name__ == "__main__":  # pragma: no cover
    run_load_pipeline()

