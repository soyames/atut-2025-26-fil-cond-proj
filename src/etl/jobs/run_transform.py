from __future__ import annotations

from src.etl.config import ETLConfig
from src.etl.transform.spark_transform import transform_and_write


def run_transform_pipeline() -> None:
    config = ETLConfig.from_env()
    transform_and_write(config)


if __name__ == "__main__":  # pragma: no cover
    run_transform_pipeline()

