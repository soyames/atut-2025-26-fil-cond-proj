from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

from src.etl.logging_utils import get_logger


LOGGER = get_logger(__name__)


def extract_books_csv(source_csv: Path, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"books_csv_{date.today()}.csv"
    LOGGER.info(f"Reading CSV source: {source_csv}")

    data = pd.read_csv(source_csv, low_memory=False)
    data["ingestion_date"] = str(date.today())
    data.to_csv(output_path, index=False)

    LOGGER.info(f"CSV extraction completed: {output_path}")
    return output_path

