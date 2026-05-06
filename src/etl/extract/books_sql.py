from __future__ import annotations

from datetime import date
from pathlib import Path
import sqlite3

import pandas as pd

from src.etl.logging_utils import get_logger


LOGGER = get_logger(__name__)


def extract_books_sql(sqlite_db: Path, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"books_sql_{date.today()}.csv"
    LOGGER.info(f"Reading SQL source: {sqlite_db}")

    query = """
    SELECT
        bm.isbn,
        bm.publisher,
        pm.publisher_country,
        pm.language
    FROM books_metadata bm
    LEFT JOIN publisher_metadata pm
        ON bm.publisher = pm.publisher
    """
    with sqlite3.connect(sqlite_db) as conn:
        df = pd.read_sql_query(query, conn)
    df["ingestion_date"] = str(date.today())
    df.to_csv(output_path, index=False)

    LOGGER.info(f"SQL extraction completed: {output_path}")
    return output_path

