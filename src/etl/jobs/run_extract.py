from __future__ import annotations

from src.etl.config import ETLConfig
from src.etl.extract.books_csv import extract_books_csv
from src.etl.extract.books_sql import extract_books_sql
from src.etl.extract.books_web import extract_books_web
from src.etl.logging_utils import get_logger


LOGGER = get_logger(__name__)


def run_extract_pipeline() -> None:
    config = ETLConfig.from_env()
    extract_books_csv(config.books_csv_path, config.bronze_dir / "books_csv")
    extract_books_sql(config.sqlite_source_db, config.bronze_dir / "books_sql")
    extract_books_web(config.web_source_url, config.web_max_pages, config.bronze_dir / "books_web")
    LOGGER.info("All extraction tasks completed")


if __name__ == "__main__":  # pragma: no cover
    run_extract_pipeline()

