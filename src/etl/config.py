from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os

from dotenv import load_dotenv


load_dotenv()


def _to_bool(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "y"}


@dataclass(frozen=True)
class ETLConfig:
    project_root: Path
    books_csv_path: Path
    sqlite_source_db: Path
    web_source_url: str
    web_max_pages: int
    bronze_dir: Path
    silver_dir: Path
    gold_dir: Path
    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    minio_bucket: str
    minio_secure: bool
    minio_prefix: str

    @classmethod
    def from_env(cls) -> "ETLConfig":
        root = Path(os.getenv("PROJECT_ROOT", ".")).resolve()
        return cls(
            project_root=root,
            books_csv_path=(root / os.getenv("BOOKS_CSV_PATH", "Books.csv")).resolve(),
            sqlite_source_db=(root / os.getenv("SQLITE_SOURCE_DB", "data/sources/books_source.db")).resolve(),
            web_source_url=os.getenv("WEB_SOURCE_URL", "https://books.toscrape.com/catalogue/page-1.html"),
            web_max_pages=int(os.getenv("WEB_MAX_PAGES", "3")),
            bronze_dir=(root / os.getenv("BRONZE_DIR", "data/bronze")).resolve(),
            silver_dir=(root / os.getenv("SILVER_DIR", "data/silver")).resolve(),
            gold_dir=(root / os.getenv("GOLD_DIR", "data/gold")).resolve(),
            minio_endpoint=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
            minio_access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            minio_secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
            minio_bucket=os.getenv("MINIO_BUCKET", "lakehouse"),
            minio_secure=_to_bool(os.getenv("MINIO_SECURE", "false")),
            minio_prefix=os.getenv("MINIO_PREFIX", "curated"),
        )

