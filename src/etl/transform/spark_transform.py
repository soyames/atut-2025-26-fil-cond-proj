from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import platform
import re
import shutil

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from py4j.protocol import Py4JJavaError

from src.etl.config import ETLConfig
from src.etl.logging_utils import get_logger


LOGGER = get_logger(__name__)
CURATED_COLUMNS = [
    "record_source",
    "title",
    "isbn",
    "author",
    "year",
    "publisher",
    "publisher_country",
    "language",
    "category",
    "upc",
    "product_type",
    "price_gbp",
    "price_excl_tax_gbp",
    "price_incl_tax_gbp",
    "tax_gbp",
    "rating",
    "availability",
    "availability_count",
    "num_reviews",
    "description",
    "product_page_url",
    "source_page",
    "image_url_s",
    "image_url_m",
    "image_url_l",
    "ingestion_date",
]
WEB_COLUMNS = [
    "title",
    "price_gbp",
    "rating",
    "availability",
    "availability_count",
    "category",
    "upc",
    "product_type",
    "price_excl_tax_gbp",
    "price_incl_tax_gbp",
    "tax_gbp",
    "num_reviews",
    "description",
    "product_page_url",
    "source_page",
    "ingestion_date",
]


def normalize_year(value: str | int | None) -> int | None:
    if value is None:
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    current_year = datetime.now(timezone.utc).year + 1
    if 0 < parsed <= current_year:
        return parsed
    return None


def clean_books_csv(df: DataFrame) -> DataFrame:
    normalize_year_udf = F.udf(normalize_year, "int")
    return (
        df.withColumnRenamed("ISBN", "isbn")
        .withColumnRenamed("Book-Title", "title")
        .withColumnRenamed("Book-Author", "author")
        .withColumnRenamed("Year-Of-Publication", "year")
        .withColumnRenamed("Publisher", "publisher")
        .withColumn("year", normalize_year_udf(F.col("year")))
        .withColumn("publisher", F.coalesce(F.col("publisher"), F.lit("Unknown")))
        .dropDuplicates(["isbn"])
    )


def _as_file_uri(path: Path) -> str:
    return path.resolve().as_uri()


def _is_windows_nativeio_error(error: Exception) -> bool:
    return "NativeIO$Windows.access0" in str(error)


def _sanitize_partition_value(value: object) -> str:
    raw = "Unknown" if pd.isna(value) else str(value)
    sanitized = re.sub(r"[<>:\"/\\\\|?*]+", "_", raw).strip().rstrip(".")
    return sanitized or "Unknown"


def _write_partitioned_csv(df: pd.DataFrame, output_root: Path, partitions: list[str]) -> None:
    output_root.mkdir(parents=True, exist_ok=True)
    grouped = df.groupby(partitions, dropna=False)
    for keys, partition_df in grouped:
        keys = keys if isinstance(keys, tuple) else (keys,)
        partition_path = output_root
        for col, value in zip(partitions, keys):
            safe = _sanitize_partition_value(value)
            partition_path = partition_path / f"{col}={safe}"
        partition_path.mkdir(parents=True, exist_ok=True)
        partition_df.to_csv(partition_path / "part-00000.csv", index=False)


def _normalize_web_dataframe_pandas(df: pd.DataFrame) -> pd.DataFrame:
    normalized = df.copy()
    for column in WEB_COLUMNS:
        if column not in normalized.columns:
            normalized[column] = "" if column not in {"availability_count", "num_reviews"} else 0
    normalized["availability_count"] = pd.to_numeric(normalized["availability_count"], errors="coerce").fillna(0).astype(int)
    normalized["num_reviews"] = pd.to_numeric(normalized["num_reviews"], errors="coerce").fillna(0).astype(int)
    return normalized[WEB_COLUMNS]


def _project_books_curated_pandas(df: pd.DataFrame) -> pd.DataFrame:
    projected = df.copy()
    projected["record_source"] = "csv_sql"
    defaults = {
        "category": "Unknown",
        "upc": "",
        "product_type": "",
        "price_gbp": 0.0,
        "price_excl_tax_gbp": 0.0,
        "price_incl_tax_gbp": 0.0,
        "tax_gbp": 0.0,
        "rating": "",
        "availability": "",
        "availability_count": 0,
        "num_reviews": 0,
        "description": "",
        "product_page_url": "",
        "source_page": "",
    }
    for column, value in defaults.items():
        if column not in projected.columns:
            projected[column] = value
    for image_col, src_col in (
        ("image_url_s", "Image-URL-S"),
        ("image_url_m", "Image-URL-M"),
        ("image_url_l", "Image-URL-L"),
    ):
        if src_col in projected.columns and image_col not in projected.columns:
            projected[image_col] = projected[src_col]
        elif image_col not in projected.columns:
            projected[image_col] = ""
    projected["availability_count"] = pd.to_numeric(projected["availability_count"], errors="coerce").fillna(0).astype(int)
    projected["num_reviews"] = pd.to_numeric(projected["num_reviews"], errors="coerce").fillna(0).astype(int)
    return projected[CURATED_COLUMNS]


def _project_web_curated_pandas(df: pd.DataFrame) -> pd.DataFrame:
    projected = df.copy()
    projected["record_source"] = "web"
    defaults = {
        "isbn": "",
        "author": "",
        "year": None,
        "publisher": "",
        "publisher_country": "",
        "language": "",
        "image_url_s": "",
        "image_url_m": "",
        "image_url_l": "",
    }
    for column, value in defaults.items():
        if column not in projected.columns:
            projected[column] = value
    projected["availability_count"] = pd.to_numeric(projected["availability_count"], errors="coerce").fillna(0).astype(int)
    projected["num_reviews"] = pd.to_numeric(projected["num_reviews"], errors="coerce").fillna(0).astype(int)
    return projected[CURATED_COLUMNS]


def _normalize_web_dataframe_spark(df: DataFrame) -> DataFrame:
    normalized = df
    for column in WEB_COLUMNS:
        if column not in normalized.columns:
            if column in {"availability_count", "num_reviews"}:
                normalized = normalized.withColumn(column, F.lit(0))
            else:
                normalized = normalized.withColumn(column, F.lit(""))
    return normalized.select(*WEB_COLUMNS)


def _project_books_curated_spark(df: DataFrame) -> DataFrame:
    projected = (
        df.withColumn("record_source", F.lit("csv_sql"))
        .withColumn("category", F.lit("Unknown"))
        .withColumn("upc", F.lit(""))
        .withColumn("product_type", F.lit(""))
        .withColumn("price_gbp", F.lit(0.0))
        .withColumn("price_excl_tax_gbp", F.lit(0.0))
        .withColumn("price_incl_tax_gbp", F.lit(0.0))
        .withColumn("tax_gbp", F.lit(0.0))
        .withColumn("rating", F.lit(""))
        .withColumn("availability", F.lit(""))
        .withColumn("availability_count", F.lit(0))
        .withColumn("num_reviews", F.lit(0))
        .withColumn("description", F.lit(""))
        .withColumn("product_page_url", F.lit(""))
        .withColumn("source_page", F.lit(""))
        .withColumn("image_url_s", F.coalesce(F.col("Image-URL-S"), F.lit("")))
        .withColumn("image_url_m", F.coalesce(F.col("Image-URL-M"), F.lit("")))
        .withColumn("image_url_l", F.coalesce(F.col("Image-URL-L"), F.lit("")))
    )
    return projected.select(*CURATED_COLUMNS)


def _project_web_curated_spark(df: DataFrame) -> DataFrame:
    projected = (
        df.withColumn("record_source", F.lit("web"))
        .withColumn("isbn", F.lit(""))
        .withColumn("author", F.lit(""))
        .withColumn("year", F.lit(0))
        .withColumn("publisher", F.lit(""))
        .withColumn("publisher_country", F.lit(""))
        .withColumn("language", F.lit(""))
        .withColumn("image_url_s", F.lit(""))
        .withColumn("image_url_m", F.lit(""))
        .withColumn("image_url_l", F.lit(""))
    )
    return projected.select(*CURATED_COLUMNS)


def _transform_windows_fallback(config: ETLConfig) -> None:
    LOGGER.info("Using Windows fallback transform (pandas)")
    books_csv = pd.concat(
        (pd.read_csv(p, low_memory=False) for p in (config.bronze_dir / "books_csv").glob("*.csv")),
        ignore_index=True,
    )
    sql_csv = pd.concat((pd.read_csv(p) for p in (config.bronze_dir / "books_sql").glob("*.csv")), ignore_index=True)
    web_json = pd.concat((pd.read_json(p, lines=True) for p in (config.bronze_dir / "books_web").glob("*.jsonl")), ignore_index=True)
    web_json = _normalize_web_dataframe_pandas(web_json)

    books_csv = books_csv.rename(
        columns={
            "ISBN": "isbn",
            "Book-Title": "title",
            "Book-Author": "author",
            "Year-Of-Publication": "year",
            "Publisher": "publisher",
        }
    )
    books_csv["year"] = books_csv["year"].apply(normalize_year)
    books_csv["publisher"] = books_csv["publisher"].fillna("Unknown")
    books_csv = books_csv.drop_duplicates(subset=["isbn"])

    sql_csv = sql_csv[["isbn", "publisher_country", "language"]].drop_duplicates(subset=["isbn"])
    enriched = books_csv.merge(sql_csv, on="isbn", how="left")
    enriched["publisher_country"] = enriched["publisher_country"].fillna("Unknown")
    enriched["language"] = enriched["language"].fillna("Unknown")
    enriched = _project_books_curated_pandas(enriched)
    web_json = _project_web_curated_pandas(web_json)

    books_output = config.silver_dir / "books_curated"
    web_output = config.silver_dir / "web_catalog_curated"
    shutil.rmtree(books_output, ignore_errors=True)
    shutil.rmtree(web_output, ignore_errors=True)
    _write_partitioned_csv(enriched, books_output, ["ingestion_date"])
    _write_partitioned_csv(web_json, web_output, ["ingestion_date"])
    LOGGER.info(f"Windows fallback transformation completed: {books_output} and {web_output}")


def transform_and_write(config: ETLConfig) -> None:
    config.silver_dir.mkdir(parents=True, exist_ok=True)
    csv_path = config.bronze_dir / "books_csv"
    sql_path = config.bronze_dir / "books_sql"
    web_path = config.bronze_dir / "books_web"
    for source_path in (csv_path, sql_path, web_path):
        if not source_path.exists():
            raise FileNotFoundError(f"Missing expected input directory: {source_path}")

    spark = (
        SparkSession.builder.appName("industrial-etl-transform")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    LOGGER.info("Spark transformation started")

    try:
        csv_df = spark.read.option("header", True).csv(_as_file_uri(csv_path))
        sql_df = spark.read.option("header", True).csv(_as_file_uri(sql_path))
        web_df = spark.read.json(_as_file_uri(web_path))
        web_df = _normalize_web_dataframe_spark(web_df)
    except Py4JJavaError as error:
        spark.stop()
        if platform.system() == "Windows" and _is_windows_nativeio_error(error):
            _transform_windows_fallback(config)
            return
        raise

    books_clean = clean_books_csv(csv_df)
    sql_clean = sql_df.select("isbn", "publisher_country", "language").dropDuplicates(["isbn"])

    enriched_books = (
        books_clean.join(sql_clean, on="isbn", how="left")
        .withColumn("publisher_country", F.coalesce(F.col("publisher_country"), F.lit("Unknown")))
        .withColumn("language", F.coalesce(F.col("language"), F.lit("Unknown")))
    )
    enriched_books = _project_books_curated_spark(enriched_books)
    web_df = _project_web_curated_spark(web_df)

    books_output = config.silver_dir / "books_curated"
    web_output = config.silver_dir / "web_catalog_curated"

    (
        enriched_books.write.mode("overwrite")
        .partitionBy("ingestion_date")
        .parquet(str(books_output))
    )
    (
        web_df.write.mode("overwrite")
        .partitionBy("ingestion_date")
        .parquet(str(web_output))
    )

    LOGGER.info(f"Spark transformation completed: {books_output} and {web_output}")
    spark.stop()

