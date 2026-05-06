from __future__ import annotations

import logging
from pathlib import Path
import sys

import pytest

from src.etl.extract import books_web
from src.etl.logging_utils import JsonFormatter, get_logger
from src.etl.transform import spark_transform


def test_parse_books_html_invalid_price() -> None:
    html = """
    <article class="product_pod">
      <h3><a href="../../../broken/index.html" title="Broken Price"></a></h3>
      <p class="price_color">N/A</p>
      <p class="instock availability">In stock</p>
      <p class="star-rating Three"></p>
    </article>
    """
    with pytest.raises(ValueError):
        books_web.parse_books_html(html, "http://example/page-1.html")


def test_extract_books_web_writes_jsonl(monkeypatch, tmp_path: Path) -> None:
    listing_html = """
    <article class="product_pod">
      <h3><a href="../../../book-a/index.html" title="Book A"></a></h3>
      <p class="price_color">Â51.77</p>
      <p class="instock availability">In stock</p>
      <p class="star-rating Five"></p>
    </article>
    """
    detail_html = """
    <ul class="breadcrumb"><li><a>Home</a></li><li><a>Books</a></li><li><a>Poetry</a></li></ul>
    <div id="product_description"></div><p>Description</p>
    <table class="table table-striped">
      <tr><th>UPC</th><td>u1</td></tr>
      <tr><th>Product Type</th><td>Books</td></tr>
      <tr><th>Price (excl. tax)</th><td>£51.77</td></tr>
      <tr><th>Price (incl. tax)</th><td>£51.77</td></tr>
      <tr><th>Tax</th><td>£0.00</td></tr>
      <tr><th>Availability</th><td>In stock (22 available)</td></tr>
      <tr><th>Number of reviews</th><td>0</td></tr>
    </table>
    """

    class _Resp:
        def __init__(self, text: str):
            self.text = text

        @staticmethod
        def raise_for_status() -> None:
            return None

    def _fake_get(url, **_kwargs):
        if "page-" in url:
            return _Resp(listing_html)
        return _Resp(detail_html)

    monkeypatch.setattr(books_web.requests, "get", _fake_get)
    out = books_web.extract_books_web(
        "https://books.toscrape.com/catalogue/page-1.html",
        2,
        tmp_path,
    )
    lines = out.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 2
    assert '"price_gbp": 51.77' in lines[0]
    assert '"category": "Poetry"' in lines[0]


def test_json_formatter_with_exception() -> None:
    formatter = JsonFormatter()
    exc_info = None
    try:
        raise RuntimeError("boom")
    except RuntimeError:
        exc_info = sys.exc_info()
    record = logging.getLogger("x").makeRecord(
        "x",
        logging.ERROR,
        __file__,
        1,
        "failed",
        args=(),
        exc_info=exc_info,
    )
    formatted = formatter.format(record)
    assert '"exception"' in formatted


def test_get_logger_reuses_existing_handler() -> None:
    logger = get_logger("unit.logger")
    same = get_logger("unit.logger")
    assert logger is same


def test_as_file_uri(tmp_path: Path) -> None:
    uri = spark_transform._as_file_uri(tmp_path)
    assert uri.startswith("file:///")


def test_clean_books_csv_chain(monkeypatch) -> None:
    import sys
    class _FakeDf:
        def withColumnRenamed(self, *_args, **_kwargs):
            return self

        def withColumn(self, *_args, **_kwargs):
            return self

        def dropDuplicates(self, *_args, **_kwargs):
            return self

    sys.modules['pyspark.sql'].functions.udf = lambda *_args, **_kwargs: (lambda x: x)
    sys.modules['pyspark.sql'].functions.col = lambda *_args, **_kwargs: "col"
    sys.modules['pyspark.sql'].functions.coalesce = lambda *_args, **_kwargs: "coalesce"
    sys.modules['pyspark.sql'].functions.lit = lambda *_args, **_kwargs: "lit"

    result = spark_transform.clean_books_csv(_FakeDf())
    assert isinstance(result, _FakeDf)


def test_transform_and_write_missing_inputs(tmp_path: Path) -> None:
    class _Cfg:
        bronze_dir = tmp_path / "bronze"
        silver_dir = tmp_path / "silver"

    with pytest.raises(FileNotFoundError):
        spark_transform.transform_and_write(_Cfg())


def test_transform_and_write_success(monkeypatch, tmp_path: Path) -> None:
    import sys
    bronze = tmp_path / "bronze"
    (bronze / "books_csv").mkdir(parents=True)
    (bronze / "books_sql").mkdir(parents=True)
    (bronze / "books_web").mkdir(parents=True)
    silver = tmp_path / "silver"

    class _Writer:
        def __init__(self):
            self.output = None

        def mode(self, *_args, **_kwargs):
            return self

        def partitionBy(self, *_args, **_kwargs):
            return self

        def parquet(self, output: str):
            self.output = output
            return None

    class _Df:
        def __init__(self):
            self.write = _Writer()
            self.columns = []

        def select(self, *_args, **_kwargs):
            return self

        def dropDuplicates(self, *_args, **_kwargs):
            return self

        def join(self, *_args, **_kwargs):
            return self

        def withColumn(self, *_args, **_kwargs):
            return self

    class _Reader:
        def option(self, *_args, **_kwargs):
            return self

        def csv(self, *_args, **_kwargs):
            return _Df()

        def json(self, *_args, **_kwargs):
            return _Df()

    class _Builder:
        def appName(self, *_args, **_kwargs):
            return self

        def config(self, *_args, **_kwargs):
            return self

        def getOrCreate(self):
            class _Spark:
                read = _Reader()

                class _SparkContext:
                    @staticmethod
                    def setLogLevel(*_args, **_kwargs):
                        return None

                sparkContext = _SparkContext()

                @staticmethod
                def stop():
                    return None

            return _Spark()

    class _Cfg:
        bronze_dir = bronze
        silver_dir = silver

    monkeypatch.setattr(spark_transform, "clean_books_csv", lambda _df: _Df())
    # Monkeypatch pyspark.sql.functions since it's imported inside the function
    sys.modules['pyspark.sql'].functions.coalesce = lambda *_args, **_kwargs: "coalesce"
    sys.modules['pyspark.sql'].functions.col = lambda *_args, **_kwargs: "col"
    sys.modules['pyspark.sql'].functions.lit = lambda *_args, **_kwargs: "lit"
    monkeypatch.setattr(spark_transform, "_normalize_web_dataframe_spark", lambda df: df)
    # Monkeypatch pyspark.sql.SparkSession since it's imported inside the function
    sys.modules['pyspark.sql'].SparkSession.builder = _Builder()

    spark_transform.transform_and_write(_Cfg())


def test_is_windows_nativeio_error() -> None:
    assert spark_transform._is_windows_nativeio_error(Exception("NativeIO$Windows.access0 failure")) is True
    assert spark_transform._is_windows_nativeio_error(Exception("other")) is False


def test_sanitize_partition_value() -> None:
    assert spark_transform._sanitize_partition_value("Actes Sud / Junior") == "Actes Sud _ Junior"
    assert spark_transform._sanitize_partition_value(None) == "Unknown"


def test_write_partitioned_csv(tmp_path: Path) -> None:
    import pandas as pd

    df = pd.DataFrame(
        [
            {"ingestion_date": "2026-05-06", "publisher": "A", "value": 1},
            {"ingestion_date": "2026-05-06", "publisher": "B", "value": 2},
        ]
    )
    out = tmp_path / "curated"
    spark_transform._write_partitioned_csv(df, out, ["ingestion_date", "publisher"])
    assert (out / "ingestion_date=2026-05-06" / "publisher=A" / "part-00000.csv").exists()
    assert (out / "ingestion_date=2026-05-06" / "publisher=B" / "part-00000.csv").exists()
    single = tmp_path / "single"
    spark_transform._write_partitioned_csv(df, single, ["ingestion_date"])
    assert (single / "ingestion_date=2026-05-06" / "part-00000.csv").exists()


def test_transform_windows_fallback(monkeypatch, tmp_path: Path) -> None:
    bronze = tmp_path / "bronze"
    (bronze / "books_csv").mkdir(parents=True)
    (bronze / "books_sql").mkdir(parents=True)
    (bronze / "books_web").mkdir(parents=True)
    (bronze / "books_csv" / "a.csv").write_text(
        "ISBN,Book-Title,Book-Author,Year-Of-Publication,Publisher,ingestion_date\n1,T,A,2000,P,2026-05-06\n",
        encoding="utf-8",
    )
    (bronze / "books_sql" / "a.csv").write_text(
        "isbn,publisher,publisher_country,language,ingestion_date\n1,P,FR,FR,2026-05-06\n",
        encoding="utf-8",
    )
    (bronze / "books_web" / "a.jsonl").write_text(
        '{"title":"X","price_gbp":10.0,"rating":"Five","availability":"In stock","source_page":"u","ingestion_date":"2026-05-06"}\n',
        encoding="utf-8",
    )

    class _Cfg:
        bronze_dir = bronze
        silver_dir = tmp_path / "silver"

    spark_transform._transform_windows_fallback(_Cfg())
    assert (_Cfg.silver_dir / "books_curated").exists()
    assert (_Cfg.silver_dir / "web_catalog_curated").exists()


def test_transform_and_write_windows_fallback_branch(monkeypatch, tmp_path: Path) -> None:
    import sys
    bronze = tmp_path / "bronze"
    (bronze / "books_csv").mkdir(parents=True)
    (bronze / "books_sql").mkdir(parents=True)
    (bronze / "books_web").mkdir(parents=True)

    class _Reader:
        def option(self, *_args, **_kwargs):
            return self

        def csv(self, *_args, **_kwargs):
            raise Exception("NativeIO$Windows.access0")

        def json(self, *_args, **_kwargs):
            raise Exception("NativeIO$Windows.access0")

    class _Builder:
        def appName(self, *_args, **_kwargs):
            return self

        def config(self, *_args, **_kwargs):
            return self

        def getOrCreate(self):
            class _Spark:
                read = _Reader()

                class _SparkContext:
                    @staticmethod
                    def setLogLevel(*_args, **_kwargs):
                        return None

                sparkContext = _SparkContext()

                @staticmethod
                def stop():
                    return None

            return _Spark()

    class _Cfg:
        bronze_dir = bronze
        silver_dir = tmp_path / "silver"

    called = {"fallback": False}
    # Monkeypatch pyspark.sql.SparkSession since it's imported inside the function
    sys.modules['pyspark.sql'].SparkSession.builder = _Builder()
    monkeypatch.setattr(spark_transform.platform, "system", lambda: "Windows")
    monkeypatch.setattr(spark_transform, "_is_windows_nativeio_error", lambda _e: True)
    monkeypatch.setattr(
        spark_transform,
        "_transform_windows_fallback",
        lambda _cfg: called.__setitem__("fallback", True),
    )

    spark_transform.transform_and_write(_Cfg())
    assert called["fallback"] is True


def test_transform_and_write_reraises_non_windows_error(monkeypatch, tmp_path: Path) -> None:
    import sys
    bronze = tmp_path / "bronze"
    (bronze / "books_csv").mkdir(parents=True)
    (bronze / "books_sql").mkdir(parents=True)
    (bronze / "books_web").mkdir(parents=True)

    class _Reader:
        def option(self, *_args, **_kwargs):
            return self

        def csv(self, *_args, **_kwargs):
            raise Exception("different failure")

    class _Builder:
        def appName(self, *_args, **_kwargs):
            return self

        def config(self, *_args, **_kwargs):
            return self

        def getOrCreate(self):
            class _Spark:
                read = _Reader()

                class _SparkContext:
                    @staticmethod
                    def setLogLevel(*_args, **_kwargs):
                        return None

                sparkContext = _SparkContext()

                @staticmethod
                def stop():
                    return None

            return _Spark()

    class _Cfg:
        bronze_dir = bronze
        silver_dir = tmp_path / "silver"

    # Monkeypatch pyspark.sql.SparkSession since it's imported inside the function
    sys.modules['pyspark.sql'].SparkSession.builder = _Builder()
    sys.modules['py4j.protocol'].Py4JJavaError = Exception
    monkeypatch.setattr(spark_transform.platform, "system", lambda: "Linux")
    monkeypatch.setattr(spark_transform, "_is_windows_nativeio_error", lambda _e: False)

    with pytest.raises(Exception):
        spark_transform.transform_and_write(_Cfg())


def test_normalize_web_dataframe_pandas() -> None:
    import pandas as pd

    df = pd.DataFrame([{"title": "A", "ingestion_date": "2026-05-06"}])
    normalized = spark_transform._normalize_web_dataframe_pandas(df)
    assert "category" in normalized.columns
    assert normalized.iloc[0]["num_reviews"] == 0


def test_normalize_web_dataframe_spark(monkeypatch) -> None:
    import sys
    class _Df:
        def __init__(self):
            self.columns = ["title", "ingestion_date"]

        def withColumn(self, name, _value):
            if name not in self.columns:
                self.columns.append(name)
            return self

        def select(self, *_args):
            return self

    sys.modules['pyspark.sql'].functions.lit = lambda value: value
    normalized = spark_transform._normalize_web_dataframe_spark(_Df())
    assert "category" in normalized.columns


def test_project_books_curated_pandas_uses_image_headers() -> None:
    import pandas as pd

    df = pd.DataFrame(
        [
            {
                "title": "A",
                "isbn": "1",
                "author": "x",
                "year": 2000,
                "publisher": "p",
                "publisher_country": "FR",
                "language": "FR",
                "Image-URL-S": "s",
                "Image-URL-M": "m",
                "Image-URL-L": "l",
                "ingestion_date": "2026-05-06",
            }
        ]
    )
    projected = spark_transform._project_books_curated_pandas(df)
    assert projected.iloc[0]["image_url_s"] == "s"
    assert projected.iloc[0]["record_source"] == "csv_sql"


def test_project_web_curated_spark(monkeypatch) -> None:
    import sys
    class _Df:
        def __init__(self):
            self.columns = ["title", "ingestion_date"]

        def withColumn(self, name, _value):
            if name not in self.columns:
                self.columns.append(name)
            return self

        def select(self, *_args):
            return self

    sys.modules['pyspark.sql'].functions.lit = lambda value: value
    projected = spark_transform._project_web_curated_spark(_Df())
    assert "record_source" in projected.columns
    assert "image_url_s" in projected.columns
