from pathlib import Path
from types import SimpleNamespace

from src.etl.jobs import run_extract, run_load, run_transform


def test_run_extract_pipeline_calls_extractors(monkeypatch) -> None:
    cfg = SimpleNamespace(
        books_csv_path=Path("Books.csv"),
        sqlite_source_db=Path("data/sources/books_source.db"),
        web_source_url="https://books.toscrape.com/catalogue/page-1.html",
        web_max_pages=1,
        bronze_dir=Path("data/bronze"),
    )
    called = {"csv": False, "sql": False, "web": False}

    monkeypatch.setattr(run_extract.ETLConfig, "from_env", lambda: cfg)
    monkeypatch.setattr(run_extract, "extract_books_csv", lambda *_args, **_kwargs: called.__setitem__("csv", True))
    monkeypatch.setattr(run_extract, "extract_books_sql", lambda *_args, **_kwargs: called.__setitem__("sql", True))
    monkeypatch.setattr(run_extract, "extract_books_web", lambda *_args, **_kwargs: called.__setitem__("web", True))

    run_extract.run_extract_pipeline()
    assert called == {"csv": True, "sql": True, "web": True}


def test_run_transform_pipeline_calls_transform(monkeypatch) -> None:
    cfg = SimpleNamespace()
    called = {"transform": False}
    monkeypatch.setattr(run_transform.ETLConfig, "from_env", lambda: cfg)
    monkeypatch.setattr(run_transform, "transform_and_write", lambda _cfg: called.__setitem__("transform", True))
    run_transform.run_transform_pipeline()
    assert called["transform"] is True


def test_run_load_pipeline_calls_loader(monkeypatch) -> None:
    cfg = SimpleNamespace(silver_dir=Path("data/silver"))
    called = {"load": False}
    monkeypatch.setattr(run_load.ETLConfig, "from_env", lambda: cfg)
    monkeypatch.setattr(run_load, "upload_directory_to_minio", lambda *_args, **_kwargs: called.__setitem__("load", True))
    run_load.run_load_pipeline()
    assert called["load"] is True

