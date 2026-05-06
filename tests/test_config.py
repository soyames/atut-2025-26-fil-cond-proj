from src.etl.config import ETLConfig


def test_config_from_env_defaults(monkeypatch) -> None:
    monkeypatch.delenv("PROJECT_ROOT", raising=False)
    cfg = ETLConfig.from_env()
    assert cfg.books_csv_path.name == "Books.csv"
    assert cfg.minio_bucket == "lakehouse"


def test_config_from_env_override(monkeypatch) -> None:
    monkeypatch.setenv("WEB_MAX_PAGES", "5")
    monkeypatch.setenv("MINIO_SECURE", "true")
    cfg = ETLConfig.from_env()
    assert cfg.web_max_pages == 5
    assert cfg.minio_secure is True

