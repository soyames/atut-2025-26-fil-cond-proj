from datetime import datetime, timezone

from src.etl.transform.spark_transform import normalize_year


def test_normalize_year_valid() -> None:
    assert normalize_year("2000") == 2000
    assert normalize_year(1999) == 1999


def test_normalize_year_invalid() -> None:
    assert normalize_year(None) is None
    assert normalize_year("abc") is None
    assert normalize_year("0") is None
    assert normalize_year(str(datetime.now(timezone.utc).year + 2)) is None


