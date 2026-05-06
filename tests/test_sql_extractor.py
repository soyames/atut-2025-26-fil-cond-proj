from pathlib import Path
import sqlite3
import pandas as pd

from src.etl.extract.books_sql import extract_books_sql


def test_extract_books_sql(tmp_path: Path) -> None:
    db = tmp_path / "source.db"
    out_dir = tmp_path / "out"

    with sqlite3.connect(db) as conn:
        pd.DataFrame([{"isbn": "1", "publisher": "P"}]).to_sql("books_metadata", conn, index=False)
        pd.DataFrame([{"publisher": "P", "publisher_country": "FR", "language": "FR"}]).to_sql(
            "publisher_metadata", conn, index=False
        )

    output = extract_books_sql(db, out_dir)
    df = pd.read_csv(output)
    assert df.iloc[0]["publisher_country"] == "FR"

