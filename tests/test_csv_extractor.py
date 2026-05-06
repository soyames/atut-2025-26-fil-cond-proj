from pathlib import Path
import pandas as pd

from src.etl.extract.books_csv import extract_books_csv


def test_extract_books_csv(tmp_path: Path) -> None:
    source = tmp_path / "input.csv"
    source.write_text("ISBN,Book-Title\n1,Test\n", encoding="utf-8")
    out_dir = tmp_path / "out"

    output = extract_books_csv(source, out_dir)
    df = pd.read_csv(output)
    assert "ingestion_date" in df.columns
    assert df.iloc[0]["ISBN"] == 1

