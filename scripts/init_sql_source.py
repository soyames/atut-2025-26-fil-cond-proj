from pathlib import Path
import sqlite3
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
CSV_PATH = ROOT / "Books.csv"
DB_PATH = ROOT / "data" / "sources" / "books_source.db"


def build_sql_source() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    books = pd.read_csv(CSV_PATH, low_memory=False)
    books = books.rename(columns={"ISBN": "isbn", "Publisher": "publisher"})
    books = books[["isbn", "publisher"]].dropna()
    publishers = books["publisher"].drop_duplicates().reset_index(drop=True)
    pub_meta = pd.DataFrame(
        {
            "publisher": publishers,
            "publisher_country": "Unknown",
            "language": "EN",
        }
    )

    with sqlite3.connect(DB_PATH) as conn:
        books.to_sql("books_metadata", conn, if_exists="replace", index=False)
        pub_meta.to_sql("publisher_metadata", conn, if_exists="replace", index=False)


if __name__ == "__main__":
    build_sql_source()
    print(f"SQLite source initialisee: {DB_PATH}")

