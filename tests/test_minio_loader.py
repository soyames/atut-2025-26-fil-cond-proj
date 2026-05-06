from pathlib import Path

from src.etl.load.minio_loader import upload_directory_to_minio


class _FakeS3:
    def __init__(self) -> None:
        self.uploaded: list[tuple[str, str, str]] = []
        self.created_bucket = False

    def head_bucket(self, Bucket: str) -> None:
        _ = Bucket

    def create_bucket(self, Bucket: str) -> None:
        _ = Bucket
        self.created_bucket = True

    def upload_file(self, filename: str, bucket: str, key: str) -> None:
        self.uploaded.append((filename, bucket, key))


class _FakeSession:
    def __init__(self, client: _FakeS3) -> None:
        self._client = client

    def client(self, *_args, **_kwargs):
        return self._client


def test_upload_directory_to_minio(monkeypatch, tmp_path: Path) -> None:
    local_dir = tmp_path / "silver"
    (local_dir / "dataset").mkdir(parents=True)
    file_path = local_dir / "dataset" / "part-000.parquet"
    file_path.write_text("data", encoding="utf-8")

    fake_s3 = _FakeS3()
    monkeypatch.setattr("boto3.session.Session", lambda: _FakeSession(fake_s3))

    class Cfg:
        minio_endpoint = "http://localhost:9000"
        minio_access_key = "x"
        minio_secret_key = "y"
        minio_bucket = "lakehouse"
        minio_prefix = "curated"

    upload_directory_to_minio(Cfg(), local_dir)
    assert len(fake_s3.uploaded) == 1
    assert fake_s3.uploaded[0][1] == "lakehouse"
    assert fake_s3.uploaded[0][2] == "curated/dataset/part-000.parquet"


def test_upload_directory_creates_bucket_if_missing(monkeypatch, tmp_path: Path) -> None:
    local_dir = tmp_path / "silver"
    local_dir.mkdir(parents=True)
    (local_dir / "file.txt").write_text("x", encoding="utf-8")

    class _MissingBucketS3(_FakeS3):
        def head_bucket(self, Bucket: str) -> None:
            _ = Bucket
            raise RuntimeError("not found")

    fake_s3 = _MissingBucketS3()
    monkeypatch.setattr("boto3.session.Session", lambda: _FakeSession(fake_s3))

    class Cfg:
        minio_endpoint = "http://localhost:9000"
        minio_access_key = "x"
        minio_secret_key = "y"
        minio_bucket = "lakehouse"
        minio_prefix = "curated"

    upload_directory_to_minio(Cfg(), local_dir)
    assert fake_s3.created_bucket is True
