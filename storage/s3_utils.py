"""Upload and download objects — S3 when configured, else local paths under data_dir."""

from __future__ import annotations

import os
import uuid
from pathlib import Path
from typing import BinaryIO, Optional, Tuple

import yaml

_CONFIG_CACHE: Optional[dict] = None


def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def load_config() -> dict:
    global _CONFIG_CACHE
    if _CONFIG_CACHE is None:
        path = _project_root() / "config" / "config.yaml"
        with open(path, encoding="utf-8") as f:
            _CONFIG_CACHE = yaml.safe_load(f)
    return _CONFIG_CACHE


def _env_override(key: str, default: str) -> str:
    return os.environ.get(key, default)


def save_upload(file_content: bytes, original_filename: str) -> Tuple[str, str]:
    """
    Persist raw bytes. Returns (logical_uri, doc_id).
    logical_uri is s3://bucket/key or file://abs/path for downstream consumers.
    """
    cfg = load_config()
    doc_id = str(uuid.uuid4())
    ext = Path(original_filename).suffix or ".bin"
    safe_name = f"{doc_id}{ext}"

    backend = _env_override("STORAGE_BACKEND", cfg["storage"]["backend"])
    if backend == "s3":
        bucket = _env_override("S3_BUCKET", cfg["storage"]["s3_bucket"])
        prefix = cfg["storage"]["s3_raw_prefix"].strip("/")
        key = f"{prefix}/{safe_name}" if prefix else safe_name
        try:
            import boto3
        except ImportError as e:
            raise RuntimeError("boto3 required for S3 backend") from e
        client = boto3.client("s3")
        client.put_object(Bucket=bucket, Key=key, Body=file_content)
        uri = f"s3://{bucket}/{key}"
        return uri, doc_id

    data_dir = Path(_env_override("DATA_DIR", cfg["app"]["data_dir"])).resolve()
    raw = data_dir / cfg["storage"]["local_raw_prefix"]
    raw.mkdir(parents=True, exist_ok=True)
    dest = raw / safe_name
    dest.write_bytes(file_content)
    uri = dest.as_uri()
    return uri, doc_id


def read_file_bytes(uri: str) -> bytes:
    """Load file from s3:// or file:// or plain absolute path."""
    if uri.startswith("s3://"):
        rest = uri[5:]
        bucket, _, key = rest.partition("/")
        import boto3

        client = boto3.client("s3")
        obj = client.get_object(Bucket=bucket, Key=key)
        return obj["Body"].read()
    if uri.startswith("file://"):
        return Path(uri[7:]).read_bytes()
    p = Path(uri)
    if p.is_file():
        return p.read_bytes()
    raise FileNotFoundError(uri)


def save_processed_json(doc_id: str, payload: dict) -> str:
    """Write processed JSON; returns URI string."""
    import json

    body = json.dumps(payload, indent=2).encode("utf-8")
    cfg = load_config()
    backend = _env_override("STORAGE_BACKEND", cfg["storage"]["backend"])
    name = f"{doc_id}.json"

    if backend == "s3":
        bucket = _env_override("S3_BUCKET", cfg["storage"]["s3_bucket"])
        prefix = cfg["storage"]["s3_processed_prefix"].strip("/")
        key = f"{prefix}/{name}" if prefix else name
        import boto3

        boto3.client("s3").put_object(
            Bucket=bucket, Key=key, Body=body, ContentType="application/json"
        )
        return f"s3://{bucket}/{key}"

    data_dir = Path(_env_override("DATA_DIR", cfg["app"]["data_dir"])).resolve()
    proc = data_dir / cfg["storage"]["local_processed_prefix"]
    proc.mkdir(parents=True, exist_ok=True)
    dest = proc / name
    dest.write_bytes(body)
    return dest.as_uri()


def open_binary_stream(uri: str) -> BinaryIO:
    from io import BytesIO

    return BytesIO(read_file_bytes(uri))
