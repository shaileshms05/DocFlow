"""Upload and download objects — S3 when configured, else local paths under data_dir."""

from __future__ import annotations

import os
import uuid
from pathlib import Path
from typing import Any, BinaryIO, Dict, Iterator, List, Optional, Tuple

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


def _s3_output_bucket() -> str:
    cfg = load_config()
    out = cfg.get("s3_output") or {}
    return _env_override("S3_OUTPUT_BUCKET", out.get("bucket") or "").strip()


def _s3_output_prefix(kind: str) -> str:
    cfg = load_config()
    out = cfg.get("s3_output") or {}
    if kind == "processed":
        p = out.get("processed_prefix", "processed")
    elif kind == "ingest":
        p = out.get("ingest_prefix", "ingest")
    else:
        p = out.get("feedback_prefix", "feedback")
    return str(p).strip("/")


def _put_s3_json(bucket: str, key: str, body: bytes) -> str:
    import boto3

    boto3.client("s3").put_object(
        Bucket=bucket, Key=key, Body=body, ContentType="application/json"
    )
    return f"s3://{bucket}/{key}"


def _data_dir() -> Path:
    cfg = load_config()
    return Path(_env_override("DATA_DIR", cfg["app"]["data_dir"])).resolve()


def save_ingest_manifest(
    doc_id: str,
    file_path: str,
    doc_type: str,
    status: str = "queued",
    processed_uri: Optional[str] = None,
) -> Optional[str]:
    """Write upload / pipeline audit JSON to output bucket (or local data/ingest if no bucket)."""
    import json
    from datetime import datetime, timezone

    body: Dict[str, Any] = {
        "doc_id": doc_id,
        "file_path": file_path,
        "doc_type": doc_type,
        "status": status,
        "ts": datetime.now(timezone.utc).isoformat(),
    }
    if processed_uri:
        body["processed_uri"] = processed_uri
    raw = json.dumps(body, indent=2).encode("utf-8")
    bucket = _s3_output_bucket()
    if bucket:
        prefix = _s3_output_prefix("ingest")
        key = f"{prefix}/{doc_id}.json" if prefix else f"{doc_id}.json"
        return _put_s3_json(bucket, key, raw)
    ingest = _data_dir() / "ingest"
    ingest.mkdir(parents=True, exist_ok=True)
    (ingest / f"{doc_id}.json").write_bytes(raw)
    return (ingest / f"{doc_id}.json").as_uri()


def _list_s3_json_objects(bucket: str, prefix: str) -> Iterator[str]:
    import boto3

    client = boto3.client("s3")
    paginator = client.get_paginator("list_objects_v2")
    pfx = prefix.strip("/")
    pfx = f"{pfx}/" if pfx else ""
    for page in paginator.paginate(Bucket=bucket, Prefix=pfx):
        for obj in page.get("Contents") or []:
            k = obj["Key"]
            if k.endswith(".json"):
                yield k


def iter_processed_payloads() -> List[dict]:
    """All processed extraction JSON objects (S3 output bucket or local processed/)."""
    import json

    bucket = _s3_output_bucket()
    out: List[dict] = []
    if bucket:
        import boto3

        client = boto3.client("s3")
        prefix = _s3_output_prefix("processed")
        for key in _list_s3_json_objects(bucket, prefix):
            try:
                obj = client.get_object(Bucket=bucket, Key=key)
                payload = json.loads(obj["Body"].read().decode("utf-8"))
                if isinstance(payload, dict) and payload.get("doc_id"):
                    out.append(payload)
            except Exception:
                continue
        return out

    cfg = load_config()
    proc = _data_dir() / cfg["storage"]["local_processed_prefix"]
    if not proc.is_dir():
        return []
    for p in sorted(proc.glob("*.json")):
        try:
            payload = json.loads(p.read_text(encoding="utf-8"))
            if isinstance(payload, dict) and payload.get("doc_id"):
                out.append(payload)
        except Exception:
            continue
    return out


def iter_feedback_payloads() -> List[dict]:
    """Feedback events from S3 feedback prefix or local data/feedback/."""
    import json

    bucket = _s3_output_bucket()
    out: List[dict] = []
    if bucket:
        import boto3

        client = boto3.client("s3")
        prefix = _s3_output_prefix("feedback")
        for key in _list_s3_json_objects(bucket, prefix):
            try:
                obj = client.get_object(Bucket=bucket, Key=key)
                row = json.loads(obj["Body"].read().decode("utf-8"))
                if isinstance(row, dict) and row.get("doc_id"):
                    out.append(row)
            except Exception:
                continue
        return out

    fb = _data_dir() / "feedback"
    if not fb.is_dir():
        return []
    for p in sorted(fb.glob("*.json")):
        try:
            row = json.loads(p.read_text(encoding="utf-8"))
            if isinstance(row, dict) and row.get("doc_id"):
                out.append(row)
        except Exception:
            continue
    return out


def save_feedback_to_output(event: Dict[str, Any]) -> str:
    """
    Write feedback JSON to S3 output bucket, or ``data/feedback/`` locally if no bucket.
    """
    import json

    eid = event.get("event_id") or str(uuid.uuid4())
    body = json.dumps(event, indent=2).encode("utf-8")
    bucket = _s3_output_bucket()
    if bucket:
        prefix = _s3_output_prefix("feedback")
        key = f"{prefix}/{eid}.json" if prefix else f"{eid}.json"
        return _put_s3_json(bucket, key, body)
    fb = _data_dir() / "feedback"
    fb.mkdir(parents=True, exist_ok=True)
    dest = fb / f"{eid}.json"
    dest.write_bytes(body)
    return dest.as_uri()


def save_upload(file_content: bytes, original_filename: str) -> Tuple[str, str]:
    """
    Persist raw bytes. Returns (logical_uri, doc_id).

    S3 is used when, in order:
    1. ``S3_INPUT_BUCKET`` or ``storage.s3_input_bucket`` is set (raw uploads to S3 even if backend is local), or
    2. ``storage.backend`` is ``s3`` and ``S3_BUCKET`` / ``s3_bucket`` is set.

    Otherwise writes under ``data_dir`` / ``local_raw_prefix``.
    """
    cfg = load_config()
    st = cfg["storage"]
    doc_id = str(uuid.uuid4())
    ext = Path(original_filename).suffix or ".bin"
    safe_name = f"{doc_id}{ext}"

    backend = _env_override("STORAGE_BACKEND", st["backend"])
    input_bucket = _env_override("S3_INPUT_BUCKET", st.get("s3_input_bucket") or "").strip()
    primary_bucket = _env_override("S3_BUCKET", st.get("s3_bucket") or "").strip()

    bucket = ""
    if input_bucket:
        bucket = input_bucket
    elif backend == "s3":
        bucket = primary_bucket

    if bucket:
        prefix = st["s3_raw_prefix"].strip("/")
        key = f"{prefix}/{safe_name}" if prefix else safe_name
        try:
            import boto3
        except ImportError as e:
            raise RuntimeError("boto3 required for S3 uploads") from e
        client = boto3.client("s3")
        client.put_object(Bucket=bucket, Key=key, Body=file_content)
        uri = f"s3://{bucket}/{key}"
        return uri, doc_id

    if backend == "s3" and not bucket:
        raise ValueError("storage.backend is s3 but no bucket configured (S3_BUCKET or s3_bucket)")

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
    """
    Write processed extraction JSON; returns canonical URI.

    If ``s3_output.bucket`` (or env ``S3_OUTPUT_BUCKET``) is set, the artifact is stored
    there and that s3:// URI is returned.

    Otherwise: existing behavior — local disk or primary ``storage.s3_bucket`` processed prefix.
    """
    import json

    body = json.dumps(payload, indent=2).encode("utf-8")
    cfg = load_config()
    name = f"{doc_id}.json"

    out_bucket = _s3_output_bucket()
    if out_bucket:
        try:
            import boto3  # noqa: F401
        except ImportError as e:
            raise RuntimeError("boto3 required for S3 output bucket") from e
        prefix = _s3_output_prefix("processed")
        key = f"{prefix}/{name}" if prefix else name
        return _put_s3_json(out_bucket, key, body)

    backend = _env_override("STORAGE_BACKEND", cfg["storage"]["backend"])
    if backend == "s3":
        bucket = _env_override("S3_BUCKET", cfg["storage"]["s3_bucket"])
        proc_prefix = cfg["storage"]["s3_processed_prefix"].strip("/")
        key = f"{proc_prefix}/{name}" if proc_prefix else name
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
