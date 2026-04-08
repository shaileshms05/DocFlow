"""AWS Textract: document text + layout blocks (replaces Tesseract when ocr.backend is textract)."""

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple

import yaml

_BOX = Dict[str, Any]


def _root() -> Path:
    return Path(__file__).resolve().parent.parent


def _cfg() -> dict:
    with open(_root() / "config" / "config.yaml", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _region() -> str:
    return os.environ.get("AWS_REGION") or _cfg().get("aws", {}).get("region") or "us-east-1"


def _textract_client():
    import boto3

    return boto3.client("textract", region_name=_region())


def _parse_s3_uri(uri: str) -> Tuple[str, str]:
    rest = uri[5:] if uri.startswith("s3://") else uri
    bucket, _, key = rest.partition("/")
    return bucket, key


def _collect_text_and_blocks_from_response(blocks: List[Dict]) -> Tuple[str, List[_BOX]]:
    """Normalize Textract Block list to plain text + structured lines."""
    lines: List[str] = []
    out_blocks: List[_BOX] = []
    for b in blocks:
        bt = b.get("BlockType")
        if bt == "LINE":
            t = b.get("Text") or ""
            lines.append(t)
            geom = b.get("Geometry") or {}
            bbox = (geom.get("BoundingBox") or {})
            out_blocks.append(
                {
                    "type": "LINE",
                    "text": t,
                    "confidence": float(b.get("Confidence") or 0) / 100.0,
                    "page": int(b.get("Page", 1)),
                    "bbox": bbox,
                }
            )
    text = "\n".join(lines)
    return text, out_blocks


def _start_and_wait_s3_text_detection(bucket: str, key: str) -> Tuple[str, List[_BOX]]:
    client = _textract_client()
    r = client.start_document_text_detection(
        DocumentLocation={"S3Object": {"Bucket": bucket, "Name": key}}
    )
    job_id = r["JobId"]
    while True:
        rr = client.get_document_text_detection(JobId=job_id)
        st = rr["JobStatus"]
        if st == "FAILED":
            raise RuntimeError(rr.get("StatusMessage", "Textract failed"))
        if st == "SUCCEEDED":
            all_blocks: List[Dict] = list(rr.get("Blocks") or [])
            nt = rr.get("NextToken")
            while nt:
                rr = client.get_document_text_detection(JobId=job_id, NextToken=nt)
                all_blocks.extend(rr.get("Blocks") or [])
                nt = rr.get("NextToken")
            return _collect_text_and_blocks_from_response(all_blocks)
        time.sleep(1.0)


def _detect_bytes_sync(data: bytes) -> Tuple[str, List[_BOX]]:
    client = _textract_client()
    r = client.detect_document_text(Document={"Bytes": data})
    blocks = r.get("Blocks") or []
    return _collect_text_and_blocks_from_response(blocks)


def extract_text_textract(file_path: str, data: bytes, suffix: str) -> Tuple[str, List[_BOX]]:
    """
    AWS Textract for images and for non-PDF objects in S3.

    **PDFs** use open-source Tesseract (same as ``ocr.backend: tesseract``), not Textract.
    """
    suffix = (suffix or ".bin").lower()
    if suffix == ".pdf":
        from streaming.ocr import extract_text_from_pdf_tesseract_bytes

        return extract_text_from_pdf_tesseract_bytes(data)

    if file_path.startswith("s3://"):
        bucket, key = _parse_s3_uri(file_path)
        return _start_and_wait_s3_text_detection(bucket, key)

    if suffix in (".png", ".jpg", ".jpeg", ".tiff", ".tif", ".webp"):
        return _detect_bytes_sync(data)

    return _detect_bytes_sync(data)
