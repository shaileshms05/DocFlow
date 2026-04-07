"""Build training rows from processed JSON on disk/DB + feedback overrides."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import List, Tuple

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import yaml
from feedback.feedback_store import list_feedback_for_training
from storage.db import ProcessedDocumentRecord, get_engine
from storage.s3_utils import read_file_bytes


def _load_json_uri(uri: str) -> dict:
    raw = read_file_bytes(uri)
    return json.loads(raw.decode("utf-8"))


def text_from_payload(payload: dict) -> str:
    """Flatten fields into a single string for ML training. Prefer raw_text if present."""
    if payload.get("raw_text"):
        return payload["raw_text"]
    parts = [payload.get("doc_type") or "", json.dumps(payload.get("fields") or {})]
    return " ".join(parts)


def build_xy() -> Tuple[List[str], List[str]]:
    """
    Returns (texts, labels) for doc_type classification.
    Feedback with field=='doc_type' overrides label for that doc_id.
    """
    engine = get_engine()
    from sqlalchemy.orm import sessionmaker

    SessionLocal = sessionmaker(bind=engine)
    texts: List[str] = []
    labels: List[str] = []
    doc_ids: List[str] = []

    with SessionLocal() as session:
        rows = session.query(ProcessedDocumentRecord).all()
        for r in rows:
            try:
                payload = _load_json_uri(r.json_uri)
            except Exception:
                continue
            texts.append(text_from_payload(payload))
            labels.append(str(payload.get("doc_type") or "unknown"))
            doc_ids.append(r.doc_id)

    overrides = {}
    for fb in list_feedback_for_training():
        if fb.get("field") == "doc_type" and fb.get("actual"):
            overrides[fb["doc_id"]] = fb["actual"].strip().lower()

    for i, did in enumerate(doc_ids):
        if did in overrides:
            labels[i] = overrides[did]

    return texts, labels
