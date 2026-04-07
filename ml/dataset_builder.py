"""Build training rows from processed JSON in S3 (or local) + feedback overrides."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import List, Tuple

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from feedback.feedback_store import list_feedback_for_training
from storage.s3_utils import iter_processed_payloads


def text_from_payload(payload: dict) -> str:
    parts = [payload.get("doc_type") or "", json.dumps(payload.get("fields") or {})]
    return " ".join(parts)


def build_xy() -> Tuple[List[str], List[str]]:
    """
    Returns (texts, labels) for doc_type classification.
    Feedback with field=='doc_type' overrides label for that doc_id.
    """
    texts: List[str] = []
    labels: List[str] = []
    doc_ids: List[str] = []

    for payload in iter_processed_payloads():
        texts.append(text_from_payload(payload))
        labels.append(str(payload.get("doc_type") or "unknown"))
        doc_ids.append(str(payload["doc_id"]))

    overrides = {}
    for fb in list_feedback_for_training():
        if fb.get("field") == "doc_type" and fb.get("actual"):
            overrides[fb["doc_id"]] = fb["actual"].strip().lower()

    for i, did in enumerate(doc_ids):
        if did in overrides:
            labels[i] = overrides[did]

    return texts, labels
