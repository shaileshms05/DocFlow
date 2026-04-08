"""Amazon Comprehend: entity + key-phrase detection (ML) on extracted document text."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List

import yaml

_ROOT = Path(__file__).resolve().parent.parent


def _cfg() -> dict:
    with open(_ROOT / "config" / "config.yaml", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _region() -> str:
    return os.environ.get("AWS_REGION") or _cfg().get("aws", {}).get("region") or "us-east-1"


def _truncate(text: str, max_bytes: int = 4500) -> str:
    """Comprehend DetectEntities max input 5000 UTF-8 bytes — stay under limit."""
    b = text.encode("utf-8")
    if len(b) <= max_bytes:
        return text
    return b[:max_bytes].decode("utf-8", errors="ignore")


def enrich_text_with_comprehend(text: str) -> Dict[str, Any]:
    """
    Returns { "entities": [...], "key_phrases": [...] } or empty dict if disabled / error.
    """
    ml = _cfg().get("ml", {}) or {}
    if not ml.get("comprehend_enabled", True):
        return {}
    if not text or not text.strip():
        return {}

    try:
        import boto3
    except ImportError:
        return {}

    client = boto3.client("comprehend", region_name=_region())
    sample = _truncate(text)

    out: Dict[str, Any] = {"entities": [], "key_phrases": []}
    try:
        er = client.detect_entities(Text=sample, LanguageCode="en")
        out["entities"] = [
            {
                "text": e.get("Text"),
                "type": e.get("Type"),
                "score": round(float(e.get("Score") or 0), 4),
            }
            for e in (er.get("Entities") or [])
        ]
    except Exception:
        pass
    try:
        kr = client.detect_key_phrases(Text=sample, LanguageCode="en")
        out["key_phrases"] = [
            {
                "text": k.get("Text"),
                "score": round(float(k.get("Score") or 0), 4),
            }
            for k in (kr.get("KeyPhrases") or [])
        ]
    except Exception:
        pass
    return out


def merge_entities_into_fields(
    fields: Dict[str, Any],
    entities: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Heuristic merge: PERSON → name, EMAIL from text already handled; add comprehend_persons list."""
    persons = [e["text"] for e in entities if e.get("type") == "PERSON" and e.get("text")]
    orgs = [e["text"] for e in entities if e.get("type") == "ORGANIZATION" and e.get("text")]
    merged = dict(fields)
    if persons and not merged.get("name"):
        merged["name"] = persons[0]
    if orgs and not merged.get("organization"):
        merged["organization"] = orgs[0]
    merged["ml_entities"] = entities[:25]
    return merged
