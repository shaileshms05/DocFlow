"""Inference: prefer MLflow Production/Staging model, else rule-based doc_type."""

from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import time
from ml.registry_bridge import load_production_model
from streaming.extractor import build_result_payload, extract_fields

# Production caching: Loading the model via network/disk on every request is a major performance killer.
_CACHED_MODEL = None
_LAST_LOAD_TIME = 0.0
_CACHE_TTL = 300.0  # Seconds (5 minutes)


def _get_production_model():
    """Retrieve the model from cache or load it if expired/missing."""
    global _CACHED_MODEL, _LAST_LOAD_TIME
    now = time.time()
    if _CACHED_MODEL is None or (now - _LAST_LOAD_TIME) > _CACHE_TTL:
        try:
            _CACHED_MODEL = load_production_model()
            _LAST_LOAD_TIME = now
        except Exception:
            # Fallback briefly to rules if model registry is unstable, but retry cache later
            pass
    return _CACHED_MODEL


def predict_doc_type(text: str) -> tuple[str, float, str]:
    """
    Returns (doc_type, confidence, source) where source is 'mlflow' or 'rules'.
    Uses the cached production model for high throughput.
    """
    clf = _get_production_model()
    if clf is not None:
        try:
            label = clf.predict([text])[0]
            proba = None
            if hasattr(clf, "predict_proba"):
                pr = clf.predict_proba([text])[0]
                proba = float(pr.max())
            return str(label), proba or 0.85, "mlflow"
        except Exception:
            pass
    ex = extract_fields(text, None)
    return ex["doc_type"], float(ex["confidence"]), "rules"


def predict_full_text(text: str, doc_type_hint: str | None = None) -> dict:
    """Merge ML doc_type (when hint unknown) with rule-based field extraction."""
    if doc_type_hint and doc_type_hint != "unknown":
        return build_result_payload("inline", text, doc_type_hint)
    doc_type, conf, src = predict_doc_type(text)
    payload = build_result_payload("inline", text, doc_type)
    payload["confidence"] = conf
    payload["doc_type_source"] = src
    return payload
