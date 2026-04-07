"""Inference: prefer MLflow Production/Staging model, else rule-based doc_type."""

from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from ml.registry_bridge import load_production_model
from streaming.extractor import build_result_payload, extract_fields


def predict_doc_type(text: str) -> tuple[str, float, str]:
    """
    Returns (doc_type, confidence, source) where source is 'mlflow' or 'rules'.
    """
    clf = load_production_model()
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
