"""Rule-based + regex field extraction (MVP). Replace with LayoutLM in advanced path."""

from __future__ import annotations

import re
from typing import Any, Dict, List, Tuple


def _detect_doc_type(text: str) -> Tuple[str, float]:
    t = text.lower()
    scores = {"resume": 0, "invoice": 0, "kyc": 0}
    resume_kw = ("curriculum", "vitae", "resume", "experience", "education", "skills", "linkedin")
    invoice_kw = ("invoice", "bill to", "amount due", "total", "tax id", "vat", "payment terms")
    kyc_kw = ("passport", "national id", "date of birth", "government", "kyc", "identity")
    for w in resume_kw:
        if w in t:
            scores["resume"] += 1
    for w in invoice_kw:
        if w in t:
            scores["invoice"] += 1
    for w in kyc_kw:
        if w in t:
            scores["kyc"] += 1
    best = max(scores, key=lambda k: scores[k])
    total = sum(scores.values()) or 1
    conf = min(0.95, 0.4 + 0.15 * scores[best])
    if scores[best] == 0:
        return "unknown", 0.3
    return best, conf


def _email(text: str) -> str | None:
    m = re.search(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+", text)
    return m.group(0) if m else None


def _phones(text: str) -> List[str]:
    return list(
        dict.fromkeys(
            re.findall(r"\+?\d[\d\s\-().]{8,}\d", text)
        )
    )[:5]


def _lines(text: str) -> List[str]:
    return [ln.strip() for ln in text.splitlines() if ln.strip()]


def _resume_fields(text: str) -> Dict[str, Any]:
    lines = _lines(text)
    name = lines[0] if lines else None
    skills: List[str] = []
    if "skills" in text.lower():
        idx = next((i for i, ln in enumerate(lines) if "skill" in ln.lower()), None)
        if idx is not None:
            chunk = "\n".join(lines[idx : idx + 8])
            skills = re.findall(r"[A-Za-z][A-Za-z0-9+#.\s]{1,24}", chunk)
            skills = [s.strip() for s in skills if len(s.strip()) > 2][:20]
    return {
        "name": name,
        "email": _email(text),
        "phone": _phones(text),
        "skills": skills or None,
    }


def _invoice_fields(text: str) -> Dict[str, Any]:
    inv_no = None
    m = re.search(r"(?:invoice\s*#|inv\s*#|no\.?)\s*[:\s]*([A-Z0-9\-]+)", text, re.I)
    if m:
        inv_no = m.group(1)
    amounts = re.findall(r"(?:\$|USD|EUR|£)\s*[\d,]+\.?\d*", text)
    total = amounts[-1] if amounts else None
    return {
        "invoice_number": inv_no,
        "total": total,
        "vendor_email": _email(text),
    }


def _kyc_fields(text: str) -> Dict[str, Any]:
    dob = None
    m = re.search(
        r"(?:dob|date of birth|born)[:\s]+(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})", text, re.I
    )
    if m:
        dob = m.group(1)
    doc_id = None
    m2 = re.search(r"(?:document|id|passport)\s*#?\s*[:\s]*([A-Z0-9]{6,})", text, re.I)
    if m2:
        doc_id = m2.group(1)
    return {"date_of_birth": dob, "document_id": doc_id}


def extract_fields(text: str, doc_type_hint: str | None = None) -> Dict[str, Any]:
    """
    Return structure:
    { doc_type, fields, confidence }
    """
    inferred, conf = _detect_doc_type(text)
    doc_type = doc_type_hint if doc_type_hint and doc_type_hint != "unknown" else inferred
    if doc_type == "resume":
        fields = _resume_fields(text)
    elif doc_type == "invoice":
        fields = _invoice_fields(text)
    elif doc_type == "kyc":
        fields = _kyc_fields(text)
    else:
        fields = {"raw_preview": text[:500]}

    return {"doc_type": doc_type, "fields": fields, "confidence": round(conf, 4)}


def build_result_payload(
    doc_id: str,
    text: str,
    doc_type_hint: str | None,
    ml: Dict[str, Any] | None = None,
    layout_blocks: list | None = None,
    ocr_backend: str | None = None,
) -> Dict[str, Any]:
    ex = extract_fields(text, doc_type_hint)
    fields = ex["fields"]
    if ml and ml.get("entities"):
        try:
            from ml.comprehend_entities import merge_entities_into_fields

            fields = merge_entities_into_fields(fields, ml["entities"])
        except Exception:
            pass
    out: Dict[str, Any] = {
        "doc_id": doc_id,
        "doc_type": ex["doc_type"],
        "fields": fields,
        "confidence": ex["confidence"],
    }
    if ocr_backend:
        out["ocr"] = {"backend": ocr_backend}
    if ml:
        out["ml"] = {
            "entities": (ml.get("entities") or [])[:50],
            "key_phrases": (ml.get("key_phrases") or [])[:30],
        }
    if layout_blocks:
        out["layout"] = {
            "line_count": len(layout_blocks),
            "lines": layout_blocks[:40],
        }
    return out
