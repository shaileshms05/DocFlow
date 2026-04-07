"""OCR: Tesseract-based text extraction (images and PDF pages)."""

from __future__ import annotations

import io
import os
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Tuple

import yaml

_BOXES_TYPE = List[Dict[str, Any]]


def _config() -> dict:
    root = Path(__file__).resolve().parent.parent
    with open(root / "config" / "config.yaml", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _setup_tesseract():
    cfg = _config().get("ocr", {})
    cmd = os.environ.get("TESSERACT_CMD", cfg.get("tesseract_cmd") or "")
    if cmd:
        import pytesseract

        pytesseract.pytesseract.tesseract_cmd = cmd


def extract_text(file_path: str) -> Tuple[str, _BOXES_TYPE]:
    """
    Read file from local path or file:// URI; return (full_text, bounding_boxes).
    Bounding boxes are best-effort from Tesseract TSV when available.
    """
    path = file_path
    if file_path.startswith("file://"):
        path = file_path[7:]
    raw = Path(path).read_bytes()
    return extract_text_from_bytes(raw, Path(path).suffix.lower())


def extract_text_from_bytes(data: bytes, suffix: str = ".png") -> Tuple[str, _BOXES_TYPE]:
    """Dispatch by extension: PDF vs image."""
    _setup_tesseract()
    import pytesseract
    from PIL import Image

    boxes: _BOXES_TYPE = []
    texts: List[str] = []

    lang = os.environ.get("OCR_LANGUAGE", _config().get("ocr", {}).get("language", "eng"))

    if suffix == ".pdf":
        try:
            from pdf2image import convert_from_bytes
        except ImportError:
            return "[pdf2image not installed; install poppler + pdf2image]", []
        try:
            pages = convert_from_bytes(data, dpi=200)
        except Exception as e:
            return f"[pdf conversion failed: {e}]", []
        for i, pil in enumerate(pages):
            t, b = _ocr_page(pytesseract, pil, lang, page_index=i)
            texts.append(t)
            boxes.extend(b)
        return "\n\n".join(texts), boxes

    try:
        pil = Image.open(io.BytesIO(data)).convert("RGB")
    except Exception:
        return "[unreadable image]", []
    t, b = _ocr_page(pytesseract, pil, lang, page_index=0)
    return t, b


def _ocr_page(pytesseract_mod, pil_image, lang: str, page_index: int) -> Tuple[str, _BOXES_TYPE]:
    text = pytesseract_mod.image_to_string(pil_image, lang=lang) or ""
    boxes: _BOXES_TYPE = []
    try:
        tsv = pytesseract_mod.image_to_data(
            pil_image, lang=lang, output_type=pytesseract_mod.Output.DICT
        )
        n = len(tsv.get("text", []))
        for i in range(n):
            word = (tsv["text"][i] or "").strip()
            if not word:
                continue
            try:
                conf = float(tsv["conf"][i])
            except (ValueError, TypeError):
                conf = -1.0
            if conf < 0:
                continue
            boxes.append(
                {
                    "page": page_index,
                    "text": word,
                    "left": int(tsv["left"][i]),
                    "top": int(tsv["top"][i]),
                    "width": int(tsv["width"][i]),
                    "height": int(tsv["height"][i]),
                    "confidence": conf / 100.0,
                }
            )
    except Exception:
        pass
    return text.strip(), boxes
