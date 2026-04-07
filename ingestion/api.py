"""FastAPI: document upload, health, and feedback endpoints."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

# Ensure project root on path for `storage`, `feedback`, etc.
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import yaml
from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from pydantic import BaseModel, Field

from feedback.feedback_store import record_feedback
from ingestion.kafka_producer import close_producer, get_producer
from storage.db import get_session_factory, init_db, upsert_document
from storage.s3_utils import load_config, save_upload

app = FastAPI(title="Document Intelligence Ingestion", version="0.1.0")


def _kafka_topics():
    with open(_ROOT / "config" / "config.yaml", encoding="utf-8") as f:
        return yaml.safe_load(f)["kafka"]


@app.on_event("startup")
def startup():
    init_db()
    try:
        from feedback.feedback_store import init_feedback_tables

        init_feedback_tables()
    except Exception:
        pass


@app.on_event("shutdown")
def shutdown():
    close_producer()


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/upload")
async def upload(
    file: UploadFile = File(...),
    doc_type: str = Form("unknown"),
):
    raw = await file.read()
    if not raw:
        raise HTTPException(400, "empty file")
    file_uri, doc_id = save_upload(raw, file.filename or "upload.bin")
    upsert_document(
        get_session_factory()(),
        doc_id=doc_id,
        file_uri=file_uri,
        doc_type_hint=doc_type if doc_type != "unknown" else None,
        status="queued",
    )
    event = {
        "doc_id": doc_id,
        "file_path": file_uri,
        "doc_type": doc_type,
    }
    topics = _kafka_topics()
    try:
        prod = get_producer()
        prod.send(
            topics["topic_document_uploads"],
            key=doc_id.encode("utf-8"),
            value=json.dumps(event).encode("utf-8"),
        )
        prod.flush()
    except Exception as e:
        raise HTTPException(503, f"kafka unavailable: {e}") from e
    return event


class FeedbackBody(BaseModel):
    doc_id: str = Field(..., description="Document UUID")
    field: str
    predicted: str | None = None
    actual: str


@app.post("/feedback")
def submit_feedback(body: FeedbackBody):
    ev = record_feedback(
        body.doc_id,
        body.field,
        body.predicted,
        body.actual,
        emit_kafka=True,
    )
    return {"accepted": True, "event": ev}


def main():
    cfg = load_config()
    host = os.environ.get("APP_HOST", cfg["app"]["host"])
    port = int(os.environ.get("APP_PORT", cfg["app"]["port"]))
    import uvicorn

    uvicorn.run("ingestion.api:app", host=host, port=port, reload=False)


if __name__ == "__main__":
    main()
