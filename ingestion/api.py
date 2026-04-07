"""FastAPI: upload file → S3 (or local) → Kafka ``document_uploads`` → downstream OCR/extraction."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Literal

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import yaml
from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from pydantic import BaseModel, Field

from feedback.feedback_store import record_feedback
from ingestion.kafka_producer import close_producer, get_producer
from storage.s3_utils import load_config, save_ingest_manifest, save_upload

app = FastAPI(
    title="Document Intelligence Ingestion",
    version="0.3.0",
    description=(
        "**Flow:** `POST /upload` stores the file (S3 when `S3_BUCKET` is set, else local), writes **ingest** "
        "manifest JSON under `ingest/` in that bucket or `data/ingest/`, then publishes to Kafka "
        "`document_uploads`. The consumer loads `file_path`, runs OCR + extraction, and writes **processed** JSON "
        "to the output bucket (or local `data/processed/`)."
    ),
)


class UploadResponse(BaseModel):
    doc_id: str
    file_path: str
    doc_type: str
    storage: Literal["s3", "local"]
    kafka_topic: str
    status: str = Field("queued", description="Recorded in S3 ingest manifest or local data/ingest")


class FeedbackBody(BaseModel):
    doc_id: str
    field: str
    predicted: str | None = None
    actual: str


def _kafka_topics():
    with open(_ROOT / "config" / "config.yaml", encoding="utf-8") as f:
        return yaml.safe_load(f)["kafka"]


@app.on_event("startup")
def startup():
    return


@app.on_event("shutdown")
def shutdown():
    close_producer()


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/upload", response_model=UploadResponse, tags=["ingest"])
async def upload(
    file: UploadFile = File(..., description="PDF or image"),
    doc_type: str = Form("unknown", description="Hint: resume | invoice | kyc | unknown"),
):
    raw = await file.read()
    if not raw:
        raise HTTPException(400, "empty file")
    try:
        file_uri, doc_id = save_upload(raw, file.filename or "upload.bin")
    except ValueError as e:
        raise HTTPException(400, str(e)) from e
    except RuntimeError as e:
        raise HTTPException(503, str(e)) from e

    storage_kind: Literal["s3", "local"] = "s3" if file_uri.startswith("s3://") else "local"

    try:
        save_ingest_manifest(doc_id, file_uri, doc_type, status="queued")
    except Exception:
        pass

    topics = _kafka_topics()
    topic_uploads = topics["topic_document_uploads"]
    event = {
        "doc_id": doc_id,
        "file_path": file_uri,
        "doc_type": doc_type,
    }
    try:
        prod = get_producer()
        prod.send(
            topic_uploads,
            key=doc_id.encode("utf-8"),
            value=json.dumps(event).encode("utf-8"),
        )
        prod.flush()
    except Exception as e:
        raise HTTPException(503, f"kafka unavailable: {e}") from e

    return UploadResponse(
        doc_id=doc_id,
        file_path=file_uri,
        doc_type=doc_type,
        storage=storage_kind,
        kafka_topic=topic_uploads,
        status="queued",
    )


@app.post("/feedback", tags=["feedback"])
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
