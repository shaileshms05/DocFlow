"""PostgreSQL metadata for documents and processing status."""

from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Optional

import yaml
from sqlalchemy import JSON, Column, DateTime, Integer, String, Text, create_engine
from sqlalchemy.orm import Session, declarative_base, sessionmaker

Base = declarative_base()


class DocumentRecord(Base):
    __tablename__ = "documents"

    id = Column(Integer, primary_key=True, autoincrement=True)
    doc_id = Column(String(64), unique=True, nullable=False, index=True)
    file_uri = Column(Text, nullable=False)
    doc_type_hint = Column(String(64), nullable=True)
    status = Column(String(32), default="uploaded")
    processed_uri = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    extra = Column(JSON, nullable=True)


class ProcessedDocumentRecord(Base):
    __tablename__ = "processed_documents"

    id = Column(Integer, primary_key=True, autoincrement=True)
    doc_id = Column(String(64), unique=True, nullable=False, index=True)
    json_uri = Column(Text, nullable=False)
    payload_summary = Column(JSON, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)


def _db_url() -> str:
    root = __file__.rsplit("storage", 1)[0]
    with open(os.path.join(root, "config", "config.yaml"), encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    return os.environ.get("DATABASE_URL", cfg["database"]["url"])


_engine = None
_SessionLocal = None


def get_engine():
    global _engine
    if _engine is None:
        _engine = create_engine(_db_url(), pool_pre_ping=True)
    return _engine


def get_session_factory():
    global _SessionLocal
    if _SessionLocal is None:
        _SessionLocal = sessionmaker(bind=get_engine(), autocommit=False, autoflush=False)
    return _SessionLocal


def init_db():
    Base.metadata.create_all(bind=get_engine())


def upsert_document(
    session: Session,
    doc_id: str,
    file_uri: str,
    doc_type_hint: Optional[str] = None,
    status: str = "uploaded",
) -> DocumentRecord:
    rec = session.query(DocumentRecord).filter_by(doc_id=doc_id).one_or_none()
    if rec:
        rec.file_uri = file_uri
        rec.doc_type_hint = doc_type_hint
        rec.status = status
    else:
        rec = DocumentRecord(
            doc_id=doc_id,
            file_uri=file_uri,
            doc_type_hint=doc_type_hint,
            status=status,
        )
        session.add(rec)
    session.commit()
    session.refresh(rec)
    return rec


def mark_processed(
    session: Session,
    doc_id: str,
    json_uri: str,
    summary: Optional[dict] = None,
):
    doc = session.query(DocumentRecord).filter_by(doc_id=doc_id).one_or_none()
    if doc:
        doc.status = "processed"
        doc.processed_uri = json_uri
        session.add(doc)
    proc = session.query(ProcessedDocumentRecord).filter_by(doc_id=doc_id).one_or_none()
    if proc:
        proc.json_uri = json_uri
        proc.payload_summary = summary
    else:
        session.add(
            ProcessedDocumentRecord(
                doc_id=doc_id, json_uri=json_uri, payload_summary=summary
            )
        )
    session.commit()
