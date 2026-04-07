"""Store feedback events (DB + optional Kafka emit)."""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime
from typing import Any, List, Optional

import yaml
from sqlalchemy import Column, DateTime, Integer, String, Text, create_engine
from sqlalchemy.orm import Session, declarative_base, sessionmaker

Base = declarative_base()


class FeedbackEvent(Base):
    __tablename__ = "feedback_events"

    id = Column(Integer, primary_key=True, autoincrement=True)
    event_id = Column(String(64), unique=True, nullable=False)
    doc_id = Column(String(64), nullable=False, index=True)
    field = Column(String(128), nullable=False)
    predicted = Column(Text, nullable=True)
    actual = Column(Text, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)


def _root() -> str:
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _db_url() -> str:
    with open(os.path.join(_root(), "config", "config.yaml"), encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    return os.environ.get("DATABASE_URL", cfg["database"]["url"])


_engine = None
_SessionLocal = None


def _get_session_factory():
    global _engine, _SessionLocal
    if _SessionLocal is None:
        _engine = create_engine(_db_url(), pool_pre_ping=True)
        Base.metadata.create_all(bind=_engine)
        _SessionLocal = sessionmaker(bind=_engine, autocommit=False, autoflush=False)
    return _SessionLocal


def init_feedback_tables():
    engine = create_engine(_db_url(), pool_pre_ping=True)
    Base.metadata.create_all(bind=engine)


def record_feedback(
    doc_id: str,
    field: str,
    predicted: Optional[str],
    actual: str,
    emit_kafka: bool = True,
) -> dict:
    event_id = str(uuid.uuid4())
    event = {
        "event_id": event_id,
        "doc_id": doc_id,
        "field": field,
        "predicted": predicted,
        "actual": actual,
        "ts": datetime.utcnow().isoformat() + "Z",
    }
    SessionLocal = _get_session_factory()
    with SessionLocal() as session:
        session.add(
            FeedbackEvent(
                event_id=event_id,
                doc_id=doc_id,
                field=field,
                predicted=predicted or "",
                actual=actual,
            )
        )
        session.commit()
    if emit_kafka:
        try:
            import sys
            from pathlib import Path

            root = Path(__file__).resolve().parent.parent
            if str(root) not in sys.path:
                sys.path.insert(0, str(root))
            from ingestion.kafka_producer import get_producer

            prod = get_producer()
            with open(os.path.join(_root(), "config", "config.yaml"), encoding="utf-8") as f:
                topic = yaml.safe_load(f)["kafka"]["topic_feedback_events"]
            prod.send(
                topic,
                key=doc_id.encode("utf-8"),
                value=json.dumps(event).encode("utf-8"),
            )
            prod.flush()
        except Exception:
            pass  # Kafka optional for local tests
    return event


def list_feedback_for_training(limit: Optional[int] = None) -> List[dict]:
    SessionLocal = _get_session_factory()
    with SessionLocal() as session:
        q = session.query(FeedbackEvent).order_by(FeedbackEvent.created_at.asc())
        if limit:
            q = q.limit(limit)
        rows = q.all()
    return [
        {
            "doc_id": r.doc_id,
            "field": r.field,
            "predicted": r.predicted,
            "actual": r.actual,
        }
        for r in rows
    ]
