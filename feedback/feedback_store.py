"""Feedback events: S3 (or local data/feedback); optional Kafka emit. No database."""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone
from typing import List, Optional

from storage.s3_utils import save_feedback_to_output


def init_feedback_tables():
    """Legacy no-op (previously SQLAlchemy)."""
    return


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
        "ts": datetime.now(timezone.utc).isoformat(),
    }
    save_feedback_to_output(event)
    if emit_kafka:
        try:
            import sys
            from pathlib import Path

            import yaml

            root = Path(__file__).resolve().parent.parent
            if str(root) not in sys.path:
                sys.path.insert(0, str(root))
            from ingestion.kafka_producer import get_producer

            prod = get_producer()
            with open(root / "config" / "config.yaml", encoding="utf-8") as f:
                topic = yaml.safe_load(f)["kafka"]["topic_feedback_events"]
            prod.send(
                topic,
                key=doc_id.encode("utf-8"),
                value=json.dumps(event).encode("utf-8"),
            )
            prod.flush()
        except Exception:
            pass
    return event


def list_feedback_for_training(limit: Optional[int] = None) -> List[dict]:
    from storage.s3_utils import iter_feedback_payloads

    rows = sorted(iter_feedback_payloads(), key=lambda r: r.get("ts") or "")
    if limit:
        rows = rows[:limit]
    return [
        {
            "doc_id": r["doc_id"],
            "field": r["field"],
            "predicted": r.get("predicted"),
            "actual": r["actual"],
        }
        for r in rows
    ]
