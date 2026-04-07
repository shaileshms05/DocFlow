"""Kafka producer for document and feedback topics."""

from __future__ import annotations

import os
from typing import Optional

import yaml
from kafka import KafkaProducer

_PRODUCER: Optional[KafkaProducer] = None


def _bootstrap_servers() -> str:
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    with open(os.path.join(root, "config", "config.yaml"), encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    return os.environ.get("KAFKA_BOOTSTRAP_SERVERS", cfg["kafka"]["bootstrap_servers"])


def get_producer() -> KafkaProducer:
    global _PRODUCER
    if _PRODUCER is None:
        _PRODUCER = KafkaProducer(
            bootstrap_servers=_bootstrap_servers().split(","),
            value_serializer=lambda v: v,
            key_serializer=lambda k: k,
        )
    return _PRODUCER


def close_producer():
    global _PRODUCER
    if _PRODUCER is not None:
        _PRODUCER.close()
        _PRODUCER = None
