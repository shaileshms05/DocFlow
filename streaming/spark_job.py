"""
Kafka → Spark Structured Streaming → OCR → extraction → storage + processed_documents topic.

Run (from project root with PYTHONPATH=.):
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 streaming/spark_job.py

Or use local driver mode:
  python -m streaming.spark_job --local-consumer
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def process_upload_event(event: dict) -> dict:
    """Single-document pipeline: load file, OCR, extract, persist, return payload."""
    from ingestion.kafka_producer import get_producer
    from storage.db import get_session_factory, init_db, mark_processed
    from storage.s3_utils import load_config, read_file_bytes, save_processed_json
    from streaming.extractor import build_result_payload
    from streaming.ocr import extract_text_from_bytes

    init_db()
    doc_id = event["doc_id"]
    file_path = event["file_path"]
    doc_type = event.get("doc_type") or "unknown"

    raw = read_file_bytes(file_path)

    suffix = Path(file_path.split("?")[0]).suffix.lower() or ".png"
    text, _boxes = extract_text_from_bytes(raw, suffix=suffix)
    payload = build_result_payload(doc_id, text, doc_type if doc_type != "unknown" else None)
    json_uri = save_processed_json(doc_id, payload)

    mark_processed(
        get_session_factory()(),
        doc_id=doc_id,
        json_uri=json_uri,
        summary={"doc_type": payload["doc_type"], "confidence": payload["confidence"]},
    )

    cfg = load_config()["kafka"]
    topic_out = os.environ.get("KAFKA_TOPIC_PROCESSED", cfg["topic_processed_documents"])
    try:
        prod = get_producer()
        prod.send(
            topic_out,
            key=doc_id.encode("utf-8"),
            value=json.dumps({**payload, "json_uri": json_uri}).encode("utf-8"),
        )
        prod.flush()
    except Exception:
        pass
    return payload


def _foreach_batch(batch_df, batch_id: int):
    import pyspark.sql.functions as F

    rows = batch_df.select(F.col("value").cast("string").alias("v")).collect()
    for row in rows:
        if not row.v:
            continue
        try:
            event = json.loads(row.v)
            process_upload_event(event)
        except Exception as e:
            print(f"[batch {batch_id}] error: {e}")


def run_spark_streaming():
    import yaml
    from pyspark.sql import SparkSession

    with open(_ROOT / "config" / "config.yaml", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    kafka_cfg = cfg["kafka"]
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", kafka_cfg["bootstrap_servers"])
    topic = os.environ.get("KAFKA_TOPIC_UPLOADS", kafka_cfg["topic_document_uploads"])
    master = os.environ.get("SPARK_MASTER", cfg["spark"]["master"])

    packages = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
    spark = (
        SparkSession.builder.appName(cfg["spark"]["app_name"])
        .master(master)
        .config("spark.jars.packages", packages)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    q = df.writeStream.foreachBatch(_foreach_batch).outputMode("update").start()
    q.awaitTermination()


def run_local_kafka_consumer():
    """Minimal consumer without Spark (dev / CI)."""
    import yaml
    from kafka import KafkaConsumer

    with open(_ROOT / "config" / "config.yaml", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    k = cfg["kafka"]
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", k["bootstrap_servers"])
    topic = os.environ.get("KAFKA_TOPIC_UPLOADS", k["topic_document_uploads"])
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap.split(","),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="doc-intel-local",
        value_deserializer=lambda b: b.decode("utf-8") if b else None,
    )
    print(f"Listening on {topic} @ {bootstrap}")
    for msg in consumer:
        if not msg.value:
            continue
        try:
            event = json.loads(msg.value)
            process_upload_event(event)
            print(f"Processed {event.get('doc_id')}")
        except Exception as e:
            print(f"Error: {e}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--local-consumer", action="store_true", help="Use kafka-python instead of Spark")
    args = p.parse_args()
    if args.local_consumer:
        run_local_kafka_consumer()
    else:
        run_spark_streaming()


if __name__ == "__main__":
    main()
