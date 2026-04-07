# Multi-Modal Document Intelligence Pipeline

This project implements a scalable multi-modal document intelligence system using Kafka and Spark for real-time processing, OCR and NLP for structured extraction, and MLflow for continuous model training and versioning. The system supports resumes, KYC documents, and invoices, with a feedback-driven learning loop.

## Architecture (high level)

1. **Upload** — `POST /upload` saves the file (local or S3), writes metadata to PostgreSQL, emits a JSON event to Kafka topic `document_uploads`.
2. **Stream processing** — Spark Structured Streaming (or a local Kafka consumer) reads `document_uploads`, runs OCR and rule-based extraction, writes JSON to processed storage, updates PostgreSQL, emits to `processed_documents`.
3. **Feedback** — `POST /feedback` stores corrections and optionally emits to `feedback_events`.
4. **Training** — `python -m ml.train` builds a dataset from processed JSON plus `doc_type` feedback overrides, trains a TF-IDF + logistic model, logs params/metrics/artifacts to MLflow, and can register a model version.
5. **Serving** — `ml.predict` loads the latest **Production**/**Staging** model from the MLflow Model Registry when available; otherwise falls back to rules.

## Layout

```
document-intelligence-system/
├── ingestion/          # FastAPI + Kafka producer
├── streaming/          # Spark job, OCR, extractor
├── ml/                 # train, predict, dataset_builder, registry_bridge
├── mlflow/             # mlflow_utils (loaded via ml/registry_bridge.py)
├── feedback/           # feedback_store
├── orchestration/      # Airflow DAG (install Airflow separately)
├── storage/            # s3_utils, db
├── config/config.yaml
├── docker/
└── requirements.txt
```

## Quick start (local)

1. **Python 3.11+**, **Tesseract**, and (for PDFs) **Poppler** installed on the host.
2. Install dependencies:

```bash
cd document-intelligence-system
pip install -r requirements.txt
```

3. Start **PostgreSQL** and **Kafka** (or use Docker Compose below).
4. Set environment variables if needed, e.g. `DATABASE_URL`, `KAFKA_BOOTSTRAP_SERVERS`, `MLFLOW_TRACKING_URI`.
5. Run the API:

```bash
export PYTHONPATH="$(pwd)"
uvicorn ingestion.api:app --reload --host 0.0.0.0 --port 8000
```

6. Run the consumer (without Spark — good for dev):

```bash
export PYTHONPATH="$(pwd)"
python -m streaming.spark_job --local-consumer
```

7. **Upload** a file:

```bash
curl -s -X POST "http://localhost:8000/upload" -F "file=@/path/to/resume.pdf" -F "doc_type=unknown"
```

8. **Feedback** (example):

```bash
curl -s -X POST "http://localhost:8000/feedback" \
  -H "Content-Type: application/json" \
  -d '{"doc_id":"<uuid>","field":"doc_type","predicted":"resume","actual":"invoice"}'
```

9. **Train** (requires enough processed rows; see `config.yaml` → `training.min_feedback_samples`):

```bash
export PYTHONPATH="$(pwd)"
python -m ml.train --register --stage Staging
```

## Run everything in Docker (recommended)

From `document-intelligence-system/docker`:

```bash
cd docker
docker compose up --build
```

This starts **Zookeeper**, **Kafka**, **PostgreSQL**, **MLflow**, the **FastAPI** app, and a **`consumer`** container that runs `python -m streaming.spark_job --local-consumer` (OCR + extraction + DB + `processed_documents` topic). The API and consumer share the **`app_data`** volume at `/data`, so uploads written by the API are visible to the worker.

| Service   | URL / port |
|-----------|------------|
| API       | `http://localhost:8000` |
| MLflow UI | `http://localhost:5000` |
| Kafka     | `localhost:9092` from your machine; `kafka:29092` inside the Compose network |

**Upload from the host** (same as local quick start):

```bash
curl -s -X POST "http://localhost:8000/upload" -F "file=@/path/to/resume.pdf" -F "doc_type=unknown"
```

**Train inside Docker** (after enough documents are processed; MLflow and Postgres must be reachable):

```bash
docker compose exec consumer python -m ml.train --register --stage Staging
```

Spark JVM is still optional: the Compose stack uses the lightweight Kafka consumer. For a full Spark job, run `spark-submit` on a cluster or add a Spark image separately; see `streaming/spark_job.py`.

## Kafka topics

| Topic                 | Purpose              |
|-----------------------|----------------------|
| `document_uploads`    | New file metadata    |
| `processed_documents` | Extraction results |
| `feedback_events`     | User corrections     |

## JSON output shape

```json
{
  "doc_id": "123",
  "doc_type": "resume",
  "fields": {
    "name": "Shailesh",
    "skills": ["Python", "AWS"]
  },
  "confidence": 0.91
}
```

## Airflow

Install Apache Airflow in a separate environment, copy `orchestration/airflow_dag.py` into your DAGs folder, and set `DOC_INTEL_ROOT` to this project path.

## Note on the `mlflow` folder

The spec keeps helpers at `mlflow/mlflow_utils.py`. Because that path would collide with the PyPI `mlflow` package, application code loads it through `ml/registry_bridge.py` (importlib), which avoids shadowing the official library.

## Interview narrative

- **Data engineering** — Kafka, Spark Streaming, PostgreSQL, object storage abstraction.
- **MLOps** — MLflow tracking, registry (Staging/Production), retraining from feedback.
- **AI** — OCR (Tesseract), heuristic + regex extraction, optional sklearn classifier.
- **System design** — Decoupled ingest, stream processing, and training/serving paths.
