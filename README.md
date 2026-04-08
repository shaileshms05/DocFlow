# Multi-Modal Document Intelligence Pipeline

This project implements a scalable multi-modal document intelligence system using Kafka and Spark for real-time processing, OCR and NLP for structured extraction, and MLflow for continuous model training and versioning. The system supports resumes, KYC documents, and invoices, with a feedback-driven learning loop.

## Architecture (high level)

1. **Upload** — `POST /upload` saves the file (local or S3), writes an **ingest** manifest to **`S3_BUCKET`** at prefix **`ingest/`** (or `data/ingest/`), emits to Kafka `document_uploads`.
2. **Stream processing** — Consumer reads `document_uploads`, loads `file_path`, writes **processed** JSON to the **same bucket** at **`processed/`**, updates **`ingest/`** with `processed_uri`, emits to `processed_documents`.
3. **Feedback** — `POST /feedback` writes JSON to **`feedback/`** on the output bucket (or `data/feedback/`) and optionally emits to `feedback_events`.
4. **Training** — `python -m ml.train` lists processed objects from S3 (or local disk), merges `doc_type` overrides from stored feedback, logs to MLflow (SQLite backend in Docker).
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
├── storage/            # s3_utils (S3 + local; no SQL)
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

3. Start **Kafka** (or use Docker Compose below). For S3, set **`S3_BUCKET`** and **`AWS_REGION`** (one bucket; paths `raw/`, `processed/`, `ingest/`, `feedback/`).
4. Set **`KAFKA_BOOTSTRAP_SERVERS`**, **`MLFLOW_TRACKING_URI`**, and AWS vars as needed.
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

7. **Upload** a file (multipart). Response includes `file_path` (`s3://…` or `file://…`), `storage`, and `kafka_topic`:

```bash
curl -s -X POST "http://localhost:8000/upload" -F "file=@/path/to/resume.pdf" -F "doc_type=unknown"
```

For **S3 → Kafka → worker**, set **`S3_BUCKET`** (and `AWS_REGION`). Uploads go to `s3://<bucket>/<s3_raw_prefix>/…` (default `raw/`). Legacy env vars **`S3_OUTPUT_BUCKET`** / **`S3_INPUT_BUCKET`** still resolve to the same bucket if **`S3_BUCKET`** is unset.

Open **`http://localhost:8000/docs`** for the interactive OpenAPI UI.

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

This starts **Kafka** (KRaft, no Zookeeper), **MLflow** (SQLite on `mlflow_data`), **API**, and **consumer**. **No Postgres.** Set **`S3_BUCKET`** and **`AWS_REGION`** in `docker/.env` (or on the host) for one-bucket storage; otherwise everything uses the **`app_data`** volume at `/data`.

If **Kafka exits immediately** after switching KRaft settings, remove old metadata and retry: `docker compose down -v` (drops volumes; only the `kafka_data` layout must match `CLUSTER_ID` in `docker-compose.yml`).

| Service   | URL / port |
|-----------|------------|
| API       | `http://localhost:8000` |
| MLflow UI | `http://localhost:5000` |
| Kafka     | `localhost:9092` from your machine; `kafka:29092` inside the Compose network |

**Upload from the host** (same as local quick start):

```bash
curl -s -X POST "http://localhost:8000/upload" -F "file=@/path/to/resume.pdf" -F "doc_type=unknown"
```

**Train inside Docker** (after enough processed JSON objects exist in S3 or `/data`; MLflow must be reachable):

```bash
docker compose exec consumer python -m ml.train --register --stage Staging
```

Spark JVM is still optional: the Compose stack uses the lightweight Kafka consumer. For a full Spark job, run `spark-submit` on a cluster or add a Spark image separately; see `streaming/spark_job.py`.

### S3 (single bucket, path prefixes)

Set **`S3_BUCKET`** (or `storage.s3_bucket` in `config.yaml`). The repo default bucket name is **`doc-flow-bucket`**; change it in YAML or env for other environments. All objects use that one bucket:

| Prefix (configurable) | Contents |
|----------------------|----------|
| `raw/` (`storage.s3_raw_prefix`) | Uploaded PDFs / images |
| `processed/` | Extraction JSON |
| `ingest/` | Upload / pipeline audit |
| `feedback/` | Feedback events |

IAM: `s3:PutObject`, `s3:GetObject`, `s3:ListBucket` on `arn:aws:s3:::<bucket>` and `arn:aws:s3:::<bucket>/*`.

`docker/.env` (copy from `docker/.env.example`):

```env
S3_BUCKET=doc-flow-bucket
AWS_REGION=eu-north-1
```

Legacy **`S3_OUTPUT_BUCKET`** / **`S3_INPUT_BUCKET`** are still read if **`S3_BUCKET`** is empty (they should point at the **same** bucket for this layout).

## Running on AWS EC2

Yes. The stack is plain **Docker Compose**, so any **x86_64** (or **arm64** if you use arm-compatible images) Linux host works. EC2 is a common choice.

### Suggested instance

| Workload | Instance (example) | Notes |
|----------|-------------------|--------|
| Demo / low traffic | `t3.large` or `t3.xlarge` | 2–4 vCPU, 8–16 GiB RAM; Kafka + MLflow need headroom |
| Heavier OCR / concurrency | `m6i.xlarge` or larger | More CPU helps Tesseract and parallel uploads |

Use a **gp3** EBS root (or data) volume of at least **30–50 GiB** so Docker images, volumes, and MLflow artifacts do not fill the disk.

### Security group (important)

- **SSH (22)** or use **SSM Session Manager** and skip opening 22 to the internet.
- **8000** — only for the FastAPI app; restrict to **your IP**, a **VPN**, or an **Application Load Balancer** security group. Do **not** leave `0.0.0.0/0` on 8000 in production without TLS and auth in front.
- **MLflow (5000)** has **no authentication** in this demo. The EC2 Compose file publishes it only on **127.0.0.1:5000** on the instance; use **SSH port forwarding** for the UI:  
  `ssh -L 5000:127.0.0.1:5000 ubuntu@<ec2-ip>`
- **Do not** expose **9092** (Kafka) to the public internet. On EC2, `docker-compose.ec2.yml` does **not** publish Kafka to the host. Your security group should still avoid wide-open rules on **8000**.

### Steps on the instance

1. **Copy or clone** this repo onto the EC2 host (e.g. `git clone …` or `scp` a tarball).
2. Install Docker (Ubuntu):

```bash
cd document-intelligence-system
bash scripts/ec2-bootstrap.sh
# log out and SSH back in so the `docker` group applies
```

3. Start the **EC2-oriented** Compose file (Kafka in **KRaft** mode — no Zookeeper — is **not** published to the host; only the API is on `:8000`). MLflow is on **127.0.0.1:5000** only (use SSH port forwarding for the UI).

```bash
cd docker
docker compose -f docker-compose.ec2.yml up -d --build
```

4. From your laptop (after the security group allows **8000** from your IP, or via ALB):

```bash
curl -s "http://<EC2_PUBLIC_IP>:8000/health"
```

5. **MLflow UI** from your laptop:

```bash
ssh -L 5000:127.0.0.1:5000 ubuntu@<EC2_PUBLIC_IP>
# then open http://127.0.0.1:5000
```

6. **Training** on the instance (SSH session):

```bash
cd docker
docker compose -f docker-compose.ec2.yml exec consumer python -m ml.train --register --stage Staging
```

Optional: attach an **Elastic IP** for a stable public address, and terminate **HTTPS** at **ALB + ACM** with targets on port 8000.

**ARM (Graviton) EC2:** use an **arm64** Ubuntu AMI. The Confluent Kafka and app images must support `arm64` (they do on recent tags); if a service fails to pull, switch to an **x86_64** instance or pin multi-arch images explicitly.

### GitHub Actions

| Workflow | When | What |
|----------|------|------|
| [`.github/workflows/ci.yml`](.github/workflows/ci.yml) | Pull requests to **`main`** | `pip install -r requirements.txt` + `python -m compileall` |
| [`.github/workflows/deploy-ec2.yml`](.github/workflows/deploy-ec2.yml) | Push to **`main`**, or **Run workflow** | Runs **verify** first, then SSH deploy |

**Deploy** SSHs to EC2, runs **`git fetch` / `reset --hard`**, then **`docker compose`**. The instance must have **Docker Engine** and the **Compose v2 plugin** (`docker compose`). If the workflow fails with **`docker: command not found`**, install once over SSH: `curl -fsSL https://get.docker.com | sudo sh`, then `sudo usermod -aG docker $USER` and **log out and back in** (or use `scripts/ec2-bootstrap.sh`). The deploy directory **must be a `git clone`** (contain **`.git`**).

**Repository secrets** (Settings → Secrets and variables → Actions):

| Secret | Required | Description |
|--------|----------|-------------|
| `EC2_HOST` | Yes | Public DNS or IP |
| `EC2_USERNAME` | Yes | e.g. `ubuntu`, `ec2-user` |
| `EC2_SSH_PRIVATE_KEY` | Yes | Private key for `authorized_keys` on the instance (use a **deploy-only** key) |
| `EC2_DEPLOY_PATH` | No | Absolute path to the repo (default: `/home/<EC2_USERNAME>/document-intelligence-system`) |
| `EC2_SSH_PORT` | No | SSH port (default `22`) |
| `EC2_DOCKER_ENV` | No | Multiline **`docker/.env`**, e.g. `S3_BUCKET=...`, `AWS_REGION=...`. Omit if you manage `.env` only on the instance. |

**SSH from GitHub-hosted runners:** Allow **SSH** from [GitHub `actions` IP ranges](https://api.github.com/meta), or use a **bastion / VPN**, or a **self-hosted** runner on EC2.

The deploy **smoke test** (`curl` … `/health`) is **non-blocking** if port 8000 is not reachable from GitHub.

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

- **Data engineering** — Kafka, Spark Streaming, S3 object storage, event-driven processing.
- **MLOps** — MLflow tracking, registry (Staging/Production), retraining from feedback.
- **AI** — OCR (Tesseract), heuristic + regex extraction, optional sklearn classifier.
- **System design** — Decoupled ingest, stream processing, and training/serving paths.
