"""MLflow tracking, registry, and model load helpers."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Optional

import yaml

_ROOT = Path(__file__).resolve().parent.parent


def _cfg() -> dict:
    with open(_ROOT / "config" / "config.yaml", encoding="utf-8") as f:
        return yaml.safe_load(f)


def tracking_uri() -> str:
    return os.environ.get("MLFLOW_TRACKING_URI", _cfg()["mlflow"]["tracking_uri"])


def experiment_name() -> str:
    return os.environ.get("MLFLOW_EXPERIMENT_NAME", _cfg()["mlflow"]["experiment_name"])


def registered_model_name() -> str:
    return os.environ.get(
        "MLFLOW_REGISTERED_MODEL_NAME", _cfg()["mlflow"]["registered_model_name"]
    )


def setup_tracking():
    import mlflow

    mlflow.set_tracking_uri(tracking_uri())
    mlflow.set_experiment(experiment_name())


def log_sklearn_run(
    model: Any,
    params: dict,
    metrics: dict,
    artifact_subdir: str = "model",
):
    import mlflow.sklearn

    setup_tracking()
    with mlflow.start_run():
        for k, v in params.items():
            mlflow.log_param(k, v)
        for k, v in metrics.items():
            mlflow.log_metric(k, float(v))
        mlflow.sklearn.log_model(model, artifact_subdir)
        run_id = mlflow.active_run().info.run_id
    return run_id


def register_model_from_run(run_id: str, stage: str = "Staging") -> Optional[str]:
    """Register sklearn model artifact from run; transition to Staging or Production."""
    import mlflow
    from mlflow.tracking import MlflowClient

    setup_tracking()
    model_uri = f"runs:/{run_id}/model"
    name = registered_model_name()
    mv = mlflow.register_model(model_uri, name)
    client = MlflowClient()
    try:
        client.transition_model_version_stage(
            name,
            str(mv.version),
            stage=stage,
            archive_existing_versions=False,
        )
    except Exception:
        pass
    return str(mv.version)


def load_production_model():
    """Load latest model in Production stage; None if missing."""
    import mlflow.sklearn

    setup_tracking()
    name = registered_model_name()
    try:
        return mlflow.sklearn.load_model(f"models:/{name}/Production")
    except Exception:
        try:
            return mlflow.sklearn.load_model(f"models:/{name}/Staging")
        except Exception:
            return None
