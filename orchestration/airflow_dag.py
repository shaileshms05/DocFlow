"""
Airflow DAG: collect feedback-aware data, train, evaluate, register model.

Place this file in Airflow's dags/ folder or set AIRFLOW__CORE__DAGS_FOLDER.
Requires Airflow 2.x and the project on PYTHONPATH.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Project root containing `ml`, `config`, etc.
PROJECT_ROOT = os.environ.get(
    "DOC_INTEL_ROOT",
    os.path.join(os.path.dirname(__file__), ".."),
)


def _fail_if_no_samples():
    import sys

    root = os.path.abspath(PROJECT_ROOT)
    if root not in sys.path:
        sys.path.insert(0, root)
    from ml.dataset_builder import build_xy

    X, _ = build_xy()
    if len(X) < 3:
        raise ValueError("Not enough processed documents to train (need >= 3).")


default_args = {
    "owner": "doc-intel",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="document_intelligence_training",
    default_args=default_args,
    description="Train doc-type model and register in MLflow",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ml", "doc-intel"],
) as dag:
    sanity = PythonOperator(
        task_id="check_training_samples",
        python_callable=_fail_if_no_samples,
    )

    train = BashOperator(
        task_id="train_and_register",
        bash_command=(
            f'cd "{PROJECT_ROOT}" && '
            f'export PYTHONPATH="{PROJECT_ROOT}" && '
            f'python -m ml.train --register --stage Staging'
        ),
    )

    sanity >> train
