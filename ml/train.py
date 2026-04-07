"""Train doc-type classifier from processed documents + feedback; log to MLflow."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import yaml
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline

from ml.dataset_builder import build_xy
from ml.registry_bridge import log_sklearn_run, register_model_from_run


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--register", action="store_true", help="Register model to MLflow after train")
    parser.add_argument("--stage", default="Staging", choices=["Staging", "Production"])
    args = parser.parse_args()

    with open(_ROOT / "config" / "config.yaml", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    min_samples = int(os.environ.get("MIN_FEEDBACK_SAMPLES", cfg["training"]["min_feedback_samples"]))

    X, y = build_xy()
    if len(X) < min_samples:
        print(f"Not enough samples ({len(X)} < {min_samples}); skipping training.")
        return

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.25, random_state=42, stratify=y if len(set(y)) > 1 else None
    )
    model = Pipeline(
        [
            ("tfidf", TfidfVectorizer(max_features=4096, ngram_range=(1, 2))),
            ("clf", LogisticRegression(max_iter=200, class_weight="balanced")),
        ]
    )
    model.fit(X_train, y_train)
    pred = model.predict(X_test)
    acc = float(accuracy_score(y_test, pred))

    params = {"model": "logistic_tfidf", "samples": len(X)}
    metrics = {"accuracy": acc, "loss": 1.0 - acc}
    run_id = log_sklearn_run(model, params, metrics)

    print(f"accuracy={acc:.4f} run_id={run_id}")

    if args.register and run_id:
        register_model_from_run(run_id, stage=args.stage)


if __name__ == "__main__":
    main()
