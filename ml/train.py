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

    # Use a more robust pipeline: Random Forest often outperforms Logistic Regression for this task.
    # We include stop words and sublinear TF scaling to handle variable document lengths.
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.metrics import classification_report, f1_score

    model = Pipeline(
        [
            (
                "tfidf",
                TfidfVectorizer(
                    max_features=8192,  # Increased from 4096
                    ngram_range=(1, 3),  # Include trigrams (e.g. "bill to amount")
                    stop_words="english",
                    sublinear_tf=True,
                ),
            ),
            (
                "clf",
                RandomForestClassifier(
                    n_estimators=200, n_jobs=-1, class_weight="balanced", random_state=42
                ),
            ),
        ]
    )

    print(f"Training on {len(X_train)} samples...")
    model.fit(X_train, y_train)

    pred = model.predict(X_test)
    acc = float(accuracy_score(y_test, pred))
    f1 = float(f1_score(y_test, pred, average="weighted"))

    print(f"accuracy={acc:.4f} f1_weighted={f1:.4f}")
    print("\nClassification Report:")
    print(classification_report(y_test, pred))

    # Log more descriptive metrics to MLflow for better analysis.
    params = {
        "model_type": "random_forest_tfidf",
        "vectorizer_ngram_range": (1, 3),
        "total_samples": len(X),
    }
    metrics = {"accuracy": acc, "f1_weighted": f1}
    run_id = log_sklearn_run(model, params, metrics)

    if args.register and run_id:
        register_model_from_run(run_id, stage=args.stage)


if __name__ == "__main__":
    main()
