"""Load `mlflow/mlflow_utils.py` without shadowing the PyPI `mlflow` package."""

from __future__ import annotations

import importlib.util
from pathlib import Path

_root = Path(__file__).resolve().parent.parent
_path = _root / "mlflow" / "mlflow_utils.py"
_spec = importlib.util.spec_from_file_location("docintel_mlflow_utils", _path)
_mod = importlib.util.module_from_spec(_spec)
assert _spec.loader is not None
_spec.loader.exec_module(_mod)

log_sklearn_run = _mod.log_sklearn_run
register_model_from_run = _mod.register_model_from_run
load_production_model = _mod.load_production_model
setup_tracking = _mod.setup_tracking
tracking_uri = _mod.tracking_uri
