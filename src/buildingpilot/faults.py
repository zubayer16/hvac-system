"""Chronological supervised fault experiments with training-only preprocessing."""

import hashlib
import importlib.metadata
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl
from sklearn.dummy import DummyClassifier
from sklearn.ensemble import (
    ExtraTreesClassifier,
    GradientBoostingClassifier,
    RandomForestClassifier,
)
from sklearn.impute import SimpleImputer
from sklearn.metrics import confusion_matrix, f1_score, precision_score, recall_score, roc_auc_score
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from buildingpilot.artifacts import save_candidate
from buildingpilot.features import FAULT_FEATURES


def time_split(frame: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    times = frame["timestamp"].unique().sort().to_list()
    if len(times) < 10:
        raise ValueError("At least ten distinct timestamps required")
    a, b = times[int(len(times) * 0.6)], times[int(len(times) * 0.8)]
    return (
        frame.filter(pl.col("timestamp") < a),
        frame.filter((pl.col("timestamp") >= a) & (pl.col("timestamp") < b)),
        frame.filter(pl.col("timestamp") >= b),
    )


def classification_metrics(model: Any, frame: pl.DataFrame) -> dict[str, Any]:
    y = frame["faulty"].to_numpy()
    x = frame.select(FAULT_FEATURES).to_numpy()
    predictions = model.predict(x)
    tn, fp, fn, tp = confusion_matrix(y, predictions, labels=[0, 1]).ravel()
    auc = None
    if len(np.unique(y)) == 2 and len(model.classes_) == 2:
        auc = float(roc_auc_score(y, model.predict_proba(x)[:, list(model.classes_).index(1)]))
    return {
        "precision": float(precision_score(y, predictions, zero_division=0)),
        "recall": float(recall_score(y, predictions, zero_division=0)),
        "f1": float(f1_score(y, predictions, zero_division=0)),
        "roc_auc": auc,
        "false_alarm_rate": float(fp / (tn + fp)) if tn + fp else None,
        "confusion_matrix": [[int(tn), int(fp)], [int(fn), int(tp)]],
        "samples": len(y),
    }


def train_faults(frame: pl.DataFrame, root: Path, snapshot: int | None = None) -> dict[str, Any]:
    if frame["source"].n_unique() != 1:
        raise ValueError("Train/evaluate each provenance separately")
    train, validation, test = time_split(frame)
    if train["faulty"].n_unique() < 2:
        raise ValueError("Training requires both normal and fault labels")
    candidates = {
        "dummy": DummyClassifier(strategy="most_frequent"),
        "extra_trees": ExtraTreesClassifier(n_estimators=80, random_state=42, n_jobs=1),
        "random_forest": RandomForestClassifier(n_estimators=80, random_state=42, n_jobs=1),
        "gradient_boosting": GradientBoostingClassifier(n_estimators=60, random_state=42),
    }
    models: dict[str, Any] = {}
    scores: dict[str, Any] = {}
    for name, estimator in candidates.items():
        model = make_pipeline(
            SimpleImputer(strategy="median", keep_empty_features=True), StandardScaler(), estimator
        )
        model.fit(train.select(FAULT_FEATURES).to_numpy(), train["faulty"].to_numpy())
        models[name] = model
        scores[name] = classification_metrics(model, validation)
    winner = max(scores, key=lambda name: scores[name]["f1"])
    model = models[winner]
    estimator = model[-1]
    importance = dict(
        zip(
            FAULT_FEATURES,
            [
                float(v)
                for v in getattr(estimator, "feature_importances_", np.zeros(len(FAULT_FEATURES)))
            ],
            strict=True,
        )
    )
    digest = hashlib.sha256(frame.sort(["timestamp", "event_id"]).write_json().encode()).hexdigest()
    metadata = {
        "family": "fault",
        "winner": winner,
        "seed": 42,
        "source": frame["source"][0],
        "dataset_hash": digest,
        "snapshot_id": snapshot,
        "sklearn_version": importlib.metadata.version("scikit-learn"),
        "features": FAULT_FEATURES,
        "validation": scores,
        "test": classification_metrics(model, test),
        "test_by_building": {
            key[0]: classification_metrics(model, group)
            for key, group in test.group_by("building_id")
        },
        "test_by_condition": {
            key[0]: classification_metrics(model, group)
            for key, group in test.group_by("operating_status")
        },
        "feature_importance": importance,
        "split": {
            "train_end": str(train["timestamp"].max()),
            "validation_start": str(validation["timestamp"].min()),
            "validation_end": str(validation["timestamp"].max()),
            "test_start": str(test["timestamp"].min()),
        },
    }
    metadata["version"] = save_candidate(root, "fault", model, metadata)
    return metadata


def main() -> None:
    import argparse
    import json

    parser = argparse.ArgumentParser()
    parser.add_argument("features", type=Path)
    parser.add_argument("--models", type=Path, default=Path("data/models"))
    args = parser.parse_args()
    print(json.dumps(train_faults(pl.read_parquet(args.features), args.models)))


if __name__ == "__main__":
    main()
