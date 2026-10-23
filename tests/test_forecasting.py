from pathlib import Path

import numpy as np

from buildingpilot.artifacts import load_current, promote
from buildingpilot.features import FORECAST_FEATURES
from buildingpilot.forecasting import NeuralForecast, regression_metrics, train_forecasts
from buildingpilot.research_data import synthetic_experiment


def test_neural_training_and_walk_forward(tmp_path: Path) -> None:
    frame = synthetic_experiment(days=3)
    metadata = train_forecasts(frame, tmp_path, epochs=5)
    assert set(metadata["validation_mae"]) == {"persistence", "sklearn", "flax"}
    for folds in metadata["walk_forward"].values():
        assert len(folds) == 3
        assert all(fold["train_end"] < fold["validation_start"] for fold in folds)
    assert metadata["test"]["mae"] >= 0
    promote(tmp_path, "forecast", metadata["version"])
    model, _ = load_current(tmp_path, "forecast")
    prediction = model.predict(
        frame.drop_nulls(FORECAST_FEATURES).select(FORECAST_FEATURES).head(3).to_numpy()
    )
    assert np.isfinite(prediction).all()
    x = np.arange(60, dtype=float).reshape(20, 3)
    y = x[:, 0] * 2
    neural = NeuralForecast(epochs=10).fit(x, y)
    assert neural.params is not None
    assert np.isfinite(neural.predict(x)).all()
    assert regression_metrics([1, 2], [1, 2])["rmse"] == 0
