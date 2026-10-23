"""Walk-forward forecast comparison with a real differentiable Flax/Optax model."""

import hashlib
from pathlib import Path
from typing import Any

import jax
import jax.numpy as jnp
import numpy as np
import optax
import polars as pl
from flax import linen as nn
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.impute import SimpleImputer
from sklearn.pipeline import make_pipeline

from buildingpilot.artifacts import save_candidate
from buildingpilot.features import FORECAST_FEATURES


class LoadNetwork(nn.Module):
    @nn.compact
    def __call__(self, x: jax.Array) -> jax.Array:
        x = nn.tanh(nn.Dense(24)(x))
        x = nn.tanh(nn.Dense(16)(x))
        return nn.Dense(1)(x).squeeze(-1)


class NeuralForecast:
    """Serializable NumPy parameters; accelerator state is reconstructed for inference."""

    def __init__(self, epochs: int = 80) -> None:
        self.epochs = epochs
        self.params: Any = None
        self.mean: Any = None
        self.scale: Any = None
        self.target_mean = 0.0
        self.target_scale = 1.0

    def fit(self, x: Any, y: Any) -> "NeuralForecast":
        self.mean = np.nanmean(x, axis=0)
        self.mean = np.nan_to_num(self.mean)
        self.scale = np.nan_to_num(np.nanstd(x, axis=0), nan=1.0)
        self.scale[self.scale < 1e-6] = 1
        self.target_mean = float(np.mean(y))
        self.target_scale = max(float(np.std(y)), 1e-6)
        features = jnp.asarray(np.nan_to_num((x - self.mean) / self.scale), dtype=jnp.float32)
        targets = jnp.asarray((y - self.target_mean) / self.target_scale, dtype=jnp.float32)
        network = LoadNetwork()
        params = network.init(jax.random.key(42), features[:1])["params"]
        optimizer = optax.chain(optax.clip_by_global_norm(1), optax.adam(0.01))
        state = optimizer.init(params)

        @jax.jit
        def update(parameters: Any, optimizer_state: Any) -> tuple[Any, Any]:
            def loss(values: Any) -> jax.Array:
                prediction = network.apply({"params": values}, features)
                return jnp.mean(optax.squared_error(prediction, targets))

            gradient = jax.grad(loss)(parameters)
            changes, optimizer_state = optimizer.update(gradient, optimizer_state, parameters)
            return optax.apply_updates(parameters, changes), optimizer_state

        for _ in range(self.epochs):
            params, state = update(params, state)
        self.params = jax.tree.map(np.asarray, params)
        return self

    def predict(self, x: Any) -> Any:
        values = jnp.asarray(np.nan_to_num((x - self.mean) / self.scale), dtype=jnp.float32)
        predictions = LoadNetwork().apply({"params": self.params}, values)
        return np.maximum(0, np.asarray(predictions) * self.target_scale + self.target_mean)


class PersistenceForecast:
    def fit(self, x: Any, y: Any) -> "PersistenceForecast":
        return self

    def predict(self, x: Any) -> Any:
        return np.maximum(0, np.asarray(x[:, 0]))


def regression_metrics(actual: Any, predicted: Any) -> dict[str, float]:
    residual = np.asarray(actual) - np.asarray(predicted)
    return {
        "mae": float(np.mean(np.abs(residual))),
        "rmse": float(np.sqrt(np.mean(residual**2))),
        "bias": float(np.mean(residual)),
    }


def train_forecasts(
    frame: pl.DataFrame, root: Path, epochs: int = 80, snapshot: int | None = None
) -> dict[str, Any]:
    if frame["source"].n_unique() != 1:
        raise ValueError("Do not mix provenance in model evaluation")
    frame = frame.drop_nulls(["lag_load_kw", "lag2_load_kw"])
    times = frame["timestamp"].unique().sort().to_list()
    if len(times) < 20:
        raise ValueError("Forecast training requires at least 20 time points")
    test_start = times[int(len(times) * 0.8)]
    development = frame.filter(pl.col("timestamp") < test_start)
    test = frame.filter(pl.col("timestamp") >= test_start)
    factories: dict[str, Any] = {
        "persistence": PersistenceForecast,
        "sklearn": lambda: make_pipeline(
            SimpleImputer(strategy="median", keep_empty_features=True),
            HistGradientBoostingRegressor(max_iter=60, max_leaf_nodes=15, random_state=42),
        ),
        "flax": lambda: NeuralForecast(epochs),
    }
    scores: dict[str, list[dict[str, Any]]] = {name: [] for name in factories}
    # Three expanding windows; validation is strictly later than training in every fold.
    for fraction in (0.4, 0.55, 0.7):
        start = times[int(len(times) * fraction)]
        stop = times[int(len(times) * (fraction + 0.1))]
        train = development.filter(pl.col("timestamp") < start)
        validation = development.filter(
            (pl.col("timestamp") >= start) & (pl.col("timestamp") < stop)
        )
        for name, factory in factories.items():
            model = factory().fit(
                train.select(FORECAST_FEATURES).to_numpy(), train["load_kw"].to_numpy()
            )
            scores[name].append(
                regression_metrics(
                    validation["load_kw"].to_numpy(),
                    model.predict(validation.select(FORECAST_FEATURES).to_numpy()),
                )
                | {
                    "train_end": str(train["timestamp"].max()),
                    "validation_start": str(validation["timestamp"].min()),
                }
            )
    average = {
        name: float(np.mean([fold["mae"] for fold in folds])) for name, folds in scores.items()
    }
    winner = min(average, key=lambda name: average[name])
    model = factories[winner]().fit(
        development.select(FORECAST_FEATURES).to_numpy(), development["load_kw"].to_numpy()
    )
    test_predictions = model.predict(test.select(FORECAST_FEATURES).to_numpy())
    # Save the trained neural model independently, even when a baseline wins.
    neural = factories["flax"]().fit(
        development.select(FORECAST_FEATURES).to_numpy(), development["load_kw"].to_numpy()
    )
    metadata = {
        "family": "forecast",
        "winner": winner,
        "seed": 42,
        "epochs": epochs,
        "features": FORECAST_FEATURES,
        "target": "interval_load_kw",
        "horizon": "one_interval",
        "source": frame["source"][0],
        "snapshot_id": snapshot,
        "dataset_hash": hashlib.sha256(
            frame.sort(["timestamp", "event_id"]).write_json().encode()
        ).hexdigest(),
        "walk_forward": scores,
        "validation_mae": average,
        "test": regression_metrics(test["load_kw"].to_numpy(), test_predictions),
        "test_by_building": {
            key[0]: regression_metrics(
                group["load_kw"].to_numpy(),
                model.predict(group.select(FORECAST_FEATURES).to_numpy()),
            )
            for key, group in test.group_by("building_id")
        },
        "reference_mean": development.select(FORECAST_FEATURES).mean().row(0),
        "reference_std": development.select(FORECAST_FEATURES).std().row(0),
        "test_start": str(test_start),
        "jax_backend": jax.default_backend(),
    }
    save_candidate(root, "neural", neural, metadata | {"family": "neural", "winner": "flax"})
    metadata["version"] = save_candidate(root, "forecast", model, metadata)
    return metadata


def main() -> None:
    import argparse
    import json

    parser = argparse.ArgumentParser()
    parser.add_argument("features", type=Path)
    parser.add_argument("--models", type=Path, default=Path("data/models"))
    parser.add_argument("--epochs", type=int, default=80)
    args = parser.parse_args()
    print(json.dumps(train_forecasts(pl.read_parquet(args.features), args.models, args.epochs)))


if __name__ == "__main__":
    main()
