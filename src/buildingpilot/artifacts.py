"""Immutable candidate artifacts and atomic model pointers; load only trusted local files."""

import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
from typing import Any

import joblib


def artifact_root() -> Path:
    return Path(os.getenv("BP_MODEL_ROOT", "data/models"))


def code_fingerprint() -> str:
    return hashlib.sha256(
        b"".join(path.read_bytes() for path in sorted(Path(__file__).parent.glob("*.py")))
    ).hexdigest()


def save_candidate(root: Path, family: str, model: Any, metadata: dict[str, Any]) -> str:
    metadata.setdefault("code_hash", code_fingerprint())
    metadata.setdefault(
        "libraries",
        {
            name: importlib.metadata.version(name)
            for name in ("numpy", "scikit-learn", "jax", "flax", "optax")
        },
    )
    encoded = json.dumps(metadata, sort_keys=True, allow_nan=False).encode()
    version = hashlib.sha256(encoded).hexdigest()[:20]
    directory = root / family / version
    if (directory / "model.joblib").exists() and (directory / "metadata.json").exists():
        return version
    directory.mkdir(parents=True, exist_ok=True)
    temporary = directory / "model.tmp"
    joblib.dump(model, temporary)
    temporary.replace(directory / "model.joblib")
    (directory / "metadata.json").write_bytes(encoded)
    return version


def promote(root: Path, family: str, version: str) -> None:
    if not (root / family / version / "metadata.json").exists():
        raise FileNotFoundError("Unknown model candidate")
    pointer = root / family / "current.json"
    if pointer.exists():
        if json.loads(pointer.read_text(encoding="utf-8"))["version"] == version:
            return
        (root / family / "previous.json").write_bytes(pointer.read_bytes())
    temporary = pointer.with_suffix(".tmp")
    temporary.write_text(json.dumps({"version": version}), encoding="utf-8")
    temporary.replace(pointer)


def load_current(root: Path, family: str) -> tuple[Any, dict[str, Any]]:
    pointer = json.loads((root / family / "current.json").read_text(encoding="utf-8"))
    version = pointer["version"]
    if (
        not isinstance(version, str)
        or len(version) != 20
        or not all(c in "0123456789abcdef" for c in version)
    ):
        raise ValueError("Invalid artifact version")
    directory = root / family / version
    metadata = json.loads((directory / "metadata.json").read_text(encoding="utf-8"))
    metadata["version"] = version
    return joblib.load(directory / "model.joblib"), metadata
