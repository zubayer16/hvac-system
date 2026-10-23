"""Real Iceberg snapshots, with explicit local SQL and shared REST catalog modes."""

import json
import os
from pathlib import Path
from typing import Any

import pyarrow as pa
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.table import Table


class Lakehouse:
    def __init__(self, catalog: Catalog, namespace: str = "buildingpilot") -> None:
        self.catalog = catalog
        self.namespace = namespace
        try:
            catalog.create_namespace(namespace)
        except NamespaceAlreadyExistsError:
            pass

    @classmethod
    def local(cls, root: Path) -> "Lakehouse":
        root = root.resolve()
        root.mkdir(parents=True, exist_ok=True)
        return cls(
            load_catalog(
                "local",
                type="sql",
                uri=f"sqlite:///{root / 'catalog.db'}",
                warehouse=(root / "warehouse").as_uri(),
                **{"py-io-impl": "buildingpilot.arrow_io.PortableArrowFileIO"},
            )
        )

    @classmethod
    def from_env(cls) -> "Lakehouse":
        if os.getenv("BP_LAKE_CATALOG", "local") == "local":
            return cls.local(Path(os.getenv("BP_LAKE_ROOT", "data/lakehouse")))
        return cls(
            load_catalog(
                "shared",
                type="rest",
                uri=os.environ["BP_CATALOG_URI"],
                warehouse="s3://warehouse/",
                **{
                    "s3.endpoint": os.environ["BP_S3_ENDPOINT"],
                    "s3.access-key-id": os.environ["BP_S3_ACCESS_KEY"],
                    "s3.secret-access-key": os.environ["BP_S3_SECRET_KEY"],
                    "s3.region": "us-east-1",
                    "s3.force-virtual-addressing": "false",
                },
            )
        )

    def table(self, name: str) -> Table:
        return self.catalog.load_table(f"{self.namespace}.{name}")

    def ensure_results(self) -> None:
        schema = pa.schema(
            [("result_id", pa.string()), ("building_id", pa.string()), ("payload", pa.string())]
        )
        for name in ("prediction_results", "optimization_recommendations"):
            try:
                self.table(name)
            except NoSuchTableError:
                self.catalog.create_table(f"{self.namespace}.{name}", schema=schema)

    def write(self, name: str, data: pa.Table, keys: list[str] | None = None) -> int:
        if not data.num_rows:
            raise ValueError("Cannot infer an Iceberg schema from an empty dataset")
        try:
            table = self.table(name)
        except NoSuchTableError:
            table = self.catalog.create_table(f"{self.namespace}.{name}", schema=data.schema)
        if keys:
            table.upsert(data, join_cols=keys)
        else:
            table.overwrite(data)
        snapshot = table.current_snapshot()
        assert snapshot is not None
        return int(snapshot.snapshot_id)

    def read(self, name: str, snapshot_id: int | None = None) -> pa.Table:
        return self.table(name).scan(snapshot_id=snapshot_id).to_arrow()

    def record_result(
        self, table: str, result_id: str, building_id: str, payload: dict[str, Any]
    ) -> None:
        self.write(
            table,
            pa.Table.from_pylist(
                [
                    {
                        "result_id": result_id,
                        "building_id": building_id,
                        "payload": json.dumps(payload, allow_nan=False),
                    }
                ]
            ),
            ["result_id"],
        )

    def snapshots(self) -> dict[str, int | None]:
        result: dict[str, int | None] = {}
        for identifier in self.catalog.list_tables(self.namespace):
            snapshot = self.catalog.load_table(identifier).current_snapshot()
            result[identifier[-1]] = int(snapshot.snapshot_id) if snapshot else None
        return result
