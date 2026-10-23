"""Build an offline architecture reference from the repository's actual source files.

Run from any working directory: python scripts/build_architecture.py
No packages or running application services are required.
"""

import ast
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]

DESCRIPTIONS = {
    "__init__.py": ("Package", "Package marker and simulation-only scope."),
    "domain.py": (
        "Domain",
        "Immutable Building and Telemetry contracts, canonical units, physical range "
        "validation and the three demo archetypes. ",
    ),
    "simulator.py": (
        "Simulation",
        "Seeded single-zone thermal dynamics, occupancy and weather generation, "
        "baseline control and optional heating degradation. ",
    ),
    "cli.py": (
        "Simulation",
        "Reusable telemetry iterator and the original finite/continuous JSONL command. "
        "This path does not use Kafka. ",
    ),
    "contracts.py": (
        "Validation",
        "Pydantic boundary dataclasses and adapters. Enforces identifiers, forbids "
        "extra fields and invokes domain validation. ",
    ),
    "settings.py": (
        "Configuration",
        "Environment-driven database, Kafka, transport and allowed-lateness settings, "
        "shared by independent processes. ",
    ),
    "logging_config.py": (
        "Operations",
        "JSON operational logs with UTC times and structured details, without raw "
        "telemetry bodies. ",
    ),
    "storage.py": (
        "Persistence",
        "SQLite WAL repository: building configuration, raw receipts, quarantine, "
        "deduplicated events, event-time history and watermarks. ",
    ),
    "ingestion.py": (
        "Ingestion",
        "Validates raw bytes through the telemetry adapter and persists accepted or "
        "quarantined records with their transport position. ",
    ),
    "broker.py": (
        "Streaming",
        "Publisher interface, Kafka and local adapters, broker readiness, consumer "
        "creation and explicit per-record offset commits. ",
    ),
    "worker.py": (
        "Ingestion",
        "Standalone Kafka consumer process. Stores each record before committing its "
        "offset, and stops on storage or commit failure. ",
    ),
    "producer.py": (
        "Streaming",
        "Publishes the existing three-building simulator to Kafka, with finite or paced "
        "continuous runs and graceful shutdown. ",
    ),
    "api.py": (
        "Serving",
        "FastAPI application factory, lifespan, registration/configuration, telemetry "
        "publication, latest/history, ingestion status and health endpoints. ",
    ),
    "test_phase1.py": (
        "Tests",
        "Regression tests for thermal physics, reproducibility, isolation, validation, "
        "missing readings and the JSONL CLI. ",
    ),
    "test_phase2.py": (
        "Tests",
        "API contracts, SQLite recovery, concurrent deduplication, quarantine, late "
        "arrivals, rollback and acknowledgment failure tests. ",
    ),
    "test_kafka_integration.py": (
        "Tests",
        "Opt-in real-broker test for HTTP publication, ingestion, consumer "
        "restart/replay, committed offsets and API queries. ",
    ),
    "build_architecture.py": (
        "Documentation",
        "Rebuilds this HTML using actual source, AST-derived imports, symbols and API "
        "routes. Standard library only. ",
    ),
    "pyproject.toml": (
        "Tooling",
        "Python package, uv dependencies, executable entry points, Ruff, strict mypy "
        "and pytest configuration. ",
    ),
    "uv.lock": (
        "Tooling",
        "Locked Python dependency versions and package hashes. Reproducibility data, "
        "not application code. ",
    ),
    "Dockerfile": (
        "Deployment",
        "Builds the shared non-root API, worker and simulator image using the locked uv "
        "environment. ",
    ),
    "compose.yaml": (
        "Deployment",
        "Local KRaft Kafka, topic initialization, API, ingestion worker and simulator, "
        "with persistent named volumes. ",
    ),
    ".dockerignore": (
        "Deployment",
        "Keeps local environments, secrets, generated data and caches out of the image "
        "build context. ",
    ),
    ".gitignore": (
        "Tooling",
        "Excludes runtime data, secrets, build artifacts and development caches from "
        "version control. ",
    ),
    ".python-version": ("Tooling", "Pins the developer Python minor version used by uv."),
    ".env.example": (
        "Configuration",
        "Documents native environment variables and container-specific address behavior "
        "without credentials. ",
    ),
    "README.md": (
        "Documentation",
        "Project overview, Mermaid architecture, repository layout, local commands and "
        "verification instructions. ",
    ),
    "architecture.md": (
        "Documentation",
        "Full target architecture, technology responsibilities, ten-phase plan and "
        "production evolution. ",
    ),
    "thermal-model.md": (
        "Documentation",
        "Thermal equations, units, example building parameters and simulator limitations. ",
    ),
    "phase2.md": (
        "Documentation",
        "Phase 2 decisions, HTTP contracts, delivery/recovery semantics, commands and "
        "verification record. ",
    ),
    "architecture-template.html": (
        "Documentation",
        "Self-contained layout, accessible interactions and SVG visualizations used to "
        "generate architecture.html. ",
    ),
}


def discover() -> list[Path]:
    paths = [
        p
        for directory in (
            "src",
            "tests",
            "scripts",
            "docs",
            "jobs",
            "dags",
            "deploy",
            "monitoring",
            ".github",
        )
        for p in (ROOT / directory).rglob("*")
        if p.is_file()
        and (
            p.suffix in {".py", ".md", ".html", ".yaml", ".yml", ".json", ".nix", ".txt"}
            or p.name.startswith("Dockerfile")
        )
        and "__pycache__" not in p.parts
    ]
    paths += [
        p
        for p in ROOT.iterdir()
        if p.is_file()
        and (
            p.name
            in {
                "Dockerfile",
                "compose.yaml",
                "pyproject.toml",
                "uv.lock",
                ".env.example",
                ".gitignore",
                ".dockerignore",
                ".python-version",
            }
            or p.suffix in {".md", ".yaml", ".nix"}
            or p.name == "flake.lock"
        )
    ]
    return sorted(paths, key=lambda p: p.relative_to(ROOT).as_posix())


def main() -> None:
    files: list[dict[str, Any]] = []
    paths = discover()
    modules = {
        "buildingpilot." + p.stem: p.relative_to(ROOT).as_posix()
        for p in paths
        if p.suffix == ".py" and p.parent.name == "buildingpilot"
    }
    modules["buildingpilot"] = modules["buildingpilot.__init__"]
    for path in paths:
        relative = path.relative_to(ROOT).as_posix()
        source = path.read_text(encoding="utf-8-sig")
        category, description = DESCRIPTIONS.get(
            path.name, ("Supporting file", "Repository supporting file.")
        )
        imports: set[str] = set()
        symbols: list[dict[str, Any]] = []
        routes: list[dict[str, Any]] = []
        if path.suffix == ".py":
            tree = ast.parse(source)
            if path.name not in DESCRIPTIONS and ast.get_docstring(tree):
                description = ast.get_docstring(tree).splitlines()[0]
                category = "Application" if relative.startswith("src/") else "Operations"
            for node in ast.walk(tree):
                if isinstance(node, ast.ImportFrom) and node.module in modules:
                    imports.add(modules[node.module])
                if isinstance(node, ast.ImportFrom) and node.module:
                    imports.update(
                        modules[f"{node.module}.{alias.name}"]
                        for alias in node.names
                        if f"{node.module}.{alias.name}" in modules
                    )
                if isinstance(node, ast.Import):
                    imports.update(
                        modules[alias.name] for alias in node.names if alias.name in modules
                    )
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                    symbols.append(
                        {
                            "name": node.name,
                            "line": node.lineno,
                            "kind": "class" if isinstance(node, ast.ClassDef) else "function",
                        }
                    )
                    for decorator in node.decorator_list:
                        if (
                            isinstance(decorator, ast.Call)
                            and isinstance(decorator.func, ast.Attribute)
                            and isinstance(decorator.func.value, ast.Name)
                            and decorator.func.value.id == "app"
                            and decorator.func.attr in {"get", "post", "put", "delete", "patch"}
                            and decorator.args
                            and isinstance(decorator.args[0], ast.Constant)
                        ):
                            routes.append(
                                {
                                    "method": decorator.func.attr.upper(),
                                    "path": decorator.args[0].value,
                                    "line": node.lineno,
                                }
                            )
        files.append(
            {
                "path": relative,
                "name": path.name,
                "category": category,
                "description": description.strip(),
                "source": source,
                "lines": len(source.splitlines()),
                "imports": sorted(imports),
                "symbols": sorted(symbols, key=lambda symbol: symbol["line"]),
                "routes": routes,
            }
        )
    data = {"files": files, "generated": datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")}
    payload = (
        json.dumps(data, ensure_ascii=True)
        .replace("<", "\\u003c")
        .replace(">", "\\u003e")
        .replace("&", "\\u0026")
    )
    template = (ROOT / "docs/architecture-template.html").read_text(encoding="utf-8")
    assert template.count("__ARCHITECTURE_DATA__") == 1
    output = ROOT / "architecture.html"
    output.write_text(template.replace("__ARCHITECTURE_DATA__", payload), encoding="utf-8")
    print(
        f"Built {output.name}: {len(files)} files, "
        f"{sum(path.suffix == '.py' for path in paths)} Python files, "
        f"{sum(len(file['routes']) for file in files)} HTTP operations"
    )


if __name__ == "__main__":
    main()
