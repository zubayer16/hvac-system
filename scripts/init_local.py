"""Create local-only service secrets without overwriting existing environment values."""

import secrets
from pathlib import Path

root = Path(__file__).resolve().parents[1]
path = root / ".env"
content = path.read_text(encoding="utf-8") if path.exists() else ""
values = {line.split("=", 1)[0] for line in content.splitlines() if "=" in line}
defaults = {
    "BP_S3_ACCESS_KEY": secrets.token_hex(12),
    "BP_S3_SECRET_KEY": secrets.token_urlsafe(32),
    "BP_AIRFLOW_PASSWORD": secrets.token_urlsafe(24),
    "BP_GRAFANA_PASSWORD": secrets.token_urlsafe(24),
}
with path.open("a", encoding="utf-8") as stream:
    stream.write("\n")
    for key, value in defaults.items():
        if key not in values:
            stream.write(f"{key}={value}\n")
print("Local secrets initialized in ignored .env; existing values preserved.")
