FROM ghcr.io/astral-sh/uv:0.9.26 AS uv
FROM python:3.12-slim
COPY --from=uv /uv /usr/local/bin/uv
WORKDIR /app
ENV UV_LINK_MODE=copy PYTHONUNBUFFERED=1 UV_NO_CACHE=1
COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev --no-install-project
COPY README.md ./
COPY src ./src
RUN uv sync --locked --no-dev --no-editable \
    && useradd --uid 10001 --create-home app \
    && mkdir -p /app/data && chown app:app /app/data
ENV PATH="/app/.venv/bin:$PATH"
USER app
CMD ["uvicorn", "buildingpilot.api:app", "--host", "0.0.0.0", "--port", "8000"]
