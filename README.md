# DeliveryOps Intelligence

A local-first, production-inspired data engineering platform built for constrained laptop hardware using a hybrid execution model.

## Architecture Overview

DeliveryOps Intelligence implements a full **5-Asset Medallion Architecture** with streaming ingestion, contract validation, Dead-Letter Queue (DLQ) routing, SQL dbt transformations, DuckDB analytics, OpenTelemetry distributed tracing, FastAPI serving, and a Streamlit control plane.

- **Streaming**: Redpanda-compatible broker (`InMemoryEventBroker` + `StreamIngestionConsumer`) with topic offsets & consumer lag
- **Data Contracts**: Pydantic v2 ingestion gate (`DeliveryEvent`, `DeadLetterRecord`) + Pandera tabular checks (`SILVER_EVENTS_SCHEMA`)
- **Storage & Lakehouse**: Hive-partitioned local Medallion lake (`bronze/`, `silver/`, `gold/`) + S3/MinIO URI mapper (`S3LakeAdapter`) + DuckDB/PostgreSQL warehouse (`GoldWarehouseLoader`)
- **Analytical & SQL Layer**: Polars + PyArrow + resource-capped DuckDB (`memory_limit=1GB`, `threads=2`) + dbt SQL models (`dbt/`)
- **Orchestration**: 5-asset declarative DAG (`AssetGraphOrchestrator`: `stream_raw_topic` -> `bronze_events_and_dlq` -> `silver_validated_events` -> `gold_dbt_marts` -> `warehouse_serving_tables`)
- **Observability**: OpenTelemetry trace spans (`traced_span`, JSON trace bundle export) + `structlog` trace-correlated JSON logging + TCP infra probes (`/infra/status`)
- **Serving & UI**: FastAPI REST API + Streamlit Control Plane Dashboard

## One-Click Launch & Shutdown (Windows)

### Start Platform (`start.bat`)
```bat
start.bat
```
This runs in **Hybrid Local-First Mode** (0 MB extra Docker RAM):
1. Creates `.env` from `.env.example` if missing.
2. Materializes the 5-asset Medallion DAG via `scripts\run_platform.py`.
3. Starts **FastAPI** on `http://127.0.0.1:8001` (`logs\fastapi.log`).
4. Starts **Streamlit** on `http://127.0.0.1:8501` (`logs\streamlit.log`).

To also start the Docker Compose `core` profile (`postgres:16-alpine`):
```bat
start.bat --with-docker
```

### Stop Platform (`stop.bat`)
```bat
stop.bat
```
Stops the background FastAPI (`8001`) and Streamlit (`8501`) processes and pauses any active DeliveryOps Docker Compose services.

## Key API Endpoints (FastAPI)

- `GET /health` — Service health & version (`0.6.0`)
- `GET /infra/status` — Live TCP reachability & fallback report (`postgres`, `redpanda`, `minio`, `jaeger_otlp`)
- `GET /observability/traces` — Recorded OpenTelemetry spans
- `GET /orchestration/assets` — 5-asset DAG lineage graph
- `POST /orchestration/materialize` — Execute the full 5-asset pipeline DAG
- `GET /warehouse/summary` — Relational serving warehouse table counts & top zones
- `GET /analytics/zones` / `GET /analytics/couriers` / `GET /analytics/dlq` — Gold marts & DLQ records

## Repository Layout

```text
deliveryops-intelligence/
├── start.bat             # One-click platform launcher (CLI + FastAPI + Streamlit)
├── stop.bat              # Clean shutdown script for local processes & Compose services
├── src/deliveryops/      # Core platform Python package
├── dbt/                  # dbt Core SQL models (staging view + Gold marts) & schema tests
├── infra/                # Postgres DDL, Redpanda topic config, MinIO bucket manifest
├── scripts/              # CLI platform entrypoint (run_platform.py)
├── tests/                # Unit and integration test suite
├── docker-compose.yml    # Resource-capped infrastructure profiles
└── pyproject.toml        # Dependencies, Ruff & Pytest configuration
```

## Development & Verification

```powershell
.\.venv\Scripts\python.exe -m ruff check .
.\.venv\Scripts\python.exe -m pytest -v
```
