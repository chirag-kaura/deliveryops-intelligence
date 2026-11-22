# DeliveryOps Intelligence

A local-first, production-inspired data engineering platform built for constrained laptop hardware using a hybrid execution model.

## Architecture Overview

DeliveryOps Intelligence separates **stateful infrastructure backing services** (run in Docker with strict memory caps and Compose profiles) from **Python data workloads** (run natively on the host in a Python 3.11.9 `.venv`).

- **Streaming**: Redpanda (Kafka-compatible, single-node dev container)
- **Object Storage**: MinIO (S3-compatible) with local filesystem fallback
- **Operational & Serving Warehouse**: PostgreSQL 16 Alpine
- **Analytical Processing**: DuckDB + Polars + PyArrow
- **SQL Modeling**: dbt Core
- **Data Contracts & Quality**: Pydantic v2 + Pandera
- **Orchestration**: Lightweight custom Python runner initially (Dagster in the orchestration phase)
- **Observability**: OpenTelemetry + `structlog` + Jaeger
- **Serving & UI**: FastAPI + Streamlit
- **Testing & Linting**: Pytest + Ruff

## Hardware & Resource Baseline

- **Host CPU**: 4 physical cores / 8 logical threads (`Intel Core i5-1135G7`)
- **Docker / WSL2 Limit**: `3 GB` configured in `.wslconfig` (`2.84 GiB` usable in Docker)
- **Combined Core Container Memory Cap**: `1,408 MB` (`postgres` 256 MB, `redpanda` 640 MB, `minio` 256 MB, `jaeger` 256 MB)

## Repository Layout

```text
deliveryops-intelligence/
├── src/deliveryops/      # Core platform Python package
│   ├── config/           # Configuration loading and validation
│   ├── contracts/        # Pydantic v2 & Pandera data contracts
│   ├── ingestion/        # Event producers & stream consumers
│   ├── storage/          # S3/MinIO & local lake storage adapters
│   ├── processing/       # DuckDB & Polars Bronze/Silver/Gold processors
│   ├── pipelines/        # Pipeline runner & orchestration hooks
│   ├── observability/    # OpenTelemetry tracing & structlog setup
│   ├── api/              # FastAPI serving layer
│   └── dashboard/        # Streamlit intelligence UI
├── tests/                # Unit and integration tests
├── infra/                # Infrastructure initialization & config assets
├── dbt/                  # dbt Core SQL models & tests
├── scripts/              # Developer utility scripts
├── docs/                 # Architecture & operational documentation
├── docker-compose.yml    # Resource-capped infrastructure profiles
├── pyproject.toml        # Package metadata, Ruff & Pytest configuration
└── .env.example          # Environment configuration template
```

## Quickstart (Phase 1 Baseline)

1. Activate the Python 3.11.9 virtual environment:
   ```powershell
   .\.venv\Scripts\Activate.ps1
   ```
2. Verify code formatting and smoke tests:
   ```powershell
   ruff check .
   pytest -v
   ```
3. Validate Docker Compose profiles (requires Docker Engine running to start services):
   ```powershell
   docker compose --profile core config
   ```
