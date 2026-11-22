# DeliveryOps Intelligence — Architecture Baseline

## 1. Design Principles

1. **Hybrid Local-First Execution**: Keep heavyweight runtime code out of Docker. Run only stateful infrastructure services in Docker with explicit `mem_limit` and `cpus` constraints, while executing Python producers, consumers, DuckDB/Polars transforms, dbt Core, FastAPI, and Streamlit natively inside `.venv` (Python 3.11.9).
2. **WSL2 Memory Safety**: `.wslconfig` remains unchanged at `memory=3GB` (`2.84 GiB` usable by Docker). All 4 infrastructure services combined are capped at `1,408 MB` (`postgres` 256 MB, `redpanda` 640 MB, `minio` 256 MB, `jaeger` 256 MB), leaving comfortable headroom against `OOMKilled` (`Exit 137`).
3. **Profile-Driven Infrastructure**: Start only the services needed for the active workflow (`core`, `streaming`, `storage`, `observability`, or `all`).
4. **Windows/WSL2 I/O Isolation**: Stateful container data (`pgdata`, `redpandadata`, `miniodata`) lives on Docker named volumes inside the WSL2 Linux filesystem rather than Windows `E:\` bind mounts.

## 2. Approved Technology Matrix

| Layer | Technology | Runtime Location | Rationale |
| :--- | :--- | :--- | :--- |
| Language & Env | Python 3.11.9 (`py -3.11` + `.venv`) | Host Windows | Full pre-built `win_amd64` wheel compatibility |
| Event Streaming | Redpanda (`v24.2.8`) | Docker (`streaming`) | Kafka-compatible C++ engine without JVM/ZooKeeper overhead |
| Object Storage | MinIO + Local Lake Fallback | Docker (`storage`) / Host | S3-compatible API (`s3://`) with zero-Docker fallback mode |
| Operational DB | PostgreSQL 16 Alpine | Docker (`core`) | Reliable concurrent OLTP/Gold warehouse |
| Analytics | DuckDB + Polars + PyArrow | Host `.venv` | In-process vectorized OLAP without JVM cluster overhead |
| Modeling | dbt Core | Host `.venv` | Standard SQL transformations, lineage, and tests |
| Data Contracts | Pydantic v2 + Pandera | Host `.venv` | Stream schema validation (DLQ routing) & tabular checks |
| Orchestration | Custom Runner -> Dagster | Host `.venv` | Zero early daemon overhead; asset-centric orchestration later |
| Observability | OpenTelemetry + `structlog` + Jaeger | Host + Docker (`observability`) | Distributed `OTLP` tracing and trace-correlated JSON logs |
| Serving & UI | FastAPI + Streamlit | Host `.venv` | Lightweight async API and analytics dashboard |
| Testing & Lint | Pytest + Ruff | Host `.venv` | Fast static checks and automated unit/integration testing |

## 3. Docker Compose Service Profiles

| Profile | Services Included | Max Memory Cap |
| :--- | :--- | :--- |
| `core` | `postgres` | `256 MB` |
| `streaming` | `redpanda` | `640 MB` |
| `storage` | `minio` | `256 MB` |
| `observability` | `jaeger` | `256 MB` |
| `all` | `postgres`, `redpanda`, `minio`, `jaeger` | `1,408 MB` |
