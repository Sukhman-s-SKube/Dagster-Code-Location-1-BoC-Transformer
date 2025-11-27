# Dagster Code Location – Bank of Canada Transformer

This repository hosts a Dagster code location that ingests macroeconomic indicators from the Bank of Canada (BoC) Valet API and the Federal Reserve Economic Data (FRED) service, assembles them into a single feature row per day, writes that record to ClickHouse, and exposes a set of Celery-backed ops for training and running forecasting models.

- **Tech stack:** Dagster assets/jobs (Python 3.11), pandas, FredAPI, ClickHouse, Celery, S3-compatible storage, Postgres run storage.
- **Outputs:** One `macro_daily` row per partition with policy rate, CPI, yield curve points, oil prices, unemployment, and derived spread (`y2 - y10`) stored in ClickHouse by an IO manager (`boc_transformer/resources.py`).
- **Deployment targets:** Local `dagster dev` (`definitions.py`), Dagster gRPC server (`boc_transformer.repository`), containerized via the provided `Dockerfile`, and Kubernetes manifests in `k8s/`.

---

## Architecture Overview

### Assets (`boc_transformer/assets.py`)

| Asset name | Source | Description |
|------------|--------|-------------|
| `daily_policy_rate` | BoC Valet `B114039` | Target for the overnight rate; exposes daily partitions with lookback metadata. |
| `daily_cpi` | BoC Valet `V41690973` | CPI (all-items, 2015=100). |
| `daily_yield_2y`, `daily_yield_5y`, `daily_yield_10y` | BoC Valet `BD.CDN.*.DQ.YLD` | 2, 5, and 10-year government yields. |
| `daily_oil` | FRED `DCOILWTICO` | West Texas Intermediate spot price. |
| `daily_unemployment` | FRED `LRUNTTTTCAQ156S` | Canadian unemployment. |
| `assemble_macro_daily_row` | Merges all above | Builds `rate`, `cpi`, `y2`, `y5`, `y10`, `spread_2_10`, `oil`, `unemploy` and writes to ClickHouse with metadata on missing fields. |

All assets share a `DailyPartitionsDefinition` starting `2015-01-01`. Metadata captures fetch URLs, as-of dates, and staleness to simplify observability inside Dagster.

### Ops & Jobs (`boc_transformer/ops.py`, `boc_transformer/repository.py`)

- `hello_world` demo job for smoke tests.
- `xg_boost_train`, `xg_boost_predict_today`, `xg_boost_evaluate_recent`, `enqueue_xgb_prediction`, `enqueue_xgb_evaluation` fire asynchronous Celery tasks (`trainer.*`) through the `boc_forecaster_celery` resource. Configurable fields allow sequence length, horizons, bucket/prefix, optional caps, and diagnostics.
- Jobs in `boc_transformer/repository.py` wrap each op for direct triggering from Dagster UI/CLI.

### Resources & IO Managers (`boc_transformer/resources.py`)

- `boc_api`: `requests.Session` preloaded with the BoC Valet base URL.
- `fred_api`: simple resource returning the FRED API key, consumed by FredAPI.
- `clickhouse_macro_io_manager`: writes and reads partitions to ClickHouse (`features.macro_daily` by default) and keeps the MergeTree table schema in sync.
- `io_manager` and `s3`: `dagster_aws` resources configured through env vars for persisting intermediate objects if needed.
- `boc_forecaster_celery`: Celery app configured via `REDIS_BROKER_URL` / `CELERY_BACKEND_URL`.

### Scheduling (`boc_transformer/schedules.py`)

`define_asset_job` is used to produce a job per raw asset plus one composed job for `assemble_macro_daily_row`. `build_schedule_from_partitioned_job` wires those jobs to partition-aware schedules so each day’s partition is materialized and assembled in the same run. `Definitions` in both `definitions.py` and `boc_transformer/repository.py` register these schedules.

---

## Prerequisites

- Python 3.11 and `pip`
- Access to:
  - BoC Valet API (no auth, but network access required)
  - FRED API key
  - ClickHouse instance
  - Redis (or another Celery-compatible broker/backend)
  - Postgres database for Dagster run storage
  - S3 or S3-compatible object store for compute logs and optional IO manager storage
- Optional: Docker, Kubernetes, and `kubectl` to deploy the gRPC server.

---

## Environment Variables

| Variable | Purpose | Required |
|----------|---------|----------|
| `FRED_API_KEY` | Passed to `fred_api` for FredAPI access. | Yes (if running FRED-backed assets). |
| `CH_HOST`, `CH_PORT`, `CH_USER`, `CH_PASS`, `CH_DB`, `CH_TABLE` | ClickHouse location and target table for the IO manager (defaults: port `30090`, db `features`, table `macro_daily`). | Yes for assembling/writing. |
| `REDIS_BROKER_URL`, `CELERY_BACKEND_URL` | Used by `boc_forecaster_celery` to enqueue/train/predict tasks. | Required when running Celery ops/jobs. |
| `S3_BUCKET`, `AWS_ENDPOINT_URL`, `AWS_REGION` | Configure the default IO manager and compute log manager S3 endpoints. `AWS_ENDPOINT_URL` can point to MinIO, `use_ssl` is disabled in config. | Required if using the S3 IO manager or compute logs. |
| `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB` | Referenced in `dagster.yaml` to back Dagster run storage. | Required when running the deployed/gRPC service. |
| `DAGSTER_HOME` | Defaults to `/opt/dagster` in the Docker image; should point to a writable directory containing `dagster.yaml` when running locally. | Recommended. |

Export these variables directly or via a `.env` that you `source` before running Dagster.

---

## Local Development

1. **Clone and install dependencies**
   ```bash
   git clone https://github.com/skube/Dagster-Code-Location-1-BoC-Transformer.git
   cd Dagster-Code-Location-1-BoC-Transformer
   python3.11 -m venv .venv && source .venv/bin/activate
   pip install --upgrade pip
   pip install -r requirements.txt
   ```

2. **Configure environment**
   - Create a `.env` (or export directly) containing the variables described above.
   - Ensure ClickHouse, Redis/Celery, Postgres, and your object store are reachable from your machine or container.

3. **Run Dagster**
   - For the modern `Definitions` API:
     ```bash
     dagster dev -m definitions
     ```
   - To emulate the production gRPC server:
     ```bash
     dagster api grpc -m boc_transformer.repository -h 0.0.0.0 -p 3030
     ```

4. **Materialize assets**
   - Single partition:
     ```bash
     dagster asset materialize daily_policy_rate --partition 2024-05-01
     ```
   - Assemble a feature row (ensures all parents run first):
     ```bash
     dagster job launch -j materialize_assemble_macro_daily_row --partition 2024-05-01
     ```
   - Backfill several days:
     ```bash
     dagster job backfill -j materialize_daily_oil --start 2024-01-01 --end 2024-01-31
     ```

5. **Trigger Celery-driven ops**
   ```bash
   dagster job launch -j xgb_training_job -c '{"ops": {"xg_boost_train": {"config": {"seq_len": 90, "horizon": 30, "model_bucket": "models", "model_prefix": "boc_policy_classifier"}}}}'
   ```
   Adjust config via the Dagster UI or CLI to point to your model artifacts. The ops simply enqueue Celery tasks; monitor them from your Celery worker logs.

---

## External Services & Data Contracts

- **BoC Valet**: Assets fetch up to 1,080 days of historical observations to determine the latest available figure. HTTP errors raise Dagster run failures, and metadata records the query URL for auditing.
- **FRED**: FredAPI is used for oil and unemployment series. Ensure your API key has not exceeded rate limits.
- **ClickHouse**: The IO manager creates `features.macro_daily` on demand with `MergeTree` and enforces one row per day by deleting pre-existing rows with the same `date`. Schema:
  ```sql
  CREATE TABLE features.macro_daily (
    date Date,
    rate Float64,
    cpi Float64,
    y2 Float64,
    y5 Float64,
    y10 Float64,
    spread_2_10 Float64,
    oil Float64,
    unemploy Float64
  ) ENGINE = MergeTree
  PARTITION BY toYYYYMM(date)
  ORDER BY date;
  ```
- **Celery**: The repo does not ship worker code. The ops assume the worker exposes `trainer.train_xgb_from_io`, `trainer.predict_xgb_from_io`, and `trainer.eval_xgb_from_io`. Configure `REDIS_BROKER_URL`/`CELERY_BACKEND_URL` to point at your broker/backend pair.
- **S3-compatible storage**: Used by both the `io_manager` (if you leverage it) and the compute log manager defined in `dagster.yaml`. When pointing to MinIO, keep `AWS_ENDPOINT_URL` reachable within the cluster/pod.

---

## Deployment

### Docker

The `Dockerfile` builds a minimal image:
1. Installs build tooling (`gcc`, `build-essential`) for numpy/pandas.
2. Installs Python dependencies from `requirements.txt`.
3. Copies the `boc_transformer` package.
4. Creates a non-root `dagster` user and exposes `dagster api grpc -m boc_transformer.repository -p 3030`.

Build and run:
```bash
docker build -t boc-transformer:latest .
docker run --rm -p 3030:3030 \
  -e DAGSTER_HOME=/opt/dagster \
  -e FRED_API_KEY=... \
  -e CH_HOST=... \
  ... \
  boc-transformer:latest
```
Mount a volume with `dagster.yaml` if you need to override the default.

### Kubernetes

The `k8s/` manifests describe a simple deployment and service in the `dagster` namespace:
- `k8s/1-deployment.yaml` – update `image: IMAGE_PLACEHOLDER` with your pushed image, and ensure `envFrom` secrets/config maps deliver all required variables (Postgres, ClickHouse, FRED, S3, Celery, etc.).
- `k8s/2-service.yaml` – exposes the gRPC server on port `3030` for ingestion by the Dagster control plane (Dagster Daemon or Dagster Cloud).

Apply in order:
```bash
kubectl apply -f k8s/1-deployment.yaml
kubectl apply -f k8s/2-service.yaml
```

### Dagster Configuration (`dagster.yaml`)

Located at the repository root, it configures:
- Postgres run/event storage.
- S3 compute log manager (bucket `dagster`, prefix `compute-logs`, `use_ssl: false`).
- Telemetry disabled.

Ensure this file is accessible at `$DAGSTER_HOME/dagster.yaml` in both local and deployed environments.

---

## Repository Layout

```
.
├── boc_transformer/
│   ├── assets.py            # Asset definitions (raw + assembled feature row)
│   ├── ops.py               # Celery orchestration ops
│   ├── repository.py        # Dagster definitions for gRPC entrypoint
│   ├── resources.py         # API clients and IO managers
│   └── schedules.py         # Asset jobs + schedules
├── definitions.py           # Lightweight Definitions object for dagster dev
├── dagster.yaml             # Run storage / compute log config
├── requirements.txt
├── Dockerfile
└── k8s/
    ├── 1-deployment.yaml
    └── 2-service.yaml
```

---

## Troubleshooting & Tips

- **Missing data:** Assets log `status=no_data_in_lookback` metadata when the upstream API has no observations inside the lookback window. Use Dagster event details to inspect the `query_url` and `staleness_days`.
- **Partition consistency:** When ClickHouse already contains a row for the partition, the IO manager deletes it before inserting the new values. Ensure no concurrent runs materialize the same partition unless you expect the newest run to win.
- **Networking:** The BoC Valet API calls time out after 20 seconds. If you deploy in a locked-down environment, allow outbound HTTPS to `bankofcanada.ca` and `api.stlouisfed.org`.
- **Celery ops:** Fail fast if the broker/backend variables are missing; Dagster will surface import or connection errors immediately. Use the returned `task_id` to trace work in your Celery monitoring tool.
- **Local ClickHouse:** For quick testing you can run `docker run -p 9000:9000 -p 8123:8123 clickhouse/clickhouse-server` and point `CH_HOST=host.docker.internal` (on macOS) with `CH_PORT=9000`.

---

## Next Steps

- Configure Dagster sensors or alerts around staleness metadata to monitor upstream data latency.
- Enrich the assembled feature row with additional macro signals by adding new assets and wiring them into `assemble_macro_daily_row`.
- Extend the Celery ops to emit run status events back into Dagster for end-to-end visibility.

