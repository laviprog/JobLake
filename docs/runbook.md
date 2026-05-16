# JobLake Local Runbook

This runbook describes the basic local infrastructure startup flow.

## Prerequisites

- Docker Desktop is running.
- You are in the repository root.
- Ports `5432`, `7077`, `8000`, `8081`, `8082`, `8088`, `8181`, `8501`, `9000`, `9001`, `9090`, `9092`, and `9900` are available on `127.0.0.1`.

## First Run

Create local environment variables:

```bash
cp .env.example .env
```

Validate compose configuration:

```bash
make compose-config
```

Build and start the platform:

```bash
docker compose up --build
```

## Service URLs

| Service | URL |
| --- | --- |
| Airflow | http://127.0.0.1:9900 |
| Kafbat UI | http://127.0.0.1:9090 |
| MinIO Console | http://127.0.0.1:9001 |
| Spark Master UI | http://127.0.0.1:8000 |
| Spark Worker UI | http://127.0.0.1:8081 |
| Trino | http://127.0.0.1:8082 |
| Iceberg REST | http://127.0.0.1:8181 |
| Agent API | http://127.0.0.1:8088/api/v1/healthcheck |
| App placeholder | http://127.0.0.1:8501/health |

Default local credentials:

| Service | Username | Password |
| --- | --- | --- |
| Airflow | `admin` | `admin` |
| MinIO | `admin` | `adminadmin` |

## Health Checks

Check running containers:

```bash
docker compose ps
```

Check the app placeholder:

```bash
curl http://127.0.0.1:8501/health
```

Expected response contains:

```text
JobLake App
status=ok
```

Check the agent API:

```bash
curl http://127.0.0.1:8088/api/v1/healthcheck
```

Run the collector preflight job:

```bash
docker compose --profile jobs run --rm collector
```

Expected output contains:

```text
JobLake collector preflight
status=ok
```

## Successful Startup Criteria

The infrastructure baseline is considered healthy when:

- `docker compose ps` shows long-running services as running or healthy.
- Airflow UI opens and lists DAGs.
- Kafbat UI opens and can connect to the `local` Kafka cluster.
- MinIO Console opens and the `joblake` bucket exists.
- Spark Master UI opens and shows the worker.
- Trino UI opens.
- Agent healthcheck returns a successful response.
- App placeholder returns `status=ok`.
- Collector preflight exits successfully.

## Useful Commands

```bash
make ps
make logs
make collector
make down
```

## Troubleshooting

If Kafka fails to start after repeated local runs, remove its local data directory after stopping the stack:

```bash
docker compose down
rm -rf data/kafka
```

If Airflow cannot write logs, check ownership of the local logs directory:

```bash
mkdir -p logs/airflow
```

If ports are already occupied, stop the conflicting process or adjust the published port in `docker-compose.yml`.

If Spark jobs later fail to access MinIO, verify that all Spark containers use the same S3-related environment variables and that Iceberg is configured with path-style access.
