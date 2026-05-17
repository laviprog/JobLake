# JobLake

AI-powered lakehouse platform for IT job market analytics.

## What It Does

JobLake collects IT vacancies, stores raw and curated data in an Iceberg lakehouse,
serves analytics through Trino, and exposes an AI agent that answers questions using
the prepared `joblake.gold` tables.

Main services:

- `collector` parses vacancy data and publishes it to Kafka.
- `spark-master` / `spark-worker` run Bronze, Silver, and Gold transformations.
- `minio` stores Iceberg data.
- `iceberg-rest` exposes the Iceberg catalog.
- `trino` queries curated tables.
- `agent` provides the FastAPI AI assistant backed by Ollama and Trino.
- `app` provides a lightweight UI.
- `airflow` orchestrates the daily pipeline.

## Requirements

- Docker and Docker Compose.
- Ollama running on the host, with the model configured by `OLLAMA_MODEL_NAME`.

Example:

```bash
ollama pull gemma4:31b-cloud
```

## Quick Start

```bash
cp .env.example .env
docker compose up --build
```

Useful local URLs:

- App: http://localhost:8501
- Agent API docs: http://localhost:8088/api/v1/docs
- Agent healthcheck: http://localhost:8088/api/v1/healthcheck
- Airflow: http://localhost:9900
- MinIO console: http://localhost:9001
- Trino: http://localhost:8082

Default Airflow credentials are configured in `.env.example`.

## Configuration

Project-level configuration lives in `.env`. Start from `.env.example` and adjust
values for your machine.

Important variables:

- `ENV`, `LOG_LEVEL` control service runtime mode and structured logging verbosity.
- `KAFKA_*` configure raw vacancy ingestion topics.
- `MINIO_*`, `AWS_*`, `JOBLAKE_WAREHOUSE`, `ICEBERG_REST_URI` configure storage.
- `TRINO_*` configure analytics access for the agent and app.
- `OLLAMA_BASE_URL`, `OLLAMA_MODEL_NAME` configure the LLM used by the agent.
- `AGENT_ROOT_PATH` controls the FastAPI root path exposed by Docker Compose.

## Agent API

Send a chat request to:

```text
POST http://localhost:8088/api/v1/agent/chat
```

Example body:

```json
{
  "message": "Which skills are most in demand?",
  "history": []
}
```

The agent answers using Trino-backed tools over curated Gold tables. It does not
execute user-provided SQL directly.

## Common Commands

```bash
make compose-config
make up
make logs
make down
```
