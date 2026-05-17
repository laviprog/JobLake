# JobLake AI Agent Service

FastAPI service that answers natural-language questions about JobLake analytics by using
Ollama tool calls over Trino Gold tables.

## Table of Contents

- [Responsibilities](#responsibilities)
- [Runtime Flow](#runtime-flow)
- [API](#api-usage)
- [Agent Tools](#agent-tools)
- [Development Checks](#development-checks)

Back to the [project README](../../README.md).

## Responsibilities

The service provides:

- a chat endpoint for the dashboard app;
- LLM orchestration through Ollama;
- tool-calling functions for market overview, skills, specializations, locations, companies,
  vacancy search, and salary distribution;
- Trino access to curated Gold analytical tables;
- structured logs and health checks.

The service does not ingest data, run Spark jobs, or mutate lakehouse tables.

## Runtime Flow

1. The dashboard sends a chat request to `POST /api/v1/agent/chat`.
2. `JobLakeAgent` builds a system prompt plus recent chat history.
3. Ollama receives the prompt and the available tool schemas.
4. If the model calls a tool, the service queries Trino and returns JSON results to the model.
5. The model produces a final answer for the user.

Tool execution is capped by `max_tool_rounds` to avoid unbounded loops.

## API Usage

### Open API Documentation

After the stack is running, open:

- Scalar docs: `http://localhost:8088/api/v1/docs`
- Swagger docs: `http://localhost:8088/api/v1/docs/swagger`
- OpenAPI schema: `http://localhost:8088/api/v1/openapi.json`

### Health Check

```bash
curl http://localhost:8088/api/v1/healthcheck
```

### Chat Request

```bash
curl -X POST http://localhost:8088/api/v1/agent/chat \
  -H 'Content-Type: application/json' \
  -d '{"message":"What are the top Python-related skills in the last 30 days?","history":[]}'
```

## Agent Tools

The LLM can call these internal tools:

| Tool | Purpose |
| --- | --- |
| `get_market_overview` | Daily vacancy, company, remote, and salary metrics. |
| `get_top_skills` | Most demanded skills. |
| `get_top_specializations` | Most demanded specializations. |
| `get_top_locations` | Vacancy demand by location. |
| `get_top_companies` | Companies with the most vacancies. |
| `search_vacancies` | Text search over enriched vacancy records. |
| `get_salary_distribution` | Salary metrics by bucket and optional filters. |

Tools query `joblake.gold.*` tables through Trino.

## Development Checks

Run formatting from `src/agent`:

```bash
uv run ruff format
```

Run the linter from `src/agent`:

```bash
uv run ruff check .
```
