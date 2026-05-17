# JobLake Collector Service

`collector` is a batch ingestion service that collects vacancies from Habr Career and
publishes raw vacancy records to Kafka.

This README covers only the collector service. Project-wide startup instructions,
infrastructure services, and lakehouse processing are documented at the repository
level.

## Responsibilities

The service does the following:

- crawls Habr Career vacancy listing pages;
- opens each vacancy detail page;
- extracts vacancy, company, location, employment, salary, skills, and description
  fields from page metadata and SSR state;
- publishes successfully parsed vacancies to a raw Kafka topic;
- publishes parse or publish failures to a Kafka error topic;
- emits structured logs for pipeline progress and failures.

The service does not deduplicate vacancies, normalize analytical dimensions, enrich
skills, build marts, or write directly to object storage. Those concerns belong to
downstream processing.

## Runtime Flow

1. `src.main` creates and runs `CollectorPipeline`.
2. `CollectorPipeline` asks `HabrCareerParser` to collect vacancy links from
   `/vacancies?type=all&sort=date&page=N`.
3. Each link is parsed into a `Vacancy` model.
4. `VacancyPublisher` publishes the model to Kafka.
5. Failed parse or publish attempts are sent to the error topic when possible.

The collector is intended to run as a finite job, not as a long-running HTTP service.

## Published Messages

Successful vacancies are published to `KAFKA_TOPIC_RAW`.

Kafka message key:

```text
vacancy.id
```

Kafka headers:

```text
source=habr_career
schema_version=1
content_type=application/json
```

Message body shape:

```json
{
  "source": "habr_career",
  "id": "123456",
  "url": "https://career.habr.com/vacancies/123456",
  "title": "Data Engineer",
  "company": {
    "id": 42,
    "name": "Example Company",
    "url": "https://career.habr.com/companies/example",
    "site": "https://example.com"
  },
  "date_posted": "2026-05-16",
  "published_at": "2026-05-16T10:00:00+03:00",
  "published_title": "today",
  "valid_through": "2026-06-16",
  "employment_type_schema": "FULL_TIME",
  "employment": "full_time",
  "employment_type_text": "Full time",
  "remote": true,
  "job_location_type": "TELECOMMUTE",
  "locations": ["Moscow, Russia"],
  "human_city_names": "Moscow",
  "short_geo": "Moscow",
  "qualification": "middle",
  "salary_qualification": "net",
  "specializations": ["Backend"],
  "skills": ["Python", "Kafka"],
  "salary": {
    "salary_from": 200000,
    "salary_to": 300000,
    "currency": "RUR",
    "formatted": "200 000 - 300 000 RUR",
    "period": "MONTH"
  },
  "description_html": "<p>...</p>",
  "description_text": "...",
  "banner_description": null
}
```

Error messages are published to `KAFKA_TOPIC_ERROR` and include the failed stage,
source, URL or vacancy ID when available, and the error text.

## Configuration

Settings are loaded from environment variables or from a local `.env` file in the
collector working directory.

| Variable | Default | Description |
| --- | --- | --- |
| `ENV` | `prod` | Logging mode. Use `dev` for console logs and `prod` for JSON logs. |
| `LOG_LEVEL` | `INFO` | Python logging level. |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka bootstrap servers. Use `kafka:29092` inside Docker Compose. |
| `KAFKA_TOPIC_RAW` | `vacancies.raw` | Topic for successfully parsed vacancy records. |
| `KAFKA_TOPIC_ERROR` | `vacancies.errors` | Topic for parse and publish failures. |
| `USER_AGENT` | `JobLake collector/0.1 (+https://github.com/laviprog/joblake)` | HTTP user agent for requests to Habr Career. |
| `HABR_CAREER_BASE_URL` | `https://career.habr.com` | Habr Career base URL. |
| `HABR_CAREER_REQUEST_DELAY_SECONDS` | `0.5` | Delay between listing pages and vacancy requests. |
| `HABR_CAREER_MAX_PAGES` | unset | Optional crawl limit for local testing or small runs. |

Example `.env` for a local run:

```dotenv
ENV=dev
LOG_LEVEL=INFO
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC_RAW=vacancies.raw
KAFKA_TOPIC_ERROR=vacancies.errors
HABR_CAREER_MAX_PAGES=1
```

## Run With Docker Compose

From the repository root:

```bash
docker compose --profile jobs run --rm collector
```

For a small smoke run:

```bash
docker compose --profile jobs run --rm \
  -e HABR_CAREER_MAX_PAGES=1 \
  collector
```

The service exits after the crawl finishes.

## Run Locally

From `src/collector`:

```bash
uv sync
uv run python -m src.main
```

Local execution expects Kafka to be reachable through `KAFKA_BOOTSTRAP_SERVERS`.

## Development Checks

Run the linter from `src/collector`:

```bash
uv run ruff check .
```

Run formatting from `src/collector`:

```bash
uv run ruff format .
```

## Source Layout

```text
src/
  main.py                # job entrypoint
  pipeline.py            # orchestration and error handling
  habr_career_parser.py  # Habr Career crawler and parser
  publisher.py           # Kafka publishing
  schema.py              # Pydantic message models
  config.py              # environment-based settings
  logging.py             # structlog configuration
```
