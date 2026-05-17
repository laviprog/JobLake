# JobLake Collector Service

Ingestion service that collects vacancies from Habr Career and
publishes raw vacancy records to Kafka.

## Table of Contents

- [Responsibilities](#responsibilities)
- [Runtime Flow](#runtime-flow)
- [Published Messages](#published-messages)
- [Development Checks](#development-checks)

Back to the [project README](../../README.md).

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

## Development Checks

Run formatting from `src/collector`:

```bash
uv run ruff format .
```

Run the linter from `src/collector`:

```bash
uv run ruff check .
```
