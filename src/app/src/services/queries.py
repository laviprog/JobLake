import re
from dataclasses import dataclass
from typing import Any

import pandas as pd

from src.config import settings
from src.services.trino_client import TrinoClient

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class DashboardData:
    metrics: dict[str, Any]
    overview: pd.DataFrame
    skills: pd.DataFrame
    specializations: pd.DataFrame
    locations: pd.DataFrame
    companies: pd.DataFrame
    salaries: pd.DataFrame
    vacancies: pd.DataFrame


def fetch_sources(client: TrinoClient) -> list[str]:
    rows = client.fetch_all(
        f"""
        SELECT DISTINCT source
        FROM {gold_table("vacancy_overview_daily")}
        WHERE source IS NOT NULL
        ORDER BY source
        """
    )
    return [row["source"] for row in rows if row.get("source")]


def load_dashboard_data(days: int, source: str | None, limit: int) -> DashboardData:
    client = TrinoClient()
    days = clamp_int(days, minimum=1, maximum=365)
    limit = clamp_int(limit, minimum=1, maximum=settings.DASHBOARD_MAX_LIMIT)

    return DashboardData(
        metrics=fetch_metrics(client, days, source),
        overview=fetch_overview(client, days, source),
        skills=fetch_item_demand(client, "skill_demand", "skill_name", source, limit),
        specializations=fetch_item_demand(
            client,
            "specialization_demand",
            "specialization_name",
            source,
            limit,
        ),
        locations=fetch_item_demand(client, "location_demand", "location_name", source, limit),
        companies=fetch_company_demand(client, source, limit),
        salaries=fetch_salary_distribution(client, source),
        vacancies=fetch_recent_vacancies(client, days, source, limit=20),
    )


def fetch_metrics(client: TrinoClient, days: int, source: str | None) -> dict[str, Any]:
    rows = client.fetch_all(
        f"""
        SELECT
            COUNT(DISTINCT vacancy_uid) AS vacancies_count,
            COUNT(DISTINCT company_key) AS companies_count,
            SUM(CASE WHEN remote THEN 1 ELSE 0 END) AS remote_count,
            CAST(SUM(CASE WHEN remote THEN 1 ELSE 0 END) AS DOUBLE)
                / NULLIF(COUNT(DISTINCT vacancy_uid), 0) AS remote_share,
            COUNT(salary_midpoint) AS salary_count,
            AVG(salary_midpoint) AS salary_avg,
            approx_percentile(salary_midpoint, 0.5) AS salary_median,
            MIN(vacancy_date) AS first_seen_date,
            MAX(vacancy_date) AS last_seen_date
        FROM {gold_table("vacancies_enriched")}
        WHERE vacancy_date >= current_date - INTERVAL '{days}' DAY
        {source_filter(source)}
        """,
        source_params(source),
    )
    return rows[0] if rows else {}


def fetch_overview(client: TrinoClient, days: int, source: str | None) -> pd.DataFrame:
    rows = client.fetch_all(
        f"""
        SELECT
            vacancy_date,
            source,
            vacancies_count,
            companies_count,
            remote_count,
            remote_share,
            salary_count,
            salary_avg,
            salary_median
        FROM {gold_table("vacancy_overview_daily")}
        WHERE vacancy_date >= current_date - INTERVAL '{days}' DAY
        {source_filter(source)}
        ORDER BY vacancy_date ASC, source ASC
        """,
        source_params(source),
    )
    frame = to_frame(
        rows,
        [
            "vacancy_date",
            "source",
            "vacancies_count",
            "companies_count",
            "remote_count",
            "remote_share",
            "salary_count",
            "salary_avg",
            "salary_median",
        ],
    )
    if not frame.empty:
        frame["vacancy_date"] = pd.to_datetime(frame["vacancy_date"])
    return frame


def fetch_item_demand(
    client: TrinoClient,
    table_name: str,
    name_column: str,
    source: str | None,
    limit: int,
) -> pd.DataFrame:
    rows = client.fetch_all(
        f"""
        SELECT
            {identifier(name_column)} AS name,
            SUM(vacancies_count) AS vacancies_count,
            SUM(companies_count) AS companies_count,
            CAST(SUM(remote_count) AS DOUBLE) / NULLIF(SUM(vacancies_count), 0) AS remote_share,
            SUM(salary_count) AS salary_count,
            AVG(salary_avg) AS salary_avg,
            MAX(last_seen_date) AS last_seen_date
        FROM {gold_table(table_name)}
        WHERE {identifier(name_column)} IS NOT NULL
        {source_filter(source)}
        GROUP BY {identifier(name_column)}
        ORDER BY vacancies_count DESC, name ASC
        LIMIT {limit}
        """,
        source_params(source),
    )
    return to_frame(
        rows,
        [
            "name",
            "vacancies_count",
            "companies_count",
            "remote_share",
            "salary_count",
            "salary_avg",
            "last_seen_date",
        ],
    )


def fetch_company_demand(client: TrinoClient, source: str | None, limit: int) -> pd.DataFrame:
    rows = client.fetch_all(
        f"""
        SELECT
            company_name,
            MIN(company_url) AS company_url,
            SUM(vacancies_count) AS vacancies_count,
            SUM(remote_count) AS remote_count,
            CAST(SUM(remote_count) AS DOUBLE) / NULLIF(SUM(vacancies_count), 0) AS remote_share,
            SUM(salary_count) AS salary_count,
            AVG(salary_avg) AS salary_avg,
            MAX(last_seen_date) AS last_seen_date
        FROM {gold_table("company_demand")}
        WHERE company_name IS NOT NULL
        {source_filter(source)}
        GROUP BY company_name
        ORDER BY vacancies_count DESC, company_name ASC
        LIMIT {limit}
        """,
        source_params(source),
    )
    return to_frame(
        rows,
        [
            "company_name",
            "company_url",
            "vacancies_count",
            "remote_count",
            "remote_share",
            "salary_count",
            "salary_avg",
            "last_seen_date",
        ],
    )


def fetch_salary_distribution(client: TrinoClient, source: str | None) -> pd.DataFrame:
    rows = client.fetch_all(
        f"""
        SELECT
            salary_bucket,
            COALESCE(salary_currency, 'unknown') AS salary_currency,
            COALESCE(salary_period, 'unknown') AS salary_period,
            SUM(vacancies_count) AS vacancies_count,
            MIN(salary_min) AS salary_min,
            MAX(salary_max) AS salary_max,
            AVG(salary_avg) AS salary_avg,
            AVG(salary_median) AS salary_median
        FROM {gold_table("salary_distribution")}
        WHERE salary_bucket IS NOT NULL
        {source_filter(source)}
        GROUP BY 1, 2, 3
        ORDER BY
            CASE salary_bucket
                WHEN '<50k' THEN 1
                WHEN '50k-100k' THEN 2
                WHEN '100k-150k' THEN 3
                WHEN '150k-200k' THEN 4
                WHEN '200k-250k' THEN 5
                WHEN '250k-300k' THEN 6
                WHEN '300k-400k' THEN 7
                WHEN '400k+' THEN 8
                ELSE 99
            END,
            vacancies_count DESC
        """,
        source_params(source),
    )
    return to_frame(
        rows,
        [
            "salary_bucket",
            "salary_currency",
            "salary_period",
            "vacancies_count",
            "salary_min",
            "salary_max",
            "salary_avg",
            "salary_median",
        ],
    )


def fetch_recent_vacancies(
    client: TrinoClient,
    days: int,
    source: str | None,
    limit: int,
) -> pd.DataFrame:
    rows = client.fetch_all(
        f"""
        SELECT
            vacancy_date,
            source,
            title,
            company_name,
            remote,
            salary_from,
            salary_to,
            salary_currency,
            salary_period,
            COALESCE(array_join(skills, ', '), '') AS skills,
            COALESCE(array_join(specializations, ', '), '') AS specializations,
            COALESCE(human_city_names, short_geo, '') AS location,
            url
        FROM {gold_table("vacancies_enriched")}
        WHERE vacancy_date >= current_date - INTERVAL '{days}' DAY
        {source_filter(source)}
        ORDER BY vacancy_date DESC, published_at DESC
        LIMIT {limit}
        """,
        source_params(source),
    )
    return to_frame(
        rows,
        [
            "vacancy_date",
            "source",
            "title",
            "company_name",
            "remote",
            "salary_from",
            "salary_to",
            "salary_currency",
            "salary_period",
            "skills",
            "specializations",
            "location",
            "url",
        ],
    )


def gold_table(table_name: str) -> str:
    return ".".join(
        [
            identifier(settings.TRINO_CATALOG),
            identifier(settings.TRINO_SCHEMA),
            identifier(table_name),
        ]
    )


def source_filter(source: str | None) -> str:
    if not source:
        return ""
    return "AND source = ?"


def source_params(source: str | None) -> tuple[Any, ...]:
    if not source:
        return ()
    return (source,)


def identifier(value: str) -> str:
    if not _IDENTIFIER_RE.fullmatch(value):
        raise ValueError(f"Invalid SQL identifier: {value}")
    return value


def clamp_int(value: int, minimum: int, maximum: int) -> int:
    return max(minimum, min(int(value), maximum))


def to_frame(rows: list[dict[str, Any]], columns: list[str]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=columns)
