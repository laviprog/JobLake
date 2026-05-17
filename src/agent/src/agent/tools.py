import json
from typing import Any

from src.agent.trino_client import trino_client

MAX_LIMIT = 20


def _limit(limit: int, default: int = 10) -> int:
    if limit <= 0:
        return default
    return min(limit, MAX_LIMIT)


def _to_json(rows: list[dict[str, Any]]) -> str:
    return json.dumps(rows, ensure_ascii=False, default=str)


async def get_market_overview(days: int = 30, source: str | None = None) -> str:
    """
    Get daily IT vacancies market overview for the latest N days.

    Args:
        days: Number of latest days to include. Use 30 by default.
        source: Optional vacancy source filter.

    Returns:
        JSON array with daily vacancies, companies, remote share and salary metrics.
    """
    days = min(max(days, 1), 365)

    sql = (
        """
        SELECT
            vacancy_date,
            source,
            vacancies_count,
            companies_count,
            remote_count,
            remote_share,
            salary_count,
            salary_avg,
            salary_median,
            salary_min,
            salary_max
        FROM joblake.gold.vacancy_overview_daily
        WHERE vacancy_date >= current_date - INTERVAL '%s' DAY
    """
        % days
    )

    params: list[Any] = []

    if source:
        sql += " AND source = ?"
        params.append(source)

    sql += " ORDER BY vacancy_date DESC, vacancies_count DESC"

    return _to_json(await trino_client.fetch_all(sql, tuple(params)))


async def get_top_skills(limit: int = 10, source: str | None = None) -> str:
    """
    Get top demanded skills in IT vacancies.

    Args:
        limit: Maximum number of skills to return.
        source: Optional vacancy source filter.

    Returns:
        JSON array with skill demand, company count, remote share and salary metrics.
    """
    limit = _limit(limit)

    sql = """
        SELECT
            source,
            skill_name,
            vacancies_count,
            demand_share,
            companies_count,
            remote_count,
            remote_share,
            salary_count,
            salary_avg,
            salary_median,
            first_seen_date,
            last_seen_date
        FROM joblake.gold.skill_demand
        WHERE 1 = 1
    """

    params: list[Any] = []

    if source:
        sql += " AND source = ?"
        params.append(source)

    sql += """
        ORDER BY vacancies_count DESC, demand_share DESC
        LIMIT ?
    """
    params.append(limit)

    return _to_json(await trino_client.fetch_all(sql, tuple(params)))


async def get_top_specializations(limit: int = 10, source: str | None = None) -> str:
    """
    Get top demanded IT specializations.

    Args:
        limit: Maximum number of specializations to return.
        source: Optional vacancy source filter.

    Returns:
        JSON array with specialization demand, company count, remote share and salary metrics.
    """
    limit = _limit(limit)

    sql = """
        SELECT
            source,
            specialization_name,
            vacancies_count,
            demand_share,
            companies_count,
            remote_count,
            remote_share,
            salary_count,
            salary_avg,
            salary_median,
            first_seen_date,
            last_seen_date
        FROM joblake.gold.specialization_demand
        WHERE 1 = 1
    """

    params: list[Any] = []

    if source:
        sql += " AND source = ?"
        params.append(source)

    sql += """
        ORDER BY vacancies_count DESC, demand_share DESC
        LIMIT ?
    """
    params.append(limit)

    return _to_json(await trino_client.fetch_all(sql, tuple(params)))


async def get_top_locations(limit: int = 10, source: str | None = None) -> str:
    """
    Get top locations by vacancy demand.

    Args:
        limit: Maximum number of locations to return.
        source: Optional vacancy source filter.

    Returns:
        JSON array with location demand, company count, remote share and salary metrics.
    """
    limit = _limit(limit)

    sql = """
        SELECT
            source,
            location_name,
            vacancies_count,
            demand_share,
            companies_count,
            remote_count,
            remote_share,
            salary_count,
            salary_avg,
            salary_median,
            first_seen_date,
            last_seen_date
        FROM joblake.gold.location_demand
        WHERE 1 = 1
    """

    params: list[Any] = []

    if source:
        sql += " AND source = ?"
        params.append(source)

    sql += """
        ORDER BY vacancies_count DESC, demand_share DESC
        LIMIT ?
    """
    params.append(limit)

    return _to_json(await trino_client.fetch_all(sql, tuple(params)))


async def get_top_companies(limit: int = 10, source: str | None = None) -> str:
    """
    Get companies with the largest number of active IT vacancies.

    Args:
        limit: Maximum number of companies to return.
        source: Optional vacancy source filter.

    Returns:
        JSON array with company vacancy counts, remote share, salary metrics and URLs.
    """
    limit = _limit(limit)

    sql = """
        SELECT
            source,
            company_name,
            company_url,
            company_site,
            vacancies_count,
            remote_count,
            remote_share,
            salary_count,
            salary_avg,
            salary_median,
            skills_count,
            specializations_count,
            locations_count,
            first_seen_date,
            last_seen_date
        FROM joblake.gold.company_demand
        WHERE 1 = 1
    """

    params: list[Any] = []

    if source:
        sql += " AND source = ?"
        params.append(source)

    sql += """
        ORDER BY vacancies_count DESC
        LIMIT ?
    """
    params.append(limit)

    return _to_json(await trino_client.fetch_all(sql, tuple(params)))


async def search_vacancies(
    query: str,
    limit: int = 10,
    remote_only: bool | None = None,
    source: str | None = None,
) -> str:
    """
    Search active IT vacancies by text query.

    Args:
        query: Search phrase, for example Python, Data Engineer, Kubernetes or company name.
        limit: Maximum number of vacancies to return.
        remote_only: If true, return only remote vacancies. If false, return only
            non-remote vacancies.
        source: Optional vacancy source filter.

    Returns:
        JSON array with matching vacancies, salaries, skills, locations and URLs.
    """
    limit = _limit(limit)

    sql = """
        SELECT
            vacancy_uid,
            source,
            title,
            company_name,
            url,
            vacancy_date,
            published_at,
            employment_type_text,
            remote,
            qualification,
            salary_from,
            salary_to,
            salary_midpoint,
            salary_currency,
            salary_period,
            skills,
            specializations,
            human_city_names,
            short_geo
        FROM joblake.gold.vacancies_enriched
        WHERE search_text LIKE ?
    """

    params: list[Any] = [f"%{query.lower()}%"]

    if remote_only is not None:
        sql += " AND remote = ?"
        params.append(remote_only)

    if source:
        sql += " AND source = ?"
        params.append(source)

    sql += """
        ORDER BY vacancy_date DESC, published_at DESC
        LIMIT ?
    """
    params.append(limit)

    return _to_json(await trino_client.fetch_all(sql, tuple(params)))


async def get_salary_distribution(
    limit: int = 20,
    source: str | None = None,
    currency: str | None = None,
    period: str | None = None,
    remote_only: bool | None = None,
) -> str:
    """
    Get salary distribution by salary buckets.

    Args:
        limit: Maximum number of buckets to return.
        source: Optional vacancy source filter.
        currency: Optional salary currency filter.
        period: Optional salary period filter.
        remote_only: If true, return only remote buckets. If false, only non-remote buckets.

    Returns:
        JSON array with salary buckets and salary metrics.
    """
    limit = _limit(limit, default=20)

    sql = """
        SELECT
            source,
            salary_currency,
            salary_period,
            qualification,
            employment,
            remote,
            salary_bucket,
            vacancies_count,
            salary_min,
            salary_max,
            salary_avg,
            salary_median
        FROM joblake.gold.salary_distribution
        WHERE 1 = 1
    """

    params: list[Any] = []

    if source:
        sql += " AND source = ?"
        params.append(source)

    if currency:
        sql += " AND salary_currency = ?"
        params.append(currency)

    if period:
        sql += " AND salary_period = ?"
        params.append(period)

    if remote_only is not None:
        sql += " AND remote = ?"
        params.append(remote_only)

    sql += """
        ORDER BY vacancies_count DESC
        LIMIT ?
    """
    params.append(limit)

    return _to_json(await trino_client.fetch_all(sql, tuple(params)))


TOOLS = [
    get_market_overview,
    get_top_skills,
    get_top_specializations,
    get_top_locations,
    get_top_companies,
    search_vacancies,
    get_salary_distribution,
]

AVAILABLE_TOOLS = {tool.__name__: tool for tool in TOOLS}
