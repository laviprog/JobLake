from typing import Any

import pandas as pd
import streamlit as st

from src.config import settings
from src.services.queries import DashboardData

ALL_SOURCES_LABEL = "Все источники"


def render_filters(sources: list[str]) -> tuple[str | None, int, int]:
    st.sidebar.header("Фильтры")
    source_label = st.sidebar.selectbox("Источник", [ALL_SOURCES_LABEL, *sources])
    days = st.sidebar.slider(
        "Период, дней",
        min_value=1,
        max_value=365,
        value=settings.DASHBOARD_DEFAULT_DAYS,
    )
    limit = st.sidebar.slider(
        "Размер топов",
        min_value=5,
        max_value=settings.DASHBOARD_MAX_LIMIT,
        value=settings.DASHBOARD_TOP_LIMIT,
    )

    return (None if source_label == ALL_SOURCES_LABEL else source_label), days, limit


def render_dashboard(data: DashboardData, days: int, source: str | None) -> None:
    source_label = source or ALL_SOURCES_LABEL
    st.subheader("Сводка рынка")
    st.caption(f"Источник: {source_label}. Период: {days} дней.")
    render_metrics(data.metrics)

    st.divider()
    render_overview(data.overview)

    st.divider()
    demand_tabs = st.tabs(["Навыки", "Специализации", "Локации", "Компании", "Зарплаты"])

    with demand_tabs[0]:
        render_demand_table(data.skills, name_label="Навык")
    with demand_tabs[1]:
        render_demand_table(data.specializations, name_label="Специализация")
    with demand_tabs[2]:
        render_demand_table(data.locations, name_label="Локация")
    with demand_tabs[3]:
        render_company_table(data.companies)
    with demand_tabs[4]:
        render_salary_distribution(data.salaries)

    st.divider()
    render_recent_vacancies(data.vacancies)


def render_metrics(metrics: dict[str, Any]) -> None:
    cols = st.columns(5)
    vacancies = as_number(metrics.get("vacancies_count"))
    companies = as_number(metrics.get("companies_count"))
    remote_share = as_number(metrics.get("remote_share"))
    salary_count = as_number(metrics.get("salary_count"))
    salary_median = as_number(metrics.get("salary_median"))

    cols[0].metric("Вакансий", format_int(vacancies))
    cols[1].metric("Компаний", format_int(companies))
    cols[2].metric("Remote", format_percent(remote_share))
    cols[3].metric("С зарплатой", format_int(salary_count))
    cols[4].metric("Медиана", format_salary(salary_median))

    last_seen = metrics.get("last_seen_date")
    if last_seen:
        st.caption(f"Последняя дата в данных: {last_seen}")


def render_overview(frame: pd.DataFrame) -> None:
    st.subheader("Динамика")
    if frame.empty:
        st.info("Нет данных для выбранных фильтров.")
        return

    chart_frame = frame[["vacancy_date", "source", "vacancies_count"]].copy()
    st.line_chart(chart_frame, x="vacancy_date", y="vacancies_count", color="source")

    cols = st.columns(2)
    with cols[0]:
        remote_frame = frame[["vacancy_date", "source", "remote_share"]].copy()
        remote_frame["remote_share"] = remote_frame["remote_share"].fillna(0) * 100
        st.bar_chart(remote_frame, x="vacancy_date", y="remote_share", color="source")
    with cols[1]:
        salary_frame = frame[["vacancy_date", "source", "salary_median"]].dropna()
        if salary_frame.empty:
            st.info("Нет зарплатных данных для графика.")
        else:
            st.line_chart(salary_frame, x="vacancy_date", y="salary_median", color="source")


def render_demand_table(frame: pd.DataFrame, name_label: str) -> None:
    if frame.empty:
        st.info("Нет данных для выбранных фильтров.")
        return

    chart_frame = frame[["name", "vacancies_count"]].set_index("name")
    st.bar_chart(chart_frame)

    display = frame.copy()
    display["remote_share"] = display["remote_share"].fillna(0) * 100
    st.dataframe(
        display,
        use_container_width=True,
        hide_index=True,
        column_config={
            "name": st.column_config.TextColumn(name_label),
            "vacancies_count": st.column_config.NumberColumn("Вакансий", format="%d"),
            "companies_count": st.column_config.NumberColumn("Компаний", format="%d"),
            "remote_share": st.column_config.ProgressColumn(
                "Remote",
                min_value=0,
                max_value=100,
                format="%.1f%%",
            ),
            "salary_count": st.column_config.NumberColumn("С зарплатой", format="%d"),
            "salary_avg": st.column_config.NumberColumn("Средняя зарплата", format="%.0f"),
            "last_seen_date": st.column_config.DateColumn("Последнее появление"),
        },
    )


def render_company_table(frame: pd.DataFrame) -> None:
    if frame.empty:
        st.info("Нет данных для выбранных фильтров.")
        return

    st.bar_chart(frame[["company_name", "vacancies_count"]].set_index("company_name"))

    display = frame.copy()
    display["remote_share"] = display["remote_share"].fillna(0) * 100
    st.dataframe(
        display,
        use_container_width=True,
        hide_index=True,
        column_config={
            "company_name": st.column_config.TextColumn("Компания"),
            "company_url": st.column_config.LinkColumn("URL"),
            "vacancies_count": st.column_config.NumberColumn("Вакансий", format="%d"),
            "remote_count": st.column_config.NumberColumn("Remote вакансий", format="%d"),
            "remote_share": st.column_config.ProgressColumn(
                "Remote",
                min_value=0,
                max_value=100,
                format="%.1f%%",
            ),
            "salary_count": st.column_config.NumberColumn("С зарплатой", format="%d"),
            "salary_avg": st.column_config.NumberColumn("Средняя зарплата", format="%.0f"),
            "last_seen_date": st.column_config.DateColumn("Последнее появление"),
        },
    )


def render_salary_distribution(frame: pd.DataFrame) -> None:
    if frame.empty:
        st.info("Нет зарплатных данных для выбранных фильтров.")
        return

    chart_frame = frame[["salary_bucket", "vacancies_count"]].set_index("salary_bucket")
    st.bar_chart(chart_frame)
    st.dataframe(
        frame,
        use_container_width=True,
        hide_index=True,
        column_config={
            "salary_bucket": st.column_config.TextColumn("Диапазон"),
            "salary_currency": st.column_config.TextColumn("Валюта"),
            "salary_period": st.column_config.TextColumn("Период"),
            "vacancies_count": st.column_config.NumberColumn("Вакансий", format="%d"),
            "salary_min": st.column_config.NumberColumn("Минимум", format="%.0f"),
            "salary_max": st.column_config.NumberColumn("Максимум", format="%.0f"),
            "salary_avg": st.column_config.NumberColumn("Средняя", format="%.0f"),
            "salary_median": st.column_config.NumberColumn("Медиана", format="%.0f"),
        },
    )


def render_recent_vacancies(frame: pd.DataFrame) -> None:
    st.subheader("Свежие вакансии")
    if frame.empty:
        st.info("Нет вакансий для выбранных фильтров.")
        return

    st.dataframe(
        frame,
        use_container_width=True,
        hide_index=True,
        column_config={
            "vacancy_date": st.column_config.DateColumn("Дата"),
            "source": st.column_config.TextColumn("Источник"),
            "title": st.column_config.TextColumn("Вакансия"),
            "company_name": st.column_config.TextColumn("Компания"),
            "remote": st.column_config.CheckboxColumn("Remote"),
            "salary_from": st.column_config.NumberColumn("От", format="%.0f"),
            "salary_to": st.column_config.NumberColumn("До", format="%.0f"),
            "salary_currency": st.column_config.TextColumn("Валюта"),
            "salary_period": st.column_config.TextColumn("Период"),
            "skills": st.column_config.TextColumn("Навыки"),
            "specializations": st.column_config.TextColumn("Специализации"),
            "location": st.column_config.TextColumn("Локация"),
            "url": st.column_config.LinkColumn("URL"),
        },
    )


def render_data_error(error: Exception) -> None:
    st.error("Не удалось получить данные из Trino.")
    st.caption(str(error))


def as_number(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def format_int(value: float | None) -> str:
    if value is None:
        return "0"
    return f"{int(value):,}".replace(",", " ")


def format_percent(value: float | None) -> str:
    if value is None:
        return "0.0%"
    return f"{value * 100:.1f}%"


def format_salary(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{int(value):,} RUB".replace(",", " ")
