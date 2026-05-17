import streamlit as st
import structlog

from src.config import settings
from src.logging import configure as configure_logging
from src.services.agent_client import AgentClient
from src.services.queries import fetch_sources, load_dashboard_data
from src.services.trino_client import TrinoClient, TrinoQueryError
from src.ui.chat import render_agent_chat
from src.ui.dashboard import render_dashboard, render_data_error, render_filters
from src.ui.layout import configure_page, render_header

_LOGGING_CONFIGURED = False


@st.cache_data(ttl=settings.DASHBOARD_CACHE_TTL_SECONDS, show_spinner=False)
def cached_sources() -> list[str]:
    return fetch_sources(TrinoClient())


@st.cache_data(ttl=settings.DASHBOARD_CACHE_TTL_SECONDS, show_spinner=False)
def cached_dashboard_data(days: int, source: str | None, limit: int):
    return load_dashboard_data(days=days, source=source, limit=limit)


def configure_app_logging() -> None:
    global _LOGGING_CONFIGURED

    if _LOGGING_CONFIGURED:
        return

    if not structlog.is_configured():
        configure_logging()
    _LOGGING_CONFIGURED = True


def main() -> None:
    configure_app_logging()
    configure_page()
    render_header()

    section = st.sidebar.radio("Раздел", ["Дашборд", "ИИ-агент"])

    if st.sidebar.button("Обновить данные", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    if section == "ИИ-агент":
        render_agent_chat(AgentClient())
        return

    try:
        sources = cached_sources()
    except TrinoQueryError as exc:
        sources = []
        st.sidebar.warning("Источники пока недоступны.")
        source, days, limit = render_filters(sources)
        dashboard_error: Exception | None = exc
    else:
        source, days, limit = render_filters(sources)
        dashboard_error = None

    if dashboard_error is not None:
        render_data_error(dashboard_error)
    else:
        try:
            with st.spinner("Загружаю данные из Trino..."):
                data = cached_dashboard_data(days=days, source=source, limit=limit)
            render_dashboard(data=data, days=days, source=source)
        except TrinoQueryError as exc:
            render_data_error(exc)


if __name__ == "__main__":
    main()
