import streamlit as st


def configure_page() -> None:
    st.set_page_config(page_title="JobLake", layout="wide")
    st.markdown(
        """
        <style>
            .block-container {
                padding-top: 1.6rem;
                padding-bottom: 2rem;
            }
            div[data-testid="stMetric"] {
                background: var(--secondary-background-color);
                border: 1px solid rgba(128, 128, 128, 0.28);
                border-radius: 8px;
                color: var(--text-color);
                padding: 0.9rem 1rem;
            }
            div[data-testid="stMetric"] * {
                color: inherit;
            }
            div[data-testid="stMetricValue"] {
                font-size: 1.7rem;
                color: var(--text-color);
            }
            .stTabs [data-baseweb="tab-list"] {
                gap: 0.25rem;
            }
            .stTabs [data-baseweb="tab"] {
                padding: 0.5rem 0.8rem;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_header() -> None:
    st.title("JobLake")
    st.caption("Аналитика IT-вакансий из lakehouse и диалог с ИИ-агентом")
