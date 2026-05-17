from contextlib import asynccontextmanager

from fastapi import FastAPI

from src import log
from src.agent.tools import TOOLS
from src.config import settings


@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info(
        "Starting application",
        app_title=app.title,
        root_path=settings.ROOT_PATH,
        ollama_base_url=settings.OLLAMA_BASE_URL,
        model=settings.OLLAMA_MODEL_NAME,
        trino_host=settings.TRINO_HOST,
        trino_port=settings.TRINO_PORT,
        trino_catalog=settings.TRINO_CATALOG,
        trino_schema=settings.TRINO_SCHEMA,
        tools_count=len(TOOLS),
    )
    yield
    log.info("Application shut down")
