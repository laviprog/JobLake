from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Application settings loaded from environment variables or a .env file.
    """

    model_config = SettingsConfigDict(env_file=".env")

    LOG_LEVEL: str = "INFO"  # DEBUG | INFO | WARNING | ERROR | CRITICAL
    ENV: str = "prod"  # dev | prod

    APP_HOST: str = "0.0.0.0"
    APP_PORT: int = 8501

    AGENT_URL: str = "http://agent:8080/api/v1"
    AGENT_TIMEOUT_SECONDS: float = 120.0

    TRINO_SCHEME: str = "http"
    TRINO_HOST: str = "trino"
    TRINO_PORT: int = 8080
    TRINO_USER: str = "joblake"
    TRINO_CATALOG: str = "joblake"
    TRINO_SCHEMA: str = "gold"
    TRINO_TIMEOUT_SECONDS: float = 30.0

    DASHBOARD_CACHE_TTL_SECONDS: int = 60
    DASHBOARD_DEFAULT_DAYS: int = 30
    DASHBOARD_TOP_LIMIT: int = 10
    DASHBOARD_MAX_LIMIT: int = 30


settings = Settings()
