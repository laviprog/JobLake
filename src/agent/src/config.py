from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Application settings loaded from environment variables or a .env file.
    """

    model_config = SettingsConfigDict(env_file=".env")

    LOG_LEVEL: str = "INFO"  # DEBUG | INFO | WARNING | ERROR | CRITICAL
    ENV: str = "prod"  # dev | prod
    ROOT_PATH: str = "/api/v1"  # API root path

    OLLAMA_BASE_URL: str = "http://host.docker.internal:11434"
    OLLAMA_MODEL_NAME: str = "gemma4:31b-cloud"

    TRINO_SCHEME: str = "http"
    TRINO_HOST: str = "trino"
    TRINO_PORT: int = 8080
    TRINO_USER: str = "joblake"
    TRINO_CATALOG: str = "joblake"
    TRINO_SCHEMA: str = "gold"


settings = Settings()
