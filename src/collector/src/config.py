from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Application settings loaded from environment variables or a .env file.
    """

    model_config = SettingsConfigDict(env_file=".env")

    LOG_LEVEL: str = "INFO"  # DEBUG | INFO | WARNING | ERROR | CRITICAL
    ENV: str = "prod"  # dev | prod

    # Kafka configuration
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    KAFKA_TOPIC_RAW: str = "vacancies.raw"
    KAFKA_TOPIC_ERROR: str = "vacancies.errors"

    USER_AGENT: str = "JobLake collector/0.1 (+https://github.com/laviprog/joblake)"
    HABR_CAREER_BASE_URL: str = "https://career.habr.com"
    HABR_CAREER_REQUEST_DELAY_SECONDS: float = 0.5
    HABR_CAREER_MAX_PAGES: int | None = None


settings = Settings()
