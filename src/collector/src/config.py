from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Application settings loaded from environment variables or a .env file.
    """

    model_config = SettingsConfigDict(env_file=".env")

    LOG_LEVEL: str = "INFO"  # DEBUG | INFO | WARNING | ERROR | CRITICAL
    ENV: str = "prod"  # dev | prod


settings = Settings()
