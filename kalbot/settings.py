from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    environment: str = Field(default="dev")
    app_name: str = Field(default="kalbot-api")
    log_level: str = Field(default="INFO")

    api_host: str = Field(default="0.0.0.0")
    api_port: int = Field(default=8000)

    database_url: str = Field(
        default="postgresql://postgres:postgres@localhost:5432/kalbot"
    )

    kalshi_api_base: str = Field(default="https://api.elections.kalshi.com/trade-api/v2")
    kalshi_api_key_id: str | None = Field(default=None)
    kalshi_private_key_path: str | None = Field(default=None)

    execution_mode: str = Field(default="paper")
    model_name: str = Field(default="baseline-logit-v1")
    model_refresh_hour_utc: int = Field(default=4)

    weather_api_base: str = Field(default="https://api.weather.gov")
    weather_user_agent: str = Field(default="kalbot-dev (kalbot@example.com)")
    weather_targets: str = Field(
        default="nyc:40.7128,-74.0060;chi:41.8781,-87.6298;mia:25.7617,-80.1918"
    )
    weather_forecast_hours: int = Field(default=24)

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="KALBOT_",
        extra="ignore",
    )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
