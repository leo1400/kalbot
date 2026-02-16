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
    kalshi_ingest_enabled: bool = Field(default=True)
    kalshi_weather_category: str = Field(default="Climate and Weather")
    kalshi_weather_series_limit: int = Field(default=12)
    kalshi_markets_per_series: int = Field(default=40)
    kalshi_series_page_size: int = Field(default=200)

    execution_mode: str = Field(default="paper")
    model_name: str = Field(default="baseline-logit-v1")
    model_refresh_hour_utc: int = Field(default=4)
    signal_publish_limit: int = Field(default=4)
    paper_edge_threshold: float = Field(default=0.03)
    max_notional_per_signal_usd: float = Field(default=125.0)
    max_daily_notional_usd: float = Field(default=500.0)
    max_contracts_per_order: int = Field(default=25)

    weather_api_base: str = Field(default="https://api.weather.gov")
    weather_user_agent: str = Field(default="kalbot-dev (kalbot@example.com)")
    weather_targets: str = Field(
        default=(
            "nyc:40.7128,-74.0060;chi:41.8781,-87.6298;"
            "mia:25.7617,-80.1918;lax:33.9416,-118.4085"
        )
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
