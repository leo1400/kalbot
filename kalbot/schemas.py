from datetime import datetime

from pydantic import BaseModel


class HealthResponse(BaseModel):
    status: str
    service: str
    environment: str
    execution_mode: str
    timestamp_utc: datetime


class SignalCard(BaseModel):
    market_ticker: str
    title: str
    city_code: str | None = None
    city_name: str | None = None
    probability_yes: float
    market_implied_yes: float
    edge: float
    confidence: float
    rationale: str
    data_source_url: str


class BotLeaderboardEntry(BaseModel):
    rank: int
    platform: str
    display_name: str
    account_address: str
    entity_type: str
    roi_pct: float
    pnl_usd: float
    volume_usd: float
    impressiveness_score: float
    snapshot_date: str
    source: str


class DashboardSummary(BaseModel):
    active_signal_count: int
    avg_confidence: float
    avg_edge: float
    strongest_edge: float
    updated_at_utc: datetime


class CopyActivityEvent(BaseModel):
    event_time: datetime
    follower_alias: str
    leader_display_name: str
    market_ticker: str
    source: str
    side: str
    contracts: int
    pnl_usd: float


class PerformanceSummary(BaseModel):
    total_orders: int
    orders_24h: int
    approved_decisions_24h: int
    open_positions: int
    notional_24h_usd: float
    open_notional_usd: float
    realized_pnl_usd: float


class PerformanceHistoryPoint(BaseModel):
    day: str
    orders: int
    notional_usd: float


class DataQualitySnapshot(BaseModel):
    target_stations: int
    stations_with_forecast_6h: int
    forecast_rows_24h: int
    observation_rows_24h: int
    market_rows_24h: int
    snapshot_rows_24h: int
    latest_forecast_age_min: float | None
    latest_observation_age_min: float | None
    latest_snapshot_age_min: float | None
    quality_score: float
    status: str


class PlaybookSignal(BaseModel):
    market_ticker: str
    title: str
    city_code: str | None = None
    city_name: str | None = None
    action: str
    edge: float
    confidence: float
    probability_yes: float
    market_implied_yes: float
    suggested_contracts: int
    suggested_notional_usd: float
    entry_price: float
    note: str


class PaperOrderRow(BaseModel):
    created_at: datetime
    market_ticker: str
    side: str
    contracts: int
    limit_price: float
    status: str
    edge: float
