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
    side: str
    contracts: int
    pnl_usd: float
