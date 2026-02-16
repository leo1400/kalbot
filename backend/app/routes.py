from datetime import datetime, timezone

from fastapi import APIRouter, Query

from kalbot.bot_intel_repo import BotIntelRepositoryError, get_bot_leaderboard
from kalbot.schemas import BotLeaderboardEntry, HealthResponse, SignalCard
from kalbot.signals_repo import SignalRepositoryError, list_current_signals
from kalbot.settings import get_settings

router = APIRouter()


@router.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    settings = get_settings()
    return HealthResponse(
        status="ok",
        service=settings.app_name,
        environment=settings.environment,
        execution_mode=settings.execution_mode,
        timestamp_utc=datetime.now(timezone.utc),
    )


@router.get("/v1/signals/current", response_model=list[SignalCard])
def current_signals() -> list[SignalCard]:
    try:
        signals = list_current_signals(limit=20)
        if signals:
            return signals
    except SignalRepositoryError:
        # Keep API useful before DB is fully wired in every environment.
        pass

    return [
        SignalCard(
            market_ticker="WEATHER-NYC-BOOTSTRAP-HIGH-GT-45",
            title="Bootstrap signal (DB not yet populated)",
            probability_yes=0.61,
            market_implied_yes=0.54,
            edge=0.07,
            confidence=0.69,
            rationale="Run the daily worker to publish live rows into Postgres.",
            data_source_url="https://www.weather.gov/",
        )
    ]


@router.get("/v1/intel/leaderboard", response_model=list[BotLeaderboardEntry])
def bot_leaderboard(
    sort: str = Query(default="impressiveness", pattern="^(impressiveness|pnl|volume|roi)$"),
    window: str = Query(default="all", pattern="^(all|1m|1w|1d)$"),
    limit: int = Query(default=20, ge=1, le=100),
) -> list[BotLeaderboardEntry]:
    try:
        rows = get_bot_leaderboard(window=window, sort=sort, limit=limit)
        if rows:
            return rows
    except BotIntelRepositoryError:
        pass

    return []
