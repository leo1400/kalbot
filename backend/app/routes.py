from datetime import datetime, timezone

from fastapi import APIRouter

from kalbot.schemas import HealthResponse, SignalCard
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
    # Placeholder signal returned until DB-backed publishing is wired.
    return [
        SignalCard(
            market_ticker="WEATHER-NYC-2026-02-17-HIGH-GT-45",
            title="NYC high temperature above 45F on Feb 17, 2026",
            probability_yes=0.61,
            market_implied_yes=0.54,
            edge=0.07,
            confidence=0.69,
            rationale=(
                "Prototype signal from baseline model. Replace with live features "
                "and daily-trained output in next phase."
            ),
            data_source_url="https://www.weather.gov/",
        )
    ]
