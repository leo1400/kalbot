from __future__ import annotations

from datetime import datetime, timezone

from kalbot.db import get_connection
from kalbot.schemas import (
    CityProvenanceRow,
    DataProvenanceSnapshot,
    SourceProvenanceRow,
)


class ProvenanceRepositoryError(RuntimeError):
    pass


def get_data_provenance_snapshot() -> DataProvenanceSnapshot:
    try:
        with get_connection() as conn, conn.cursor() as cur:
            sources = _load_sources(cur)
            cities = _load_city_rows(cur)
    except Exception as exc:
        raise ProvenanceRepositoryError(f"Failed to load provenance snapshot: {exc}") from exc

    return DataProvenanceSnapshot(
        generated_at_utc=datetime.now(timezone.utc),
        sources=sources,
        cities=cities,
    )


def empty_data_provenance_snapshot() -> DataProvenanceSnapshot:
    return DataProvenanceSnapshot(
        generated_at_utc=datetime.now(timezone.utc),
        sources=[],
        cities=[],
    )


def _load_sources(cur) -> list[SourceProvenanceRow]:
    cur.execute(
        """
        SELECT
          (SELECT MAX(created_at) FROM weather_forecasts) AS weather_last,
          (SELECT MAX(captured_at) FROM market_snapshots) AS kalshi_last,
          (SELECT MAX(event_time) FROM copy_activity_events) AS bot_last,
          (SELECT COUNT(*) FROM copy_activity_events) AS bot_total_count,
          (
            SELECT COUNT(*)
            FROM copy_activity_events
            WHERE source ILIKE '%demo%'
               OR source ILIKE '%seed%'
          ) AS bot_synthetic_count
        """
    )
    row = cur.fetchone()

    weather_age = _age_minutes(row["weather_last"])
    kalshi_age = _age_minutes(row["kalshi_last"])
    bot_age = _age_minutes(row["bot_last"])

    bot_total = int(row["bot_total_count"] or 0)
    bot_synthetic = int(row["bot_synthetic_count"] or 0)
    if bot_total == 0:
        bot_mode = "unavailable"
        bot_note = "no bot intel feed rows ingested yet"
    elif bot_synthetic == bot_total:
        bot_mode = "demo"
        bot_note = "all bot intel rows are synthetic/demo"
    else:
        bot_mode = "real"
        bot_note = "non-demo bot intel activity observed"

    return [
        SourceProvenanceRow(
            source_key="weather_nws",
            mode="real",
            status=_fresh_status(weather_age, good_max=180.0, degraded_max=360.0),
            last_event_utc=row["weather_last"],
            note="NOAA/NWS forecasts and observations",
        ),
        SourceProvenanceRow(
            source_key="kalshi_market_data",
            mode="real",
            status=_fresh_status(kalshi_age, good_max=10.0, degraded_max=30.0),
            last_event_utc=row["kalshi_last"],
            note="Kalshi weather market snapshots",
        ),
        SourceProvenanceRow(
            source_key="bot_intel_feed",
            mode=bot_mode,
            status=_fresh_status(bot_age, good_max=60.0, degraded_max=360.0),
            last_event_utc=row["bot_last"],
            note=bot_note,
        ),
    ]


def _load_city_rows(cur) -> list[CityProvenanceRow]:
    cur.execute(
        """
        WITH city_markets AS (
          SELECT
            regexp_replace(m.market_ticker, '^KXLOWT([A-Z]+)-.*$', '\\1') AS city_code,
            COUNT(*) FILTER (WHERE m.close_time IS NULL OR m.close_time > NOW()) AS open_market_count,
            MAX(ms.captured_at) AS latest_snapshot_at
          FROM markets m
          LEFT JOIN LATERAL (
            SELECT captured_at
            FROM market_snapshots ms
            WHERE ms.market_id = m.id
            ORDER BY ms.captured_at DESC
            LIMIT 1
          ) ms ON TRUE
          WHERE m.market_ticker LIKE 'KXLOWT%-26%'
          GROUP BY 1
        ),
        active_signal_cities AS (
          SELECT DISTINCT regexp_replace(m.market_ticker, '^KXLOWT([A-Z]+)-.*$', '\\1') AS city_code
          FROM published_signals ps
          JOIN markets m ON m.id = ps.market_id
          WHERE ps.is_active = TRUE
            AND m.market_ticker LIKE 'KXLOWT%-26%'
        )
        SELECT
          cm.city_code,
          cm.open_market_count,
          cm.latest_snapshot_at,
          CASE WHEN act.city_code IS NULL THEN FALSE ELSE TRUE END AS has_active_signal
        FROM city_markets cm
        LEFT JOIN active_signal_cities act ON act.city_code = cm.city_code
        ORDER BY cm.open_market_count DESC, cm.city_code ASC
        """
    )
    rows = cur.fetchall()

    result: list[CityProvenanceRow] = []
    for row in rows:
        city_code = str(row["city_code"])
        city_name = _city_name_from_code(city_code)
        snapshot_age = _age_minutes(row["latest_snapshot_at"])

        cur.execute(
            """
            SELECT MAX(created_at) AS latest_forecast_at
            FROM weather_forecasts
            WHERE station_id = ANY(%s)
            """,
            (_station_candidates(city_code),),
        )
        forecast_row = cur.fetchone()
        forecast_age = _age_minutes(forecast_row["latest_forecast_at"])

        coverage_status = _city_coverage_status(snapshot_age, forecast_age)
        result.append(
            CityProvenanceRow(
                city_code=city_code,
                city_name=city_name,
                open_market_count=int(row["open_market_count"] or 0),
                has_active_signal=bool(row["has_active_signal"]),
                latest_snapshot_age_min=snapshot_age,
                latest_forecast_age_min=forecast_age,
                coverage_status=coverage_status,
            )
        )

    return result


def _age_minutes(ts: datetime | None) -> float | None:
    if ts is None:
        return None
    now = datetime.now(timezone.utc)
    return max(0.0, (now - ts).total_seconds() / 60.0)


def _fresh_status(age_min: float | None, good_max: float, degraded_max: float) -> str:
    if age_min is None:
        return "stale"
    if age_min <= good_max:
        return "good"
    if age_min <= degraded_max:
        return "degraded"
    return "stale"


def _city_coverage_status(snapshot_age: float | None, forecast_age: float | None) -> str:
    if snapshot_age is None or snapshot_age > 60.0:
        return "stale_market"
    if forecast_age is None:
        return "market_only"
    if forecast_age <= 180.0 and snapshot_age <= 30.0:
        return "model_ready"
    if forecast_age <= 360.0:
        return "degraded"
    return "stale_weather"


def _station_candidates(city_code: str) -> list[str]:
    base = city_code.upper()
    aliases = {
        "PHIL": ["KPHL", "PHIL", "KPHIL"],
        "NYC": ["KNYC", "KJFK", "KLGA", "KEWR", "NYC"],
        "LAX": ["KLAX", "LAX"],
        "CHI": ["KORD", "KMDW", "CHI"],
        "MIA": ["KMIA", "MIA"],
        "SF": ["KSFO", "SFO"],
        "AUS": ["KAUS", "KATT", "AUS"],
    }
    if base in aliases:
        return aliases[base]
    return [f"K{base}", base]


def _city_name_from_code(city_code: str) -> str:
    names = {
        "LAX": "Los Angeles",
        "NYC": "New York City",
        "PHIL": "Philadelphia",
        "CHI": "Chicago",
        "MIA": "Miami",
        "SF": "San Francisco",
        "AUS": "Austin",
    }
    upper = city_code.upper()
    return names.get(upper, upper)
