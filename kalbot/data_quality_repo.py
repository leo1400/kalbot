from __future__ import annotations

from kalbot.db import get_connection
from kalbot.schemas import DataQualitySnapshot


class DataQualityRepositoryError(RuntimeError):
    pass


def get_data_quality_snapshot(target_stations: int) -> DataQualitySnapshot:
    query = """
        SELECT
          COALESCE((
            SELECT COUNT(*)
            FROM weather_forecasts wf
            WHERE wf.created_at >= NOW() - INTERVAL '24 hours'
          ), 0) AS forecast_rows_24h,
          COALESCE((
            SELECT COUNT(*)
            FROM weather_observations wo
            WHERE wo.created_at >= NOW() - INTERVAL '24 hours'
          ), 0) AS observation_rows_24h,
          COALESCE((
            SELECT COUNT(*)
            FROM markets m
            WHERE m.updated_at >= NOW() - INTERVAL '24 hours'
          ), 0) AS market_rows_24h,
          COALESCE((
            SELECT COUNT(*)
            FROM market_snapshots ms
            WHERE ms.captured_at >= NOW() - INTERVAL '24 hours'
          ), 0) AS snapshot_rows_24h,
          COALESCE((
            SELECT COUNT(DISTINCT wf.station_id)
            FROM weather_forecasts wf
            WHERE wf.created_at >= NOW() - INTERVAL '6 hours'
          ), 0) AS stations_with_forecast_6h,
          (
            SELECT EXTRACT(EPOCH FROM (NOW() - MAX(wf.created_at))) / 60.0
            FROM weather_forecasts wf
          ) AS latest_forecast_age_min,
          (
            SELECT EXTRACT(EPOCH FROM (NOW() - MAX(wo.created_at))) / 60.0
            FROM weather_observations wo
          ) AS latest_observation_age_min,
          (
            SELECT EXTRACT(EPOCH FROM (NOW() - MAX(ms.captured_at))) / 60.0
            FROM market_snapshots ms
          ) AS latest_snapshot_age_min
    """
    try:
        with get_connection() as conn, conn.cursor() as cur:
            cur.execute(query)
            row = cur.fetchone()
    except Exception as exc:
        raise DataQualityRepositoryError(f"Failed to load data quality snapshot: {exc}") from exc

    forecast_age = _optional_float(row["latest_forecast_age_min"])
    observation_age = _optional_float(row["latest_observation_age_min"])
    snapshot_age = _optional_float(row["latest_snapshot_age_min"])

    coverage_score = _ratio(float(row["stations_with_forecast_6h"]), float(max(1, target_stations)))
    freshness_score = (
        _freshness_component(forecast_age, 180.0)
        + _freshness_component(observation_age, 180.0)
        + _freshness_component(snapshot_age, 30.0)
    ) / 3.0
    quality_score = round((0.55 * freshness_score) + (0.45 * coverage_score), 4)

    status = "good"
    if quality_score < 0.45 or snapshot_age is None or snapshot_age > 60.0:
        status = "stale"
    elif quality_score < 0.75:
        status = "degraded"

    return DataQualitySnapshot(
        target_stations=int(max(1, target_stations)),
        stations_with_forecast_6h=int(row["stations_with_forecast_6h"]),
        forecast_rows_24h=int(row["forecast_rows_24h"]),
        observation_rows_24h=int(row["observation_rows_24h"]),
        market_rows_24h=int(row["market_rows_24h"]),
        snapshot_rows_24h=int(row["snapshot_rows_24h"]),
        latest_forecast_age_min=forecast_age,
        latest_observation_age_min=observation_age,
        latest_snapshot_age_min=snapshot_age,
        quality_score=quality_score,
        status=status,
    )


def empty_data_quality_snapshot(target_stations: int) -> DataQualitySnapshot:
    return DataQualitySnapshot(
        target_stations=max(1, target_stations),
        stations_with_forecast_6h=0,
        forecast_rows_24h=0,
        observation_rows_24h=0,
        market_rows_24h=0,
        snapshot_rows_24h=0,
        latest_forecast_age_min=None,
        latest_observation_age_min=None,
        latest_snapshot_age_min=None,
        quality_score=0.0,
        status="stale",
    )


def _optional_float(value: object) -> float | None:
    if value is None:
        return None
    return float(value)


def _ratio(value: float, total: float) -> float:
    if total <= 0:
        return 0.0
    return max(0.0, min(1.0, value / total))


def _freshness_component(age_min: float | None, threshold_min: float) -> float:
    if age_min is None:
        return 0.0
    return max(0.0, min(1.0, 1.0 - (age_min / threshold_min)))
