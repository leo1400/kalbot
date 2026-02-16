from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.request import Request, urlopen

from psycopg import errors

from kalbot.db import get_connection
from kalbot.settings import Settings


class WeatherIngestError(RuntimeError):
    pass


@dataclass(frozen=True)
class WeatherTarget:
    name: str
    latitude: float
    longitude: float


@dataclass
class WeatherIngestSummary:
    targets_attempted: int = 0
    targets_succeeded: int = 0
    forecast_rows_written: int = 0
    observation_rows_written: int = 0
    target_failures: list[str] | None = None

    def __post_init__(self) -> None:
        if self.target_failures is None:
            self.target_failures = []


def ingest_weather_data(settings: Settings) -> WeatherIngestSummary:
    targets = parse_weather_targets(settings.weather_targets)
    if not targets:
        raise WeatherIngestError("No weather targets configured.")

    summary = WeatherIngestSummary(targets_attempted=0)
    headers = {
        "User-Agent": settings.weather_user_agent,
        "Accept": "application/geo+json",
    }

    try:
        with get_connection() as conn, conn.cursor() as cur:
            targets = _augment_targets_with_market_cities(cur=cur, targets=targets)
            summary.targets_attempted = len(targets)
            for target in targets:
                try:
                    _ingest_target(
                        cur=cur,
                        headers=headers,
                        target=target,
                        settings=settings,
                        summary=summary,
                    )
                    summary.targets_succeeded += 1
                except Exception as exc:
                    summary.target_failures.append(f"{target.name}: {exc}")
    except errors.UndefinedTable as exc:
        raise WeatherIngestError(
            "Weather tables missing. Apply infra/migrations/001_initial_schema.sql."
        ) from exc
    except Exception as exc:
        raise WeatherIngestError(f"Failed weather ingestion: {exc}") from exc

    return summary


def parse_weather_targets(raw: str) -> list[WeatherTarget]:
    targets: list[WeatherTarget] = []
    for chunk in raw.split(";"):
        item = chunk.strip()
        if not item:
            continue
        try:
            name_part, coord_part = item.split(":", maxsplit=1)
            lat_text, lon_text = coord_part.split(",", maxsplit=1)
            targets.append(
                WeatherTarget(
                    name=name_part.strip(),
                    latitude=float(lat_text.strip()),
                    longitude=float(lon_text.strip()),
                )
            )
        except ValueError:
            continue
    return targets


def _augment_targets_with_market_cities(cur: Any, targets: list[WeatherTarget]) -> list[WeatherTarget]:
    target_by_name = {t.name.lower(): t for t in targets}
    cur.execute(
        """
        SELECT DISTINCT regexp_replace(market_ticker, '^KXLOWT([A-Z]+)-.*$', '\\1') AS city_code
        FROM markets
        WHERE market_ticker LIKE 'KXLOWT%-26%'
        """
    )
    rows = cur.fetchall()

    for row in rows:
        city_code = str(row["city_code"]).lower()
        if city_code in target_by_name:
            continue
        coords = _city_coordinates(city_code)
        if coords is None:
            continue
        target_by_name[city_code] = WeatherTarget(
            name=city_code,
            latitude=coords[0],
            longitude=coords[1],
        )

    return list(target_by_name.values())


def _city_coordinates(city_code: str) -> tuple[float, float] | None:
    lookup = {
        "nyc": (40.7128, -74.0060),
        "chi": (41.8781, -87.6298),
        "mia": (25.7617, -80.1918),
        "lax": (33.9416, -118.4085),
        "aus": (30.2672, -97.7431),
        "phil": (39.9526, -75.1652),
        "sf": (37.7749, -122.4194),
    }
    return lookup.get(city_code.lower())


def _ingest_target(
    cur: Any,
    headers: dict[str, str],
    target: WeatherTarget,
    settings: Settings,
    summary: WeatherIngestSummary,
) -> None:
    points_url = (
        f"{settings.weather_api_base}/points/{target.latitude},{target.longitude}"
    )
    points = _fetch_json(points_url, headers=headers, timeout_seconds=15)
    properties = points.get("properties", {})

    forecast_hourly_url = properties.get("forecastHourly")
    stations_url = properties.get("observationStations")
    if not forecast_hourly_url or not stations_url:
        raise WeatherIngestError("NWS point payload missing forecast/stations links.")

    station_id, station_observation_url = _resolve_station(
        stations_url=stations_url,
        headers=headers,
        target=target,
    )

    forecast_payload = _fetch_json(forecast_hourly_url, headers=headers, timeout_seconds=20)
    forecast_props = forecast_payload.get("properties", {})
    periods = forecast_props.get("periods", [])[: settings.weather_forecast_hours]
    issued_at_text = forecast_props.get("generatedAt") or datetime.now(
        timezone.utc
    ).isoformat()
    issued_at = _parse_datetime(issued_at_text)

    for period in periods:
        valid_at = _parse_datetime(period["startTime"])
        summary.forecast_rows_written += _upsert_forecast_metric(
            cur=cur,
            source="nws_hourly",
            station_id=station_id,
            issued_at=issued_at,
            valid_at=valid_at,
            metric="temperature",
            value=float(period["temperature"]),
            unit=str(period.get("temperatureUnit", "")),
        )

        precip = period.get("probabilityOfPrecipitation", {})
        if isinstance(precip, dict) and precip.get("value") is not None:
            summary.forecast_rows_written += _upsert_forecast_metric(
                cur=cur,
                source="nws_hourly",
                station_id=station_id,
                issued_at=issued_at,
                valid_at=valid_at,
                metric="precip_probability",
                value=float(precip["value"]),
                unit="percent",
            )

        humidity = period.get("relativeHumidity", {})
        if isinstance(humidity, dict) and humidity.get("value") is not None:
            summary.forecast_rows_written += _upsert_forecast_metric(
                cur=cur,
                source="nws_hourly",
                station_id=station_id,
                issued_at=issued_at,
                valid_at=valid_at,
                metric="relative_humidity",
                value=float(humidity["value"]),
                unit="percent",
            )

        wind_speed = _parse_wind_speed_mph(str(period.get("windSpeed", "")))
        if wind_speed is not None:
            summary.forecast_rows_written += _upsert_forecast_metric(
                cur=cur,
                source="nws_hourly",
                station_id=station_id,
                issued_at=issued_at,
                valid_at=valid_at,
                metric="wind_speed",
                value=wind_speed,
                unit="mph",
            )

    observation_payload = _fetch_json(
        station_observation_url, headers=headers, timeout_seconds=20
    )
    obs_props = observation_payload.get("properties", {})
    observed_at = _parse_datetime(obs_props["timestamp"])

    observation_metrics = [
        ("temperature", obs_props.get("temperature")),
        ("dewpoint", obs_props.get("dewpoint")),
        ("relative_humidity", obs_props.get("relativeHumidity")),
        ("wind_speed", obs_props.get("windSpeed")),
        ("barometric_pressure", obs_props.get("barometricPressure")),
        ("sea_level_pressure", obs_props.get("seaLevelPressure")),
        ("visibility", obs_props.get("visibility")),
        ("precipitation_last_hour", obs_props.get("precipitationLastHour")),
    ]

    for metric_name, payload in observation_metrics:
        value, unit = _measurement_value(payload)
        if value is None:
            continue
        summary.observation_rows_written += _upsert_observation_metric(
            cur=cur,
            station_id=station_id,
            observed_at=observed_at,
            metric=metric_name,
            value=value,
            unit=unit,
        )


def _resolve_station(
    stations_url: str, headers: dict[str, str], target: WeatherTarget
) -> tuple[str, str]:
    stations_payload = _fetch_json(stations_url, headers=headers, timeout_seconds=15)
    features = stations_payload.get("features", [])
    if not features:
        synthetic_id = target.name.upper()
        return synthetic_id, f"{stations_url.rstrip('/')}/observations/latest"

    station = features[0].get("properties", {})
    station_id = station.get("stationIdentifier") or target.name.upper()
    station_url = station.get("@id")
    if not station_url:
        station_url = features[0].get("id") or f"{stations_url.rstrip('/')}/0"
    return station_id, f"{station_url.rstrip('/')}/observations/latest"


def _fetch_json(
    url: str, headers: dict[str, str], timeout_seconds: int = 15
) -> dict[str, Any]:
    req = Request(url=url, headers=headers, method="GET")
    with urlopen(req, timeout=timeout_seconds) as response:
        payload = response.read().decode("utf-8")
    return json.loads(payload)


def _measurement_value(payload: Any) -> tuple[float | None, str]:
    if not isinstance(payload, dict):
        return None, ""
    value = payload.get("value")
    if value is None:
        return None, ""
    unit_code = str(payload.get("unitCode", ""))
    return float(value), unit_code


def _upsert_forecast_metric(
    cur: Any,
    source: str,
    station_id: str,
    issued_at: datetime,
    valid_at: datetime,
    metric: str,
    value: float,
    unit: str,
) -> int:
    cur.execute(
        """
        INSERT INTO weather_forecasts (
          source, station_id, issued_at, valid_at, metric, value, unit
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (source, station_id, issued_at, valid_at, metric)
        DO UPDATE SET
          value = EXCLUDED.value,
          unit = EXCLUDED.unit
        """,
        (source, station_id, issued_at, valid_at, metric, value, unit),
    )
    return cur.rowcount


def _upsert_observation_metric(
    cur: Any,
    station_id: str,
    observed_at: datetime,
    metric: str,
    value: float,
    unit: str,
) -> int:
    cur.execute(
        """
        INSERT INTO weather_observations (
          station_id, observed_at, metric, value, unit
        )
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (station_id, observed_at, metric)
        DO UPDATE SET
          value = EXCLUDED.value,
          unit = EXCLUDED.unit
        """,
        (station_id, observed_at, metric, value, unit),
    )
    return cur.rowcount


def _parse_wind_speed_mph(text: str) -> float | None:
    numbers = re.findall(r"\d+(?:\.\d+)?", text)
    if not numbers:
        return None
    values = [float(n) for n in numbers]
    return sum(values) / len(values)


def _parse_datetime(text: str) -> datetime:
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    return datetime.fromisoformat(text)
