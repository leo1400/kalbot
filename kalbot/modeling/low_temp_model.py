from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from kalbot.db import get_connection


@dataclass
class FeatureBuildSummary:
    examples: int
    stations: int
    output_path: str


@dataclass
class TrainSummary:
    samples: int
    stations: int
    global_sigma_f: float
    rmse_f: float
    output_path: str


def build_low_temp_training_features(run_date: str) -> FeatureBuildSummary:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            WITH latest_forecast AS (
              SELECT DISTINCT ON (station_id, DATE(valid_at))
                station_id,
                DATE(valid_at) AS forecast_date,
                issued_at,
                valid_at,
                value,
                unit
              FROM weather_forecasts
              WHERE metric = 'temperature'
              ORDER BY station_id, DATE(valid_at), issued_at DESC, valid_at ASC
            ),
            forecast_daily AS (
              SELECT
                station_id,
                forecast_date,
                MIN(
                  CASE
                    WHEN upper(unit) IN ('C', 'DEGC', 'WMOUNIT:DEGC')
                      THEN (value * 9.0 / 5.0) + 32.0
                    ELSE value
                  END
                ) AS forecast_low_f
              FROM latest_forecast
              GROUP BY station_id, forecast_date
            ),
            obs_daily AS (
              SELECT
                station_id,
                DATE(observed_at) AS obs_date,
                MIN(
                  CASE
                    WHEN upper(unit) IN ('C', 'DEGC', 'WMOUNIT:DEGC')
                      THEN (value * 9.0 / 5.0) + 32.0
                    ELSE value
                  END
                ) AS observed_low_f
              FROM weather_observations
              WHERE metric = 'temperature'
              GROUP BY station_id, DATE(observed_at)
            )
            SELECT
              f.station_id,
              f.forecast_date::text AS forecast_date,
              f.forecast_low_f,
              o.observed_low_f,
              (o.observed_low_f - f.forecast_low_f) AS forecast_error_f
            FROM forecast_daily f
            JOIN obs_daily o
              ON o.station_id = f.station_id
             AND o.obs_date = f.forecast_date
            ORDER BY f.station_id, f.forecast_date
            """
        )
        rows = cur.fetchall()

    normalized_rows: list[dict[str, Any]] = []
    for row in rows:
        normalized_rows.append(
            {
                "station_id": str(row["station_id"]),
                "forecast_date": str(row["forecast_date"]),
                "forecast_low_f": float(row["forecast_low_f"]),
                "observed_low_f": float(row["observed_low_f"]),
                "forecast_error_f": float(row["forecast_error_f"]),
            }
        )

    output_dir = Path("artifacts") / "features" / run_date
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "low_temp_training_examples.json"
    output_path.write_text(json.dumps(normalized_rows, indent=2), encoding="utf-8")

    station_count = len({row["station_id"] for row in normalized_rows})
    return FeatureBuildSummary(
        examples=len(normalized_rows),
        stations=station_count,
        output_path=str(output_path),
    )


def train_low_temp_model(run_date: str) -> TrainSummary:
    features_path = (
        Path("artifacts")
        / "features"
        / run_date
        / "low_temp_training_examples.json"
    )
    if not features_path.exists():
        raise RuntimeError(f"Features file not found: {features_path}")

    examples: list[dict[str, Any]] = json.loads(features_path.read_text(encoding="utf-8"))
    if not examples:
        raise RuntimeError("No training examples available for low-temp model.")

    station_errors: dict[str, list[float]] = {}
    all_errors: list[float] = []

    for row in examples:
        station = str(row["station_id"])
        err = float(row["forecast_error_f"])
        station_errors.setdefault(station, []).append(err)
        all_errors.append(err)

    global_sigma = _safe_sigma(all_errors)
    station_sigma = {
        station: _safe_sigma(errors, fallback=global_sigma)
        for station, errors in station_errors.items()
    }
    rmse = math.sqrt(sum(err * err for err in all_errors) / max(1, len(all_errors)))

    model = {
        "version": "low-temp-normal-v1",
        "trained_at_utc": datetime.now(timezone.utc).isoformat(),
        "samples": len(all_errors),
        "global_sigma_f": global_sigma,
        "rmse_f": rmse,
        "station_sigma_f": station_sigma,
    }

    out_dir = Path("artifacts") / "models"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "low_temp_model_latest.json"
    out_path.write_text(json.dumps(model, indent=2), encoding="utf-8")

    return TrainSummary(
        samples=len(all_errors),
        stations=len(station_sigma),
        global_sigma_f=global_sigma,
        rmse_f=rmse,
        output_path=str(out_path),
    )


def load_low_temp_model() -> dict[str, Any] | None:
    model_path = Path("artifacts") / "models" / "low_temp_model_latest.json"
    if not model_path.exists():
        return None
    return json.loads(model_path.read_text(encoding="utf-8"))


def _safe_sigma(values: list[float], fallback: float = 3.5) -> float:
    if not values:
        return max(1.5, fallback)
    mean = sum(values) / len(values)
    variance = sum((v - mean) ** 2 for v in values) / max(1, len(values))
    sigma = math.sqrt(variance)
    return max(1.5, sigma)
