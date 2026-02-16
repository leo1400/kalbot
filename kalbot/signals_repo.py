from __future__ import annotations

import math
import re
from datetime import date, datetime, timezone

from psycopg import errors

from kalbot.db import get_connection
from kalbot.modeling.low_temp_model import load_low_temp_model
from kalbot.schemas import SignalCard
from kalbot.settings import Settings, get_settings


class SignalRepositoryError(RuntimeError):
    pass


def list_current_signals(limit: int = 20) -> list[SignalCard]:
    query = """
        WITH latest_signals AS (
          SELECT DISTINCT ON (ps.market_id)
            ps.market_id,
            ps.model_run_id,
            ps.confidence,
            ps.rationale,
            ps.data_source_url,
            ps.published_at
          FROM published_signals ps
          WHERE ps.is_active = TRUE
          ORDER BY ps.market_id, ps.published_at DESC
        )
        SELECT
          m.market_ticker,
          m.title,
          p.prob_yes AS probability_yes,
          COALESCE(ms.last_price_yes, p.prob_yes) AS market_implied_yes,
          p.prob_yes - COALESCE(ms.last_price_yes, p.prob_yes) AS edge,
          ls.confidence,
          ls.rationale,
          ls.data_source_url
        FROM latest_signals ls
        JOIN markets m ON m.id = ls.market_id
        JOIN predictions p
          ON p.market_id = ls.market_id
         AND p.model_run_id = ls.model_run_id
        LEFT JOIN LATERAL (
          SELECT last_price_yes
          FROM market_snapshots ms
          WHERE ms.market_id = m.id
          ORDER BY ms.captured_at DESC
          LIMIT 1
        ) ms ON TRUE
        ORDER BY ls.published_at DESC
        LIMIT %s
    """
    try:
        with get_connection() as conn, conn.cursor() as cur:
            cur.execute(query, (limit,))
            rows = cur.fetchall()
    except Exception as exc:
        raise SignalRepositoryError(f"Failed to query current signals: {exc}") from exc

    results: list[SignalCard] = []
    for row in rows:
        results.append(
            SignalCard(
                market_ticker=row["market_ticker"],
                title=row["title"],
                probability_yes=float(row["probability_yes"]),
                market_implied_yes=float(row["market_implied_yes"]),
                edge=float(row["edge"]),
                confidence=float(row["confidence"]),
                rationale=row["rationale"],
                data_source_url=row["data_source_url"],
            )
        )
    return results


def publish_best_signal_for_date(run_date: date) -> str:
    try:
        return publish_live_low_temp_signal(run_date)
    except SignalRepositoryError:
        return publish_demo_signal_for_date(run_date)


def publish_live_low_temp_signal(run_date: date) -> str:
    settings: Settings = get_settings()
    now = datetime.now(timezone.utc)

    try:
        with get_connection() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  m.id,
                  m.market_ticker,
                  m.title,
                  m.close_time,
                  COALESCE(ms.last_price_yes, 0.50) AS market_implied_yes,
                  COALESCE(ms.volume, 0) AS market_volume
                FROM markets m
                LEFT JOIN LATERAL (
                  SELECT last_price_yes, volume
                  FROM market_snapshots ms
                  WHERE ms.market_id = m.id
                  ORDER BY ms.captured_at DESC
                  LIMIT 1
                ) ms ON TRUE
                WHERE m.market_ticker LIKE 'KXLOWT%-26%'
                  AND (m.close_time IS NULL OR m.close_time > NOW())
                ORDER BY m.close_time ASC NULLS LAST, COALESCE(ms.volume, 0) DESC
                LIMIT 1
                """
            )
            market = cur.fetchone()
            if not market:
                raise SignalRepositoryError("No live KXLOWT market available.")

            condition = _parse_low_temp_condition(str(market["title"]))
            if condition is None:
                threshold = _extract_temperature_threshold(market["market_ticker"])
                if threshold is None:
                    raise SignalRepositoryError("Could not parse low-temp condition.")
                condition = {"kind": "gt", "low": threshold, "high": None}
            city_code = _extract_low_temp_city_code(market["market_ticker"])
            if not city_code:
                raise SignalRepositoryError("Could not parse city code from market ticker.")

            cur.execute(
                """
                SELECT station_id, value, unit, valid_at
                FROM weather_forecasts
                WHERE station_id = ANY(%s)
                  AND metric = 'temperature'
                  AND valid_at >= NOW() - INTERVAL '1 hour'
                  AND (%s IS NULL OR valid_at <= %s)
                ORDER BY valid_at ASC
                """,
                (_station_candidates(city_code), market["close_time"], market["close_time"]),
            )
            rows = cur.fetchall()
            market_implied_yes = float(market["market_implied_yes"])
            forecast_temps_f = [_to_fahrenheit(float(r["value"]), str(r["unit"])) for r in rows]
            projected_low_f = min(forecast_temps_f) if forecast_temps_f else None

            trained_model = load_low_temp_model()
            model_version = (
                str(trained_model.get("version"))
                if trained_model and trained_model.get("version")
                else "low-temp-heuristic-fallback"
            )
            station_id = str(rows[0]["station_id"]) if rows else _station_candidates(city_code)[0]
            sigma_f = _resolve_sigma_f(trained_model, station_id)

            if projected_low_f is not None:
                model_prob_yes = _condition_probability(
                    condition=condition, mu_f=projected_low_f, sigma_f=sigma_f
                )
                model_prob_yes = _clamp(model_prob_yes, 0.01, 0.99)
                sample_bonus = (
                    min(0.1, float(trained_model.get("samples", 0)) / 500.0)
                    if trained_model
                    else 0.0
                )
                confidence = _clamp(
                    0.60 + min(0.25, abs(model_prob_yes - market_implied_yes) * 1.5) + sample_bonus,
                    0.55,
                    0.97,
                )
            else:
                model_prob_yes = market_implied_yes
                confidence = 0.55

            spread = _clamp(min(0.2, sigma_f / 20.0), 0.05, 0.20)
            ci_low = _clamp(model_prob_yes - spread, 0.01, 0.99)
            ci_high = _clamp(model_prob_yes + spread, 0.01, 0.99)
            edge = model_prob_yes - market_implied_yes

            cur.execute(
                """
                INSERT INTO model_runs (
                  model_name, run_type, training_start, training_end,
                  validation_score, calibration_error, metadata
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
                RETURNING id
                """,
                (
                    f"{settings.model_name}:{model_version}",
                    "trained_low_temp",
                    now,
                    now,
                    None,
                    None,
                    (
                        "{"
                        f"\"condition\": \"{condition['kind']}\", "
                        f"\"condition_low_f\": {condition['low']}, "
                        f"\"condition_high_f\": {('null' if condition['high'] is None else condition['high'])}, "
                        f"\"projected_low_f\": {('null' if projected_low_f is None else f'{projected_low_f:.2f}')}, "
                        f"\"sigma_f\": {sigma_f:.3f}, "
                        f"\"station_id\": \"{station_id}\", "
                        f"\"market_implied_yes\": {market_implied_yes:.4f}"
                        "}"
                    ),
                ),
            )
            model_run_id = cur.fetchone()["id"]

            cur.execute(
                """
                INSERT INTO predictions (
                  model_run_id, market_id, prob_yes, ci_low, ci_high, predicted_at
                )
                VALUES (%s, %s, %s, %s, %s, NOW())
                RETURNING id
                """,
                (model_run_id, market["id"], model_prob_yes, ci_low, ci_high),
            )
            prediction_id = cur.fetchone()["id"]

            approved = edge >= 0.03
            reason = (
                f"Low-temp model edge={edge:.3f} "
                f"(city={city_code}, condition={condition['kind']}, "
                f"projected_low={'n/a' if projected_low_f is None else f'{projected_low_f:.1f}F'})"
            )
            cur.execute(
                """
                INSERT INTO trade_decisions (
                  prediction_id, edge, threshold, approved, reason, created_at
                )
                VALUES (%s, %s, %s, %s, %s, NOW())
                """,
                (prediction_id, edge, 0.03, approved, reason),
            )

            if projected_low_f is not None:
                rationale = (
                    f"Kalbot live low-temp model ({model_version}): {city_code} market condition "
                    f"{_condition_label(condition)}. NWS projects low around {projected_low_f:.1f}F "
                    f"(sigma={sigma_f:.1f}F). "
                    f"Model YES={model_prob_yes:.1%} vs market YES={market_implied_yes:.1%}."
                )
            else:
                rationale = (
                    f"Kalbot market-only fallback for {city_code} {_condition_label(condition)}. "
                    f"No matching weather forecast rows found, so model mirrors market "
                    f"YES={market_implied_yes:.1%}."
                )
            cur.execute(
                """
                INSERT INTO published_signals (
                  market_id, model_run_id, confidence, rationale, data_source_url, is_active, published_at
                )
                VALUES (%s, %s, %s, %s, %s, TRUE, NOW())
                """,
                (
                    market["id"],
                    model_run_id,
                    confidence,
                    rationale,
                    "https://api.weather.gov/",
                ),
            )

            cur.execute(
                """
                UPDATE published_signals
                SET is_active = FALSE
                WHERE market_id = %s
                  AND model_run_id <> %s
                  AND is_active = TRUE
                """,
                (market["id"], model_run_id),
            )

            # Deactivate old synthetic/demo signals once a live Kalshi-backed signal exists.
            cur.execute(
                """
                UPDATE published_signals ps
                SET is_active = FALSE
                FROM markets m
                WHERE ps.market_id = m.id
                  AND m.kalshi_market_id LIKE 'WEATHER-%'
                  AND ps.is_active = TRUE
                """
            )

    except errors.UndefinedTable as exc:
        raise SignalRepositoryError(
            "Schema not found. Apply infra/migrations/001_initial_schema.sql first."
        ) from exc
    except SignalRepositoryError:
        raise
    except Exception as exc:
        raise SignalRepositoryError(f"Failed to publish live low-temp signal: {exc}") from exc

    return (
        "Published live low-temp signal for "
        f"{market['market_ticker']} (city={city_code}, condition={condition['kind']}, "
        f"projected_low={'n/a' if projected_low_f is None else f'{projected_low_f:.1f}F'})."
    )


def publish_demo_signal_for_date(run_date: date) -> str:
    settings: Settings = get_settings()

    market_ticker = f"WEATHER-NYC-{run_date.isoformat()}-HIGH-GT-45F"
    event_ticker = f"WEATHER-NYC-{run_date.isoformat()}"
    title = f"NYC high temperature above 45F on {run_date.isoformat()}?"
    now = datetime.now(timezone.utc)

    try:
        with get_connection() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO markets (
                  kalshi_market_id, event_ticker, market_ticker, title, close_time, settle_time
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (kalshi_market_id)
                DO UPDATE SET updated_at = NOW()
                RETURNING id
                """,
                (market_ticker, event_ticker, market_ticker, title, now, now),
            )
            market_id = cur.fetchone()["id"]

            cur.execute(
                """
                INSERT INTO market_snapshots (
                  market_id, bid_yes, ask_yes, last_price_yes, volume, captured_at
                )
                VALUES (%s, %s, %s, %s, %s, NOW())
                """,
                (market_id, 0.53, 0.55, 0.54, 1200),
            )

            cur.execute(
                """
                INSERT INTO model_runs (
                  model_name, run_type, training_start, training_end,
                  validation_score, calibration_error, metadata
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
                RETURNING id
                """,
                (
                    settings.model_name,
                    "daily",
                    now,
                    now,
                    0.182,
                    0.031,
                    '{"source":"demo-seed","note":"replace with real trainer output"}',
                ),
            )
            model_run_id = cur.fetchone()["id"]

            cur.execute(
                """
                INSERT INTO predictions (
                  model_run_id, market_id, prob_yes, ci_low, ci_high, predicted_at
                )
                VALUES (%s, %s, %s, %s, %s, NOW())
                RETURNING id
                """,
                (model_run_id, market_id, 0.61, 0.55, 0.67),
            )
            prediction_id = cur.fetchone()["id"]

            cur.execute(
                """
                INSERT INTO trade_decisions (
                  prediction_id, edge, threshold, approved, reason, created_at
                )
                VALUES (%s, %s, %s, %s, %s, NOW())
                """,
                (
                    prediction_id,
                    0.07,
                    0.03,
                    True,
                    "Demo edge exceeds paper threshold. Replace with model-driven rules.",
                ),
            )

            cur.execute(
                """
                INSERT INTO published_signals (
                  market_id, model_run_id, confidence, rationale, data_source_url, is_active, published_at
                )
                VALUES (%s, %s, %s, %s, %s, TRUE, NOW())
                """,
                (
                    market_id,
                    model_run_id,
                    0.69,
                    "Demo published signal seeded by daily worker. Replace with real features and model output.",
                    "https://www.weather.gov/",
                ),
            )
            # Keep only the latest published row active for this market.
            cur.execute(
                """
                UPDATE published_signals
                SET is_active = FALSE
                WHERE market_id = %s
                  AND model_run_id <> %s
                  AND is_active = TRUE
                """,
                (market_id, model_run_id),
            )
    except errors.UndefinedTable as exc:
        raise SignalRepositoryError(
            "Schema not found. Apply infra/migrations/001_initial_schema.sql first."
        ) from exc
    except Exception as exc:
        raise SignalRepositoryError(f"Failed to publish demo signal: {exc}") from exc

    return f"Published demo signal for {market_ticker}."


def _extract_temperature_threshold(market_ticker: str) -> float | None:
    match = re.search(r"-T(\d+(?:\.\d+)?)$", market_ticker)
    if not match:
        return None
    return float(match.group(1))


def _parse_low_temp_condition(title: str) -> dict[str, float | str | None] | None:
    range_match = re.search(r"(\d+(?:\.\d+)?)\s*-\s*(\d+(?:\.\d+)?)°", title)
    if range_match:
        low = float(range_match.group(1))
        high = float(range_match.group(2))
        return {"kind": "range", "low": low, "high": high}

    lt_match = re.search(r"<\s*(\d+(?:\.\d+)?)°", title)
    if lt_match:
        return {"kind": "lt", "low": float(lt_match.group(1)), "high": None}

    gt_match = re.search(r">\s*(\d+(?:\.\d+)?)°", title)
    if gt_match:
        return {"kind": "gt", "low": float(gt_match.group(1)), "high": None}

    return None


def _extract_low_temp_city_code(market_ticker: str) -> str | None:
    match = re.search(r"^KXLOWT([A-Z]+)-", market_ticker)
    if not match:
        return None
    return match.group(1)


def _station_candidates(city_code: str) -> list[str]:
    base = city_code.upper()
    return [f"K{base}", base]


def _to_fahrenheit(value: float, unit: str) -> float:
    upper = unit.upper()
    if upper in {"F", "DEGF", "WMOUNIT:DEGF"}:
        return value
    if upper in {"C", "DEGC", "WMOUNIT:DEGC"}:
        return (value * 9.0 / 5.0) + 32.0
    return value


def _condition_probability(
    condition: dict[str, float | str | None], mu_f: float, sigma_f: float
) -> float:
    kind = str(condition["kind"])
    low = float(condition["low"])
    if kind == "lt":
        return _normal_cdf(low, mu_f, sigma_f)
    if kind == "gt":
        return 1.0 - _normal_cdf(low, mu_f, sigma_f)
    if kind == "range":
        high = float(condition["high"])
        lo = min(low, high)
        hi = max(low, high)
        return _normal_cdf(hi, mu_f, sigma_f) - _normal_cdf(lo, mu_f, sigma_f)
    return 0.5


def _normal_cdf(x: float, mu: float, sigma: float) -> float:
    sigma = max(1e-6, sigma)
    z = (x - mu) / (sigma * math.sqrt(2.0))
    return 0.5 * (1.0 + math.erf(z))


def _resolve_sigma_f(model: dict | None, station_id: str) -> float:
    if not model:
        return 3.5
    station_sigma = model.get("station_sigma_f", {})
    if isinstance(station_sigma, dict) and station_id in station_sigma:
        return max(1.5, float(station_sigma[station_id]))
    return max(1.5, float(model.get("global_sigma_f", 3.5)))


def _condition_label(condition: dict[str, float | str | None]) -> str:
    kind = str(condition["kind"])
    low = float(condition["low"])
    if kind == "lt":
        return f"< {low}F"
    if kind == "gt":
        return f"> {low}F"
    if kind == "range":
        high = float(condition["high"])
        return f"{low}-{high}F"
    return "unknown"


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))
