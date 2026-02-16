from __future__ import annotations

import json
import math
import re
from datetime import date, datetime, timezone

from psycopg import errors

from kalbot.db import get_connection
from kalbot.modeling.low_temp_model import load_low_temp_model
from kalbot.schemas import DashboardSummary, PlaybookSignal, SignalCard
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
          COALESCE(
            CASE
              WHEN ms.bid_yes IS NOT NULL
               AND ms.ask_yes IS NOT NULL
               AND ms.bid_yes > 0
               AND ms.ask_yes > 0
              THEN (ms.bid_yes + ms.ask_yes) / 2.0
            END,
            ms.last_price_yes,
            p.prob_yes
          ) AS market_implied_yes,
          p.prob_yes - COALESCE(
            CASE
              WHEN ms.bid_yes IS NOT NULL
               AND ms.ask_yes IS NOT NULL
               AND ms.bid_yes > 0
               AND ms.ask_yes > 0
              THEN (ms.bid_yes + ms.ask_yes) / 2.0
            END,
            ms.last_price_yes,
            p.prob_yes
          ) AS edge,
          ls.confidence,
          ls.rationale,
          ls.data_source_url
        FROM latest_signals ls
        JOIN markets m ON m.id = ls.market_id
        JOIN predictions p
          ON p.market_id = ls.market_id
         AND p.model_run_id = ls.model_run_id
        LEFT JOIN LATERAL (
          SELECT bid_yes, ask_yes, last_price_yes
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

    return [
        SignalCard(
            market_ticker=row["market_ticker"],
            title=row["title"],
            city_code=_extract_low_temp_city_code(row["market_ticker"]),
            city_name=_city_name_from_code(_extract_low_temp_city_code(row["market_ticker"])),
            probability_yes=float(row["probability_yes"]),
            market_implied_yes=float(row["market_implied_yes"]),
            edge=float(row["edge"]),
            confidence=float(row["confidence"]),
            rationale=row["rationale"],
            data_source_url=row["data_source_url"],
        )
        for row in rows
    ]


def list_signal_playbook(limit: int = 6) -> list[PlaybookSignal]:
    settings = get_settings()
    signals = list_current_signals(limit=limit)
    playbook: list[PlaybookSignal] = []

    for signal in signals:
        action = _derive_playbook_action(
            edge=signal.edge,
            confidence=signal.confidence,
            edge_threshold=settings.paper_edge_threshold,
        )
        entry_price = _playbook_entry_price(action=action, market_yes=signal.market_implied_yes)
        suggested_notional = _suggested_notional(
            action=action,
            confidence=signal.confidence,
            edge=signal.edge,
            max_notional=settings.max_notional_per_signal_usd,
            edge_threshold=settings.paper_edge_threshold,
        )
        suggested_contracts = _contracts_for_notional(
            entry_price=entry_price,
            max_notional=suggested_notional,
            max_contracts=settings.max_contracts_per_order,
        )
        note = _playbook_note(action=action, edge=signal.edge, confidence=signal.confidence)

        playbook.append(
            PlaybookSignal(
                market_ticker=signal.market_ticker,
                title=signal.title,
                city_code=signal.city_code,
                city_name=signal.city_name,
                action=action,
                edge=signal.edge,
                confidence=signal.confidence,
                probability_yes=signal.probability_yes,
                market_implied_yes=signal.market_implied_yes,
                suggested_contracts=suggested_contracts,
                suggested_notional_usd=round(suggested_notional, 2),
                entry_price=round(entry_price, 4),
                note=note,
            )
        )

    return playbook


def get_dashboard_summary() -> DashboardSummary:
    query = """
        WITH latest_signals AS (
          SELECT DISTINCT ON (ps.market_id)
            ps.market_id,
            ps.model_run_id,
            ps.confidence,
            ps.published_at
          FROM published_signals ps
          WHERE ps.is_active = TRUE
          ORDER BY ps.market_id, ps.published_at DESC
        )
        SELECT
          COUNT(*) AS active_signal_count,
          COALESCE(AVG(ls.confidence), 0)::float8 AS avg_confidence,
          COALESCE(AVG(p.prob_yes - COALESCE(ms.market_yes, p.prob_yes)), 0)::float8 AS avg_edge,
          COALESCE(MAX(ABS(p.prob_yes - COALESCE(ms.market_yes, p.prob_yes))), 0)::float8 AS strongest_edge,
          COALESCE(MAX(ls.published_at), NOW()) AS updated_at_utc
        FROM latest_signals ls
        JOIN predictions p
          ON p.market_id = ls.market_id
         AND p.model_run_id = ls.model_run_id
        LEFT JOIN LATERAL (
          SELECT COALESCE(
            CASE
              WHEN s.bid_yes IS NOT NULL
               AND s.ask_yes IS NOT NULL
               AND s.bid_yes > 0
               AND s.ask_yes > 0
              THEN (s.bid_yes + s.ask_yes) / 2.0
            END,
            s.last_price_yes
          ) AS market_yes
          FROM market_snapshots s
          WHERE s.market_id = ls.market_id
          ORDER BY s.captured_at DESC
          LIMIT 1
        ) ms ON TRUE
    """

    try:
        with get_connection() as conn, conn.cursor() as cur:
            cur.execute(query)
            row = cur.fetchone()
    except Exception as exc:
        raise SignalRepositoryError(f"Failed to query dashboard summary: {exc}") from exc

    return DashboardSummary(
        active_signal_count=int(row["active_signal_count"]),
        avg_confidence=float(row["avg_confidence"]),
        avg_edge=float(row["avg_edge"]),
        strongest_edge=float(row["strongest_edge"]),
        updated_at_utc=row["updated_at_utc"],
    )


def publish_best_signal_for_date(run_date: date) -> str:
    try:
        return publish_live_low_temp_signals(run_date)
    except SignalRepositoryError:
        return publish_demo_signal_for_date(run_date)


def publish_live_low_temp_signals(run_date: date) -> str:
    settings: Settings = get_settings()
    model = load_low_temp_model()
    model_version = model.get("version") if model else "low-temp-heuristic-fallback"
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
                  COALESCE(
                    CASE
                      WHEN ms.bid_yes IS NOT NULL
                       AND ms.ask_yes IS NOT NULL
                       AND ms.bid_yes > 0
                       AND ms.ask_yes > 0
                      THEN (ms.bid_yes + ms.ask_yes) / 2.0
                    END,
                    ms.last_price_yes,
                    0.50
                  ) AS market_implied_yes,
                  COALESCE(ms.volume, 0) AS market_volume
                FROM markets m
                LEFT JOIN LATERAL (
                  SELECT bid_yes, ask_yes, last_price_yes, volume
                  FROM market_snapshots ms
                  WHERE ms.market_id = m.id
                  ORDER BY ms.captured_at DESC
                  LIMIT 1
                ) ms ON TRUE
                WHERE m.market_ticker LIKE 'KXLOWT%-26%'
                  AND (m.close_time IS NULL OR m.close_time > NOW())
                ORDER BY m.close_time ASC NULLS LAST, COALESCE(ms.volume, 0) DESC
                LIMIT 250
                """
            )
            markets = cur.fetchall()
            if not markets:
                raise SignalRepositoryError("No live KXLOWT markets available.")

            candidates: list[dict] = []
            for market in markets:
                candidate = _evaluate_low_temp_market_candidate(
                    cur=cur,
                    market=market,
                    model=model,
                    model_version=model_version,
                )
                if candidate is not None:
                    candidates.append(candidate)

            if not candidates:
                raise SignalRepositoryError("No valid low-temp signal candidates.")

            forecasted = [c for c in candidates if c["has_forecast"]]
            liquid = [c for c in forecasted if c["market_volume"] >= 10]
            pool = liquid if liquid else (forecasted if forecasted else candidates)
            pool.sort(key=lambda c: c["ranking_score"], reverse=True)
            selected = _select_diversified_signals(
                candidates=pool,
                limit=settings.signal_publish_limit,
                max_per_city=2,
            )

            # Replace active signal set with the fresh ranked selection.
            cur.execute("UPDATE published_signals SET is_active = FALSE WHERE is_active = TRUE")

            published_tickers: list[str] = []
            for signal in selected:
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
                        f"{settings.model_name}:{signal['model_version']}",
                        "trained_low_temp_ranked",
                        now,
                        now,
                        None,
                        None,
                        json.dumps(signal["metadata"]),
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
                    (
                        model_run_id,
                        signal["market_id"],
                        signal["prob_yes"],
                        signal["ci_low"],
                        signal["ci_high"],
                    ),
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
                        signal["edge"],
                        0.03,
                        signal["edge"] >= 0.03,
                        signal["decision_reason"],
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
                        signal["market_id"],
                        model_run_id,
                        signal["confidence"],
                        signal["rationale"],
                        "https://api.weather.gov/",
                    ),
                )
                published_tickers.append(signal["market_ticker"])

    except errors.UndefinedTable as exc:
        raise SignalRepositoryError(
            "Schema not found. Apply infra/migrations/001_initial_schema.sql first."
        ) from exc
    except SignalRepositoryError:
        raise
    except Exception as exc:
        raise SignalRepositoryError(f"Failed to publish live low-temp signals: {exc}") from exc

    return f"Published {len(published_tickers)} live signals: {', '.join(published_tickers)}."


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


def _evaluate_low_temp_market_candidate(
    cur, market: dict, model: dict | None, model_version: str
) -> dict | None:
    market_ticker = str(market["market_ticker"])
    title = str(market["title"])
    condition = _parse_low_temp_condition(title)
    if condition is None:
        threshold = _extract_temperature_threshold(market_ticker)
        if threshold is None:
            return None
        condition = {"kind": "gt", "low": threshold, "high": None}

    city_code = _extract_low_temp_city_code(market_ticker)
    if not city_code:
        return None

    station_candidates = _station_candidates(city_code)
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
        (station_candidates, market["close_time"], market["close_time"]),
    )
    rows = cur.fetchall()

    forecast_temps_f = [_to_fahrenheit(float(r["value"]), str(r["unit"])) for r in rows]
    projected_low_f = min(forecast_temps_f) if forecast_temps_f else None
    market_implied_yes = float(market["market_implied_yes"])
    market_volume = float(market["market_volume"] or 0)

    station_id = str(rows[0]["station_id"]) if rows else station_candidates[0]
    sigma_f = _resolve_sigma_f(model, station_id)

    if projected_low_f is not None:
        prob_yes = _condition_probability(condition, projected_low_f, sigma_f)
        prob_yes = _clamp(prob_yes, 0.01, 0.99)
        sample_bonus = min(0.1, float(model.get("samples", 0)) / 500.0) if model else 0.0
        confidence = _clamp(
            0.60 + min(0.25, abs(prob_yes - market_implied_yes) * 1.5) + sample_bonus,
            0.55,
            0.97,
        )
        rationale = (
            f"Kalbot live low-temp model ({model_version}): {city_code} condition "
            f"{_condition_label(condition)}. NWS projects low ~{projected_low_f:.1f}F "
            f"(sigma={sigma_f:.1f}F). Model YES={prob_yes:.1%} vs market YES={market_implied_yes:.1%}."
        )
    else:
        prob_yes = market_implied_yes
        confidence = 0.55
        rationale = (
            f"Kalbot market-only fallback for {city_code} {_condition_label(condition)}. "
            f"No matching weather forecast rows found, model mirrors market YES={market_implied_yes:.1%}."
        )

    spread = _clamp(min(0.2, sigma_f / 20.0), 0.05, 0.20)
    ci_low = _clamp(prob_yes - spread, 0.01, 0.99)
    ci_high = _clamp(prob_yes + spread, 0.01, 0.99)
    edge = prob_yes - market_implied_yes

    # Rank by edge magnitude + slight preference for forecast-backed and liquid markets.
    has_forecast = projected_low_f is not None
    info_score = 1.0 - min(1.0, abs(market_implied_yes - 0.5) * 2.0)
    ranking_score = (
        abs(edge)
        + (0.2 if has_forecast else 0.0)
        + min(0.05, market_volume / 20000.0)
        + (0.03 * info_score)
    )

    return {
        "market_id": int(market["id"]),
        "market_ticker": market_ticker,
        "city_code": city_code,
        "condition": condition,
        "market_volume": market_volume,
        "prob_yes": prob_yes,
        "market_implied_yes": market_implied_yes,
        "edge": edge,
        "confidence": confidence,
        "ci_low": ci_low,
        "ci_high": ci_high,
        "has_forecast": has_forecast,
        "model_version": model_version,
        "ranking_score": ranking_score,
        "rationale": rationale,
        "decision_reason": (
            f"Ranked low-temp signal edge={edge:.3f}, "
            f"condition={condition['kind']}, city={city_code}, "
            f"projected_low={'n/a' if projected_low_f is None else f'{projected_low_f:.1f}F'}."
        ),
        "metadata": {
            "city_code": city_code,
            "condition": condition["kind"],
            "condition_low_f": condition["low"],
            "condition_high_f": condition["high"],
            "projected_low_f": projected_low_f,
            "sigma_f": sigma_f,
            "station_id": station_id,
            "market_implied_yes": market_implied_yes,
            "market_volume": market_volume,
            "ranking_score": ranking_score,
        },
    }


def _extract_temperature_threshold(market_ticker: str) -> float | None:
    match = re.search(r"-T(\d+(?:\.\d+)?)$", market_ticker)
    if not match:
        return None
    return float(match.group(1))


def _parse_low_temp_condition(title: str) -> dict[str, float | str | None] | None:
    normalized = title.replace("\u00C2\u00B0", "\u00B0")

    range_match = re.search(
        r"(\d+(?:\.\d+)?)\s*-\s*(\d+(?:\.\d+)?)(?:\s*(?:\u00B0|deg)\s*F?)?",
        normalized,
        flags=re.IGNORECASE,
    )
    if range_match:
        low = float(range_match.group(1))
        high = float(range_match.group(2))
        return {"kind": "range", "low": low, "high": high}

    lt_match = re.search(
        r"<\s*(\d+(?:\.\d+)?)(?:\s*(?:\u00B0|deg)\s*F?)?",
        normalized,
        flags=re.IGNORECASE,
    )
    if lt_match:
        return {"kind": "lt", "low": float(lt_match.group(1)), "high": None}

    gt_match = re.search(
        r">\s*(\d+(?:\.\d+)?)(?:\s*(?:\u00B0|deg)\s*F?)?",
        normalized,
        flags=re.IGNORECASE,
    )
    if gt_match:
        return {"kind": "gt", "low": float(gt_match.group(1)), "high": None}

    return None


def _extract_low_temp_city_code(market_ticker: str) -> str | None:
    match = re.search(r"^KXLOWT([A-Z]+)-", market_ticker)
    if not match:
        return None
    return match.group(1)


def _city_name_from_code(city_code: str | None) -> str | None:
    if not city_code:
        return None
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


def _select_diversified_signals(
    candidates: list[dict], limit: int, max_per_city: int = 2
) -> list[dict]:
    if limit <= 0 or not candidates:
        return []

    selected: list[dict] = []
    city_counts: dict[str, int] = {}

    # Pass 1: capture top-ranked unique cities first.
    for candidate in candidates:
        city = str(candidate.get("city_code") or "UNKNOWN")
        if city in city_counts:
            continue
        selected.append(candidate)
        city_counts[city] = 1
        if len(selected) >= limit:
            return selected

    # Pass 2: fill remaining slots with best leftovers, bounded per city.
    for candidate in candidates:
        if candidate in selected:
            continue
        city = str(candidate.get("city_code") or "UNKNOWN")
        if city_counts.get(city, 0) >= max_per_city:
            continue
        selected.append(candidate)
        city_counts[city] = city_counts.get(city, 0) + 1
        if len(selected) >= limit:
            break

    return selected


def _derive_playbook_action(edge: float, confidence: float, edge_threshold: float) -> str:
    if confidence < 0.58:
        return "pass"
    if edge >= edge_threshold:
        return "lean_yes"
    if edge <= -edge_threshold:
        return "lean_no"
    return "pass"


def _playbook_entry_price(action: str, market_yes: float) -> float:
    clamped_yes = _clamp(market_yes, 0.01, 0.99)
    if action == "lean_no":
        return _clamp(1.0 - clamped_yes, 0.01, 0.99)
    return clamped_yes


def _suggested_notional(
    action: str, confidence: float, edge: float, max_notional: float, edge_threshold: float
) -> float:
    if action == "pass":
        return 0.0
    if max_notional <= 0:
        return 0.0

    confidence_weight = _clamp((confidence - 0.55) / 0.40, 0.0, 1.0)
    edge_weight = _clamp(abs(edge) / max(edge_threshold * 2.0, 0.01), 0.0, 1.0)
    sizing_weight = 0.35 + (0.65 * confidence_weight * edge_weight)
    return max_notional * sizing_weight


def _playbook_note(action: str, edge: float, confidence: float) -> str:
    if action == "pass":
        return "Skip for now. Edge/confidence is not strong enough."
    direction = "YES" if action == "lean_yes" else "NO"
    return (
        f"Model leans {direction}. Edge={edge * 100:.1f} pts, "
        f"confidence={confidence * 100:.1f}%."
    )


def _contracts_for_notional(entry_price: float, max_notional: float, max_contracts: int) -> int:
    if entry_price <= 0 or max_notional <= 0 or max_contracts <= 0:
        return 0
    affordable = int(max_notional // entry_price)
    return max(0, min(affordable, max_contracts))


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))
