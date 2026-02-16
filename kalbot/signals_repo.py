from __future__ import annotations

from datetime import date, datetime, timezone

from psycopg import errors

from kalbot.db import get_connection
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
