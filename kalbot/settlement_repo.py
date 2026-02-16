from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any
from urllib.request import Request, urlopen

from psycopg import errors

from kalbot.db import get_connection
from kalbot.settings import Settings, get_settings


class SettlementRepositoryError(RuntimeError):
    pass


@dataclass
class SettlementReconcileSummary:
    checked_markets: int = 0
    settled_markets: int = 0
    closed_positions: int = 0
    metrics_days_written: int = 0
    fetch_failures: int = 0


def reconcile_settlements(run_date: date, settings: Settings | None = None) -> str:
    cfg = settings or get_settings()
    summary = SettlementReconcileSummary()
    metric_dates: set[date] = set()

    try:
        with get_connection() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT m.id, m.market_ticker
                FROM markets m
                LEFT JOIN settlements s ON s.market_id = m.id
                WHERE s.market_id IS NULL
                  AND m.market_ticker LIKE 'KXLOWT%%'
                  AND (
                    m.close_time <= NOW()
                    OR m.settle_time <= NOW()
                    OR EXISTS (SELECT 1 FROM predictions p WHERE p.market_id = m.id)
                    OR EXISTS (SELECT 1 FROM positions pos WHERE pos.market_id = m.id)
                  )
                ORDER BY COALESCE(m.settle_time, m.close_time) ASC NULLS FIRST
                LIMIT 400
                """
            )
            candidates = cur.fetchall()

            for market in candidates:
                summary.checked_markets += 1
                try:
                    payload = _fetch_market_payload(
                        api_base=cfg.kalshi_api_base,
                        market_ticker=str(market["market_ticker"]),
                        timeout_seconds=max(5, cfg.bot_intel_feed_timeout_seconds),
                    )
                except Exception:
                    summary.fetch_failures += 1
                    continue
                settled_yes = _market_result_to_bool(payload.get("result"))
                if settled_yes is None:
                    continue
                status = str(payload.get("status") or "").strip().lower()
                if status not in {"settled", "finalized", "determined"}:
                    continue

                settled_at = _market_settled_at(payload)
                metric_dates.add(settled_at.date())
                cur.execute(
                    """
                    INSERT INTO settlements (market_id, settled_yes, settled_at, created_at)
                    VALUES (%s, %s, %s, NOW())
                    ON CONFLICT (market_id)
                    DO UPDATE SET
                      settled_yes = EXCLUDED.settled_yes,
                      settled_at = EXCLUDED.settled_at
                    """,
                    (int(market["id"]), settled_yes, settled_at),
                )
                summary.settled_markets += int(cur.rowcount or 0)

                cur.execute(
                    """
                    UPDATE markets
                    SET settle_time = %s, updated_at = NOW()
                    WHERE id = %s
                    """,
                    (settled_at, int(market["id"])),
                )

            summary.closed_positions = _close_open_positions_for_settlements(
                cur=cur, execution_mode=cfg.execution_mode
            )

            for metric_date in sorted(metric_dates):
                _upsert_daily_metrics(cur=cur, metric_date=metric_date, execution_mode=cfg.execution_mode)
                summary.metrics_days_written += 1

            # Guarantee a row for current run day when closes happen today in execution mode.
            if summary.closed_positions > 0 and run_date not in metric_dates:
                _upsert_daily_metrics(cur=cur, metric_date=run_date, execution_mode=cfg.execution_mode)
                summary.metrics_days_written += 1

    except errors.UndefinedTable as exc:
        raise SettlementRepositoryError(
            "Settlement/metrics tables missing. Apply infra/migrations/001_initial_schema.sql."
        ) from exc
    except Exception as exc:
        raise SettlementRepositoryError(f"Failed settlement reconcile: {exc}") from exc

    return (
        "Settlement reconcile complete: "
        f"checked={summary.checked_markets}, settled={summary.settled_markets}, "
        f"closed_positions={summary.closed_positions}, metric_days={summary.metrics_days_written}, "
        f"fetch_failures={summary.fetch_failures}."
    )


def _fetch_market_payload(
    api_base: str, market_ticker: str, timeout_seconds: int
) -> dict[str, Any]:
    url = f"{api_base.rstrip('/')}/markets/{market_ticker}"
    req = Request(url=url, headers={"Accept": "application/json"}, method="GET")
    with urlopen(req, timeout=timeout_seconds) as response:
        raw = response.read().decode("utf-8")
    payload = json.loads(raw)
    market = payload.get("market")
    if not isinstance(market, dict):
        raise SettlementRepositoryError(
            f"Kalshi market payload missing object for ticker {market_ticker}."
        )
    return market


def _market_result_to_bool(raw_result: Any) -> bool | None:
    text = str(raw_result or "").strip().lower()
    if text == "yes":
        return True
    if text == "no":
        return False
    return None


def _market_settled_at(payload: dict[str, Any]) -> datetime:
    settlement_ts = payload.get("settlement_ts")
    if settlement_ts:
        parsed = _parse_time(settlement_ts)
        if parsed is not None:
            return parsed

    expiration_time = payload.get("expiration_time")
    if expiration_time:
        parsed = _parse_time(expiration_time)
        if parsed is not None:
            return parsed

    close_time = payload.get("close_time")
    parsed_close = _parse_time(close_time)
    if parsed_close is not None:
        return parsed_close
    return datetime.now(timezone.utc)


def _parse_time(raw: Any) -> datetime | None:
    if raw in (None, ""):
        return None
    text = str(raw).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _close_open_positions_for_settlements(cur: Any, execution_mode: str) -> int:
    cur.execute(
        """
        UPDATE positions p
        SET
          status = 'closed',
          closed_at = s.settled_at,
          realized_pnl = (
            CASE
              WHEN p.side = 'yes' AND s.settled_yes = TRUE THEN (1.0 - p.entry_price) * p.contracts
              WHEN p.side = 'yes' AND s.settled_yes = FALSE THEN (0.0 - p.entry_price) * p.contracts
              WHEN p.side = 'no' AND s.settled_yes = FALSE THEN (1.0 - p.entry_price) * p.contracts
              WHEN p.side = 'no' AND s.settled_yes = TRUE THEN (0.0 - p.entry_price) * p.contracts
              ELSE 0.0
            END
          )
        FROM settlements s
        WHERE p.market_id = s.market_id
          AND p.execution_mode = %s
          AND p.status = 'open'
        """,
        (execution_mode,),
    )
    return int(cur.rowcount or 0)


def _upsert_daily_metrics(cur: Any, metric_date: date, execution_mode: str) -> None:
    cur.execute(
        """
        WITH settled_markets AS (
          SELECT
            s.market_id,
            s.settled_at,
            CASE WHEN s.settled_yes THEN 1.0 ELSE 0.0 END AS outcome
          FROM settlements s
          WHERE s.settled_at::date = %s
        ),
        scored AS (
          SELECT
            sm.market_id,
            sm.outcome,
            LEAST(GREATEST(pred.prob_yes::float8, 0.000001), 0.999999) AS prob_yes
          FROM settled_markets sm
          JOIN LATERAL (
            SELECT p.prob_yes, p.predicted_at
            FROM predictions p
            WHERE p.market_id = sm.market_id
              AND p.predicted_at <= sm.settled_at
            ORDER BY p.predicted_at DESC
            LIMIT 1
          ) pred ON TRUE
        )
        SELECT
          COUNT(*) AS scored_count,
          AVG(POWER(scored.prob_yes - scored.outcome, 2.0))::float8 AS brier_score,
          AVG(
            -(
              (scored.outcome * LN(scored.prob_yes))
              + ((1.0 - scored.outcome) * LN(1.0 - scored.prob_yes))
            )
          )::float8 AS log_loss,
          AVG(ABS(scored.prob_yes - scored.outcome))::float8 AS calibration_error
        FROM scored
        """,
        (metric_date,),
    )
    score_row = cur.fetchone()

    cur.execute(
        """
        SELECT COALESCE(SUM(p.realized_pnl), 0)::float8 AS gross_pnl
        FROM positions p
        WHERE p.execution_mode = %s
          AND p.status = 'closed'
          AND p.closed_at::date = %s
        """,
        (execution_mode, metric_date),
    )
    pnl_row = cur.fetchone()
    gross_pnl = float(pnl_row["gross_pnl"] or 0.0)

    cur.execute(
        """
        WITH daily AS (
          SELECT
            p.closed_at::date AS day,
            SUM(p.realized_pnl)::float8 AS pnl
          FROM positions p
          WHERE p.execution_mode = %s
            AND p.status = 'closed'
            AND p.closed_at::date <= %s
          GROUP BY p.closed_at::date
        ),
        equity AS (
          SELECT day, SUM(pnl) OVER (ORDER BY day) AS equity
          FROM daily
        ),
        drawdown AS (
          SELECT
            day,
            (MAX(equity) OVER (ORDER BY day) - equity) AS dd
          FROM equity
        )
        SELECT COALESCE(MAX(dd), 0)::float8 AS max_drawdown
        FROM drawdown
        """,
        (execution_mode, metric_date),
    )
    dd_row = cur.fetchone()
    max_drawdown = float(dd_row["max_drawdown"] or 0.0)

    scored_count = int(score_row["scored_count"] or 0)
    brier_score = _none_if_nan(score_row["brier_score"]) if scored_count > 0 else None
    log_loss = _none_if_nan(score_row["log_loss"]) if scored_count > 0 else None
    calibration_error = _none_if_nan(score_row["calibration_error"]) if scored_count > 0 else None

    cur.execute(
        """
        INSERT INTO daily_metrics (
          metric_date,
          execution_mode,
          brier_score,
          log_loss,
          calibration_error,
          gross_pnl,
          net_pnl,
          max_drawdown,
          created_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (metric_date, execution_mode)
        DO UPDATE SET
          brier_score = EXCLUDED.brier_score,
          log_loss = EXCLUDED.log_loss,
          calibration_error = EXCLUDED.calibration_error,
          gross_pnl = EXCLUDED.gross_pnl,
          net_pnl = EXCLUDED.net_pnl,
          max_drawdown = EXCLUDED.max_drawdown
        """,
        (
            metric_date,
            execution_mode,
            brier_score,
            log_loss,
            calibration_error,
            gross_pnl,
            gross_pnl,
            max_drawdown,
        ),
    )


def _none_if_nan(value: Any) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(parsed):
        return None
    return parsed
