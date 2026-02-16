from __future__ import annotations

import json
import math
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from kalbot.db import get_connection
from kalbot.schemas import BacktestSummary


class BacktestRepositoryError(RuntimeError):
    pass


@dataclass
class BacktestRow:
    market_ticker: str
    settled_at: datetime
    outcome_yes: float
    model_prob_yes: float
    market_prob_yes: float
    model_brier: float
    market_brier: float
    model_log_loss: float
    market_log_loss: float


def get_backtest_summary(days: int = 60) -> BacktestSummary:
    rows = _load_backtest_rows(days=days)
    if not rows:
        return BacktestSummary(
            window_days=days,
            settled_samples=0,
            model_brier=None,
            market_brier=None,
            model_log_loss=None,
            market_log_loss=None,
            brier_edge=None,
            log_loss_edge=None,
            updated_at_utc=datetime.now(timezone.utc),
        )

    model_brier = sum(row.model_brier for row in rows) / len(rows)
    market_brier = sum(row.market_brier for row in rows) / len(rows)
    model_log_loss = sum(row.model_log_loss for row in rows) / len(rows)
    market_log_loss = sum(row.market_log_loss for row in rows) / len(rows)

    return BacktestSummary(
        window_days=days,
        settled_samples=len(rows),
        model_brier=model_brier,
        market_brier=market_brier,
        model_log_loss=model_log_loss,
        market_log_loss=market_log_loss,
        brier_edge=market_brier - model_brier,
        log_loss_edge=market_log_loss - model_log_loss,
        updated_at_utc=datetime.now(timezone.utc),
    )


def write_backtest_report(run_date: date, days: int = 60) -> str:
    rows = _load_backtest_rows(days=days)
    summary = get_backtest_summary(days=days)
    payload = {
        "summary": summary.model_dump(),
        "rows": [asdict(row) for row in rows[:300]],
    }
    out_dir = Path("artifacts") / "backtests" / run_date.isoformat()
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "model_vs_market.json"
    out_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    return (
        f"Backtest complete: samples={summary.settled_samples}, "
        f"brier_edge={'n/a' if summary.brier_edge is None else f'{summary.brier_edge:.4f}'}, "
        f"path={out_path}."
    )


def _load_backtest_rows(days: int) -> list[BacktestRow]:
    since_date = date.today() - timedelta(days=max(days - 1, 0))
    query = """
        WITH latest_prediction AS (
          SELECT
            s.market_id,
            s.settled_at,
            s.settled_yes,
            p.prob_yes::float8 AS model_prob_yes,
            p.predicted_at,
            ROW_NUMBER() OVER (
              PARTITION BY s.market_id
              ORDER BY p.predicted_at DESC
            ) AS rn
          FROM settlements s
          JOIN predictions p
            ON p.market_id = s.market_id
           AND p.predicted_at <= s.settled_at
          WHERE s.settled_at::date >= %s
        )
        SELECT
          m.market_ticker,
          lp.settled_at,
          lp.settled_yes,
          lp.model_prob_yes,
          COALESCE(
            snap.market_prob_yes,
            lp.model_prob_yes
          ) AS market_prob_yes
        FROM latest_prediction lp
        JOIN markets m ON m.id = lp.market_id
        LEFT JOIN LATERAL (
          SELECT
            COALESCE(
              CASE
                WHEN ms.bid_yes IS NOT NULL
                 AND ms.ask_yes IS NOT NULL
                 AND ms.bid_yes > 0
                 AND ms.ask_yes > 0
                THEN ((ms.bid_yes + ms.ask_yes) / 2.0)
              END,
              ms.last_price_yes
            )::float8 AS market_prob_yes
          FROM market_snapshots ms
          WHERE ms.market_id = lp.market_id
            AND ms.captured_at <= lp.predicted_at
          ORDER BY ms.captured_at DESC
          LIMIT 1
        ) snap ON TRUE
        WHERE lp.rn = 1
        ORDER BY lp.settled_at DESC
    """
    try:
        with get_connection() as conn, conn.cursor() as cur:
            cur.execute(query, (since_date,))
            db_rows = cur.fetchall()
    except Exception as exc:
        raise BacktestRepositoryError(f"Failed to load backtest rows: {exc}") from exc

    rows: list[BacktestRow] = []
    for row in db_rows:
        outcome = 1.0 if bool(row["settled_yes"]) else 0.0
        model_prob = _clip(float(row["model_prob_yes"]))
        market_prob = _clip(float(row["market_prob_yes"]))
        rows.append(
            BacktestRow(
                market_ticker=str(row["market_ticker"]),
                settled_at=row["settled_at"],
                outcome_yes=outcome,
                model_prob_yes=model_prob,
                market_prob_yes=market_prob,
                model_brier=_brier(model_prob, outcome),
                market_brier=_brier(market_prob, outcome),
                model_log_loss=_log_loss(model_prob, outcome),
                market_log_loss=_log_loss(market_prob, outcome),
            )
        )

    return rows


def _clip(value: float) -> float:
    return max(0.000001, min(0.999999, value))


def _brier(prob_yes: float, outcome_yes: float) -> float:
    return (prob_yes - outcome_yes) ** 2


def _log_loss(prob_yes: float, outcome_yes: float) -> float:
    if outcome_yes >= 0.5:
        return -math.log(prob_yes)
    return -math.log(1.0 - prob_yes)
