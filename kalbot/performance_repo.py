from __future__ import annotations

from datetime import date, timedelta

from kalbot.db import get_connection
from kalbot.schemas import (
    AccuracyHistoryPoint,
    AccuracySummary,
    PaperOrderRow,
    PerformanceHistoryPoint,
    PerformanceSummary,
)


class PerformanceRepositoryError(RuntimeError):
    pass


def get_performance_summary(execution_mode: str = "paper") -> PerformanceSummary:
    query = """
        SELECT
          COALESCE((
            SELECT COUNT(*)
            FROM orders o
            WHERE o.execution_mode = %(mode)s
          ), 0) AS total_orders,
          COALESCE((
            SELECT COUNT(*)
            FROM orders o
            WHERE o.execution_mode = %(mode)s
              AND o.created_at >= NOW() - INTERVAL '24 hours'
          ), 0) AS orders_24h,
          COALESCE((
            SELECT COUNT(*)
            FROM trade_decisions td
            WHERE td.approved = TRUE
              AND td.created_at >= NOW() - INTERVAL '24 hours'
          ), 0) AS approved_decisions_24h,
          COALESCE((
            SELECT COUNT(*)
            FROM positions p
            WHERE p.execution_mode = %(mode)s
              AND p.status = 'open'
          ), 0) AS open_positions,
          COALESCE((
            SELECT SUM(o.contracts * COALESCE(o.limit_price, 0))
            FROM orders o
            WHERE o.execution_mode = %(mode)s
              AND o.created_at >= NOW() - INTERVAL '24 hours'
          ), 0)::float8 AS notional_24h_usd,
          COALESCE((
            SELECT SUM(p.contracts * p.entry_price)
            FROM positions p
            WHERE p.execution_mode = %(mode)s
              AND p.status = 'open'
          ), 0)::float8 AS open_notional_usd,
          COALESCE((
            SELECT SUM(p.realized_pnl)
            FROM positions p
            WHERE p.execution_mode = %(mode)s
              AND p.realized_pnl IS NOT NULL
          ), 0)::float8 AS realized_pnl_usd
    """

    try:
        with get_connection() as conn, conn.cursor() as cur:
            cur.execute(query, {"mode": execution_mode})
            row = cur.fetchone()
    except Exception as exc:
        raise PerformanceRepositoryError(f"Failed to load performance summary: {exc}") from exc

    return PerformanceSummary(
        total_orders=int(row["total_orders"]),
        orders_24h=int(row["orders_24h"]),
        approved_decisions_24h=int(row["approved_decisions_24h"]),
        open_positions=int(row["open_positions"]),
        notional_24h_usd=float(row["notional_24h_usd"]),
        open_notional_usd=float(row["open_notional_usd"]),
        realized_pnl_usd=float(row["realized_pnl_usd"]),
    )


def get_performance_history(days: int = 14, execution_mode: str = "paper") -> list[PerformanceHistoryPoint]:
    since_date = date.today() - timedelta(days=max(days - 1, 0))
    query = """
        SELECT
          DATE_TRUNC('day', o.created_at)::date AS day,
          COUNT(*) AS orders,
          COALESCE(SUM(o.contracts * COALESCE(o.limit_price, 0)), 0)::float8 AS notional_usd
        FROM orders o
        WHERE o.execution_mode = %s
          AND o.created_at::date >= %s
        GROUP BY DATE_TRUNC('day', o.created_at)::date
        ORDER BY day ASC
    """

    try:
        with get_connection() as conn, conn.cursor() as cur:
            cur.execute(query, (execution_mode, since_date))
            rows = cur.fetchall()
    except Exception as exc:
        raise PerformanceRepositoryError(f"Failed to load performance history: {exc}") from exc

    by_day: dict[date, tuple[int, float]] = {}
    for row in rows:
        row_day = row["day"]
        by_day[row_day] = (int(row["orders"]), float(row["notional_usd"]))

    points: list[PerformanceHistoryPoint] = []
    current = since_date
    end = date.today()
    while current <= end:
        orders, notional = by_day.get(current, (0, 0.0))
        points.append(
            PerformanceHistoryPoint(
                day=current.isoformat(),
                orders=orders,
                notional_usd=notional,
            )
        )
        current += timedelta(days=1)

    return points


def get_accuracy_summary(days: int = 30, execution_mode: str = "paper") -> AccuracySummary:
    since_date = date.today() - timedelta(days=max(days - 1, 0))

    query = """
        WITH scored AS (
          SELECT
            dm.metric_date,
            dm.brier_score::float8 AS brier_score,
            dm.log_loss::float8 AS log_loss,
            dm.calibration_error::float8 AS calibration_error,
            COALESCE((
              SELECT COUNT(*)
              FROM settlements s
              JOIN LATERAL (
                SELECT p.predicted_at
                FROM predictions p
                WHERE p.market_id = s.market_id
                  AND p.predicted_at <= s.settled_at
                ORDER BY p.predicted_at DESC
                LIMIT 1
              ) pred ON TRUE
              WHERE s.settled_at::date = dm.metric_date
            ), 0) AS resolved_markets
          FROM daily_metrics dm
          WHERE dm.execution_mode = %s
            AND dm.metric_date >= %s
        )
        SELECT
          COALESCE(SUM(scored.resolved_markets), 0) AS resolved_markets,
          MAX(scored.metric_date) AS latest_metric_date,
          CASE
            WHEN COALESCE(SUM(scored.resolved_markets), 0) > 0
              THEN SUM(scored.brier_score * scored.resolved_markets)::float8
                / SUM(scored.resolved_markets)::float8
          END AS brier_score,
          CASE
            WHEN COALESCE(SUM(scored.resolved_markets), 0) > 0
              THEN SUM(scored.log_loss * scored.resolved_markets)::float8
                / SUM(scored.resolved_markets)::float8
          END AS log_loss,
          CASE
            WHEN COALESCE(SUM(scored.resolved_markets), 0) > 0
              THEN SUM(scored.calibration_error * scored.resolved_markets)::float8
                / SUM(scored.resolved_markets)::float8
          END AS calibration_error
        FROM scored
    """

    try:
        with get_connection() as conn, conn.cursor() as cur:
            cur.execute(query, (execution_mode, since_date))
            row = cur.fetchone()
    except Exception as exc:
        raise PerformanceRepositoryError(f"Failed to load accuracy summary: {exc}") from exc

    latest = row["latest_metric_date"]
    return AccuracySummary(
        window_days=days,
        resolved_markets=int(row["resolved_markets"] or 0),
        latest_metric_date=latest.isoformat() if latest else None,
        brier_score=_float_or_none(row["brier_score"]),
        log_loss=_float_or_none(row["log_loss"]),
        calibration_error=_float_or_none(row["calibration_error"]),
    )


def get_accuracy_history(
    days: int = 30, execution_mode: str = "paper"
) -> list[AccuracyHistoryPoint]:
    since_date = date.today() - timedelta(days=max(days - 1, 0))
    query = """
        SELECT
          dm.metric_date,
          dm.brier_score::float8 AS brier_score,
          dm.log_loss::float8 AS log_loss,
          dm.calibration_error::float8 AS calibration_error,
          dm.gross_pnl::float8 AS gross_pnl,
          dm.net_pnl::float8 AS net_pnl,
          dm.max_drawdown::float8 AS max_drawdown,
          COALESCE((
            SELECT COUNT(*)
            FROM settlements s
            JOIN LATERAL (
              SELECT p.predicted_at
              FROM predictions p
              WHERE p.market_id = s.market_id
                AND p.predicted_at <= s.settled_at
              ORDER BY p.predicted_at DESC
              LIMIT 1
            ) pred ON TRUE
            WHERE s.settled_at::date = dm.metric_date
          ), 0) AS resolved_markets
        FROM daily_metrics dm
        WHERE dm.execution_mode = %s
          AND dm.metric_date >= %s
        ORDER BY dm.metric_date ASC
    """

    try:
        with get_connection() as conn, conn.cursor() as cur:
            cur.execute(query, (execution_mode, since_date))
            rows = cur.fetchall()
    except Exception as exc:
        raise PerformanceRepositoryError(f"Failed to load accuracy history: {exc}") from exc

    by_day: dict[date, AccuracyHistoryPoint] = {}
    for row in rows:
        metric_date = row["metric_date"]
        by_day[metric_date] = AccuracyHistoryPoint(
            day=metric_date.isoformat(),
            resolved_markets=int(row["resolved_markets"] or 0),
            brier_score=_float_or_none(row["brier_score"]),
            log_loss=_float_or_none(row["log_loss"]),
            calibration_error=_float_or_none(row["calibration_error"]),
            gross_pnl=float(row["gross_pnl"] or 0.0),
            net_pnl=float(row["net_pnl"] or 0.0),
            max_drawdown=_float_or_none(row["max_drawdown"]),
        )

    points: list[AccuracyHistoryPoint] = []
    current = since_date
    end = date.today()
    while current <= end:
        point = by_day.get(current)
        if point is None:
            points.append(
                AccuracyHistoryPoint(
                    day=current.isoformat(),
                    resolved_markets=0,
                    brier_score=None,
                    log_loss=None,
                    calibration_error=None,
                    gross_pnl=0.0,
                    net_pnl=0.0,
                    max_drawdown=None,
                )
            )
        else:
            points.append(point)
        current += timedelta(days=1)
    return points


def empty_performance_summary() -> PerformanceSummary:
    return PerformanceSummary(
        total_orders=0,
        orders_24h=0,
        approved_decisions_24h=0,
        open_positions=0,
        notional_24h_usd=0.0,
        open_notional_usd=0.0,
        realized_pnl_usd=0.0,
    )


def list_recent_orders(limit: int = 20, execution_mode: str = "paper") -> list[PaperOrderRow]:
    query = """
        SELECT
          o.created_at,
          m.market_ticker,
          o.side,
          o.contracts,
          COALESCE(o.limit_price, 0)::float8 AS limit_price,
          o.status,
          td.edge
        FROM orders o
        JOIN trade_decisions td ON td.id = o.decision_id
        JOIN predictions p ON p.id = td.prediction_id
        JOIN markets m ON m.id = p.market_id
        WHERE o.execution_mode = %s
        ORDER BY o.created_at DESC
        LIMIT %s
    """
    try:
        with get_connection() as conn, conn.cursor() as cur:
            cur.execute(query, (execution_mode, limit))
            rows = cur.fetchall()
    except Exception as exc:
        raise PerformanceRepositoryError(f"Failed to load recent orders: {exc}") from exc

    return [
        PaperOrderRow(
            created_at=row["created_at"],
            market_ticker=row["market_ticker"],
            side=row["side"],
            contracts=int(row["contracts"]),
            limit_price=float(row["limit_price"]),
            status=row["status"],
            edge=float(row["edge"]),
        )
        for row in rows
    ]


def empty_accuracy_summary(days: int = 30) -> AccuracySummary:
    return AccuracySummary(
        window_days=days,
        resolved_markets=0,
        latest_metric_date=None,
        brier_score=None,
        log_loss=None,
        calibration_error=None,
    )


def _float_or_none(raw) -> float | None:
    if raw is None:
        return None
    return float(raw)
