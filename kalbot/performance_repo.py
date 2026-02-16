from __future__ import annotations

from datetime import date, timedelta

from kalbot.db import get_connection
from kalbot.schemas import PaperOrderRow, PerformanceHistoryPoint, PerformanceSummary


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
