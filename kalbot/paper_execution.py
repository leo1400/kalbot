from __future__ import annotations

from datetime import date

from kalbot.db import get_connection
from kalbot.settings import Settings, get_settings


class PaperExecutionError(RuntimeError):
    pass


def execute_paper_trades(run_date: date) -> str:
    settings = get_settings()
    if settings.execution_mode != "paper":
        return f"Execution skipped (mode={settings.execution_mode})."

    spent_notional = 0.0
    placed_orders = 0

    query = """
        WITH latest_signals AS (
          SELECT DISTINCT ON (ps.market_id)
            ps.market_id,
            ps.model_run_id
          FROM published_signals ps
          WHERE ps.is_active = TRUE
          ORDER BY ps.market_id, ps.published_at DESC
        ),
        latest_predictions AS (
          SELECT
            ls.market_id,
            m.market_ticker,
            p.id AS prediction_id,
            p.prob_yes,
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
            ) AS market_yes
          FROM latest_signals ls
          JOIN markets m ON m.id = ls.market_id
          JOIN predictions p
            ON p.market_id = ls.market_id
           AND p.model_run_id = ls.model_run_id
          LEFT JOIN LATERAL (
            SELECT bid_yes, ask_yes, last_price_yes
            FROM market_snapshots ms
            WHERE ms.market_id = ls.market_id
            ORDER BY ms.captured_at DESC
            LIMIT 1
          ) ms ON TRUE
        )
        SELECT
          lp.market_id,
          lp.market_ticker,
          lp.prediction_id,
          td.id AS decision_id,
          lp.prob_yes,
          lp.market_yes,
          (lp.prob_yes - lp.market_yes) AS edge
        FROM latest_predictions lp
        JOIN LATERAL (
          SELECT id
          FROM trade_decisions td
          WHERE td.prediction_id = lp.prediction_id
          ORDER BY td.created_at DESC
          LIMIT 1
        ) td ON TRUE
        WHERE ABS(lp.prob_yes - lp.market_yes) >= %s
        ORDER BY ABS(lp.prob_yes - lp.market_yes) DESC, lp.market_ticker ASC
    """

    try:
        with get_connection() as conn, conn.cursor() as cur:
            cur.execute(query, (settings.paper_edge_threshold,))
            rows = cur.fetchall()

            for row in rows:
                remaining_budget = settings.max_daily_notional_usd - spent_notional
                if remaining_budget <= 0:
                    break

                side, entry_price = _edge_to_order(
                    edge=float(row["edge"]),
                    market_yes=float(row["market_yes"]),
                )
                per_signal_budget = min(settings.max_notional_per_signal_usd, remaining_budget)
                contracts = _contracts_for_notional(
                    entry_price=entry_price,
                    max_notional=per_signal_budget,
                    max_contracts=settings.max_contracts_per_order,
                )
                if contracts <= 0:
                    continue

                cur.execute(
                    """
                    SELECT 1
                    FROM positions
                    WHERE market_id = %s
                      AND execution_mode = %s
                      AND side = %s
                      AND status = 'open'
                    LIMIT 1
                    """,
                    (row["market_id"], settings.execution_mode, side),
                )
                if cur.fetchone() is not None:
                    continue

                order_ref = (
                    f"paper-{run_date.isoformat()}-{row['market_ticker']}-{placed_orders + 1}"
                )
                cur.execute(
                    """
                    INSERT INTO orders (
                      decision_id, execution_mode, side, contracts, limit_price,
                      status, external_order_id, created_at, updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, 'filled', %s, NOW(), NOW())
                    RETURNING id
                    """,
                    (
                        row["decision_id"],
                        settings.execution_mode,
                        side,
                        contracts,
                        entry_price,
                        order_ref,
                    ),
                )
                cur.fetchone()

                cur.execute(
                    """
                    INSERT INTO positions (
                      market_id, execution_mode, side, entry_price, contracts, opened_at, status
                    )
                    VALUES (%s, %s, %s, %s, %s, NOW(), 'open')
                    """,
                    (
                        row["market_id"],
                        settings.execution_mode,
                        side,
                        entry_price,
                        contracts,
                    ),
                )

                spent_notional += contracts * entry_price
                placed_orders += 1

    except Exception as exc:
        raise PaperExecutionError(f"Failed to execute paper trades: {exc}") from exc

    if placed_orders == 0:
        return "Paper execution complete: no new orders placed."

    return (
        "Paper execution complete: "
        f"orders={placed_orders}, spent_notional=${spent_notional:.2f}, "
        f"edge_threshold={settings.paper_edge_threshold:.3f}."
    )


def _contracts_for_notional(entry_price: float, max_notional: float, max_contracts: int) -> int:
    if entry_price <= 0 or max_notional <= 0 or max_contracts <= 0:
        return 0
    affordable = int(max_notional // entry_price)
    return max(0, min(affordable, max_contracts))


def _edge_to_order(edge: float, market_yes: float) -> tuple[str, float]:
    clipped_market_yes = max(0.01, min(0.99, market_yes))
    if edge >= 0:
        return "yes", clipped_market_yes
    return "no", max(0.01, min(0.99, 1.0 - clipped_market_yes))
