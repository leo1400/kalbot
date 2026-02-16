from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone

from psycopg import errors

from kalbot.db import get_connection
from kalbot.schemas import BotLeaderboardEntry, CopyActivityEvent


class BotIntelRepositoryError(RuntimeError):
    pass


@dataclass(frozen=True)
class DemoTrader:
    platform: str
    account_address: str
    display_name: str
    entity_type: str
    roi_pct: float
    pnl_usd: float
    volume_usd: float
    source: str

    @property
    def impressiveness_score(self) -> float:
        return self.roi_pct


def seed_demo_bot_intel(run_date: date) -> str:
    traders = [
        DemoTrader(
            platform="KALSHI",
            account_address="0x4a1f...91bd",
            display_name="TempEdge_Atlas",
            entity_type="bot",
            roi_pct=42.7,
            pnl_usd=12840.21,
            volume_usd=30092.33,
            source="kalbot_demo_seed",
        ),
        DemoTrader(
            platform="KALSHI",
            account_address="0x7f88...0de1",
            display_name="StormAlpha",
            entity_type="bot",
            roi_pct=36.2,
            pnl_usd=9932.44,
            volume_usd=27438.72,
            source="kalbot_demo_seed",
        ),
        DemoTrader(
            platform="KALSHI",
            account_address="0x9bc2...4fa0",
            display_name="PolarConvex",
            entity_type="person",
            roi_pct=31.1,
            pnl_usd=7832.11,
            volume_usd=25189.54,
            source="kalbot_demo_seed",
        ),
    ]

    try:
        with get_connection() as conn, conn.cursor() as cur:
            for trader in traders:
                cur.execute(
                    """
                    INSERT INTO tracked_traders (
                      platform, account_address, display_name, entity_type, source, is_active
                    )
                    VALUES (%s, %s, %s, %s, %s, TRUE)
                    ON CONFLICT (platform, account_address)
                    DO UPDATE SET
                      display_name = EXCLUDED.display_name,
                      entity_type = EXCLUDED.entity_type,
                      source = EXCLUDED.source,
                      updated_at = NOW()
                    RETURNING id
                    """,
                    (
                        trader.platform,
                        trader.account_address,
                        trader.display_name,
                        trader.entity_type,
                        trader.source,
                    ),
                )
                trader_id = cur.fetchone()["id"]

                cur.execute(
                    """
                    INSERT INTO trader_performance_snapshots (
                      trader_id, snapshot_date, window_name, roi_pct, pnl_usd, volume_usd,
                      win_rate_pct, impressiveness_score, source, created_at
                    )
                    VALUES (%s, %s, 'all', %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (trader_id, snapshot_date, window_name, source)
                    DO UPDATE SET
                      roi_pct = EXCLUDED.roi_pct,
                      pnl_usd = EXCLUDED.pnl_usd,
                      volume_usd = EXCLUDED.volume_usd,
                      win_rate_pct = EXCLUDED.win_rate_pct,
                      impressiveness_score = EXCLUDED.impressiveness_score
                    """,
                    (
                        trader_id,
                        run_date,
                        trader.roi_pct,
                        trader.pnl_usd,
                        trader.volume_usd,
                        63.5,
                        trader.impressiveness_score,
                        trader.source,
                        datetime.now(timezone.utc),
                    ),
                )

            cur.execute(
                """
                SELECT id FROM tracked_traders
                WHERE platform = 'KALSHI' AND account_address = %s
                """,
                (traders[0].account_address,),
            )
            leader_id = cur.fetchone()["id"]

            cur.execute(
                """
                INSERT INTO copy_activity_events (
                  event_time, follower_alias, leader_trader_id, market_ticker, side, contracts, pnl_usd, source
                )
                VALUES (NOW(), %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    "RainRunner",
                    leader_id,
                    f"WEATHER-NYC-{run_date.isoformat()}-HIGH-GT-45F",
                    "yes",
                    8,
                    23.50,
                    "kalbot_demo_seed",
                ),
            )
    except errors.UndefinedTable as exc:
        raise BotIntelRepositoryError(
            "Bot intel schema missing. Apply infra/migrations/002_bot_intel.sql."
        ) from exc
    except Exception as exc:
        raise BotIntelRepositoryError(f"Failed to seed bot intel: {exc}") from exc

    return "Bot intel snapshot seeded."


def get_bot_leaderboard(
    window: str = "all", sort: str = "impressiveness", limit: int = 20
) -> list[BotLeaderboardEntry]:
    sort_sql = {
        "impressiveness": "l.impressiveness_score DESC",
        "pnl": "l.pnl_usd DESC",
        "volume": "l.volume_usd DESC",
        "roi": "l.roi_pct DESC",
    }.get(sort, "l.impressiveness_score DESC")

    query = f"""
        WITH latest AS (
          SELECT
            s.*,
            ROW_NUMBER() OVER (
              PARTITION BY s.trader_id
              ORDER BY s.snapshot_date DESC, s.created_at DESC
            ) AS rn
          FROM trader_performance_snapshots s
          WHERE s.window_name = %s
        )
        SELECT
          t.platform,
          t.display_name,
          t.account_address,
          t.entity_type,
          l.roi_pct,
          l.pnl_usd,
          l.volume_usd,
          l.impressiveness_score,
          l.snapshot_date,
          l.source
        FROM latest l
        JOIN tracked_traders t ON t.id = l.trader_id
        WHERE l.rn = 1
        ORDER BY {sort_sql}
        LIMIT %s
    """

    try:
        with get_connection() as conn, conn.cursor() as cur:
            cur.execute(query, (window, limit))
            rows = cur.fetchall()
    except Exception as exc:
        raise BotIntelRepositoryError(f"Failed to load leaderboard: {exc}") from exc

    entries: list[BotLeaderboardEntry] = []
    for index, row in enumerate(rows, start=1):
        entries.append(
            BotLeaderboardEntry(
                rank=index,
                platform=row["platform"],
                display_name=row["display_name"],
                account_address=row["account_address"],
                entity_type=row["entity_type"],
                roi_pct=float(row["roi_pct"]),
                pnl_usd=float(row["pnl_usd"]),
                volume_usd=float(row["volume_usd"]),
                impressiveness_score=float(row["impressiveness_score"]),
                snapshot_date=row["snapshot_date"].isoformat(),
                source=row["source"],
            )
        )
    return entries


def list_recent_copy_activity(limit: int = 20) -> list[CopyActivityEvent]:
    query = """
        SELECT
          c.event_time,
          c.follower_alias,
          t.display_name AS leader_display_name,
          c.market_ticker,
          c.side,
          c.contracts,
          c.pnl_usd
        FROM copy_activity_events c
        JOIN tracked_traders t ON t.id = c.leader_trader_id
        ORDER BY c.event_time DESC
        LIMIT %s
    """

    try:
        with get_connection() as conn, conn.cursor() as cur:
            cur.execute(query, (limit,))
            rows = cur.fetchall()
    except Exception as exc:
        raise BotIntelRepositoryError(f"Failed to load copy activity: {exc}") from exc

    events: list[CopyActivityEvent] = []
    for row in rows:
        events.append(
            CopyActivityEvent(
                event_time=row["event_time"],
                follower_alias=row["follower_alias"],
                leader_display_name=row["leader_display_name"],
                market_ticker=row["market_ticker"],
                side=row["side"],
                contracts=int(row["contracts"]),
                pnl_usd=float(row["pnl_usd"]),
            )
        )
    return events
