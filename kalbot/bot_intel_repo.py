from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen

from psycopg import errors

from kalbot.db import get_connection
from kalbot.schemas import BotLeaderboardEntry, CopyActivityEvent
from kalbot.settings import Settings, get_settings


class BotIntelRepositoryError(RuntimeError):
    pass


@dataclass(frozen=True)
class TraderSnapshotInput:
    platform: str
    account_address: str
    display_name: str
    entity_type: str
    roi_pct: float
    pnl_usd: float
    volume_usd: float
    win_rate_pct: float | None
    impressiveness_score: float
    source: str


@dataclass(frozen=True)
class CopyEventInput:
    event_time: datetime
    follower_alias: str
    leader_account_address: str
    market_ticker: str
    side: str
    contracts: int
    pnl_usd: float
    source: str


@dataclass(frozen=True)
class BotIntelFeed:
    snapshot_date: date
    source: str
    traders: list[TraderSnapshotInput]
    activity: list[CopyEventInput]


def refresh_bot_intel(run_date: date, settings: Settings | None = None) -> str:
    cfg = settings or get_settings()
    if not cfg.bot_intel_ingest_enabled:
        return "Bot intel ingest disabled by config."

    try:
        with get_connection() as conn, conn.cursor() as cur:
            purged = _purge_synthetic_rows(cur) if not cfg.bot_intel_allow_demo_seed else 0

            feed = _load_feed_for_date(run_date=run_date, settings=cfg)
            if feed is None:
                if purged > 0:
                    return f"No bot intel feed configured; skipped (purged {purged} synthetic rows)."
                return "No bot intel feed configured; skipping."

            trader_map: dict[str, int] = {}
            snapshots_written = 0
            events_written = 0

            for trader in feed.traders:
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
                trader_id = int(cur.fetchone()["id"])
                trader_map[trader.account_address] = trader_id

                cur.execute(
                    """
                    INSERT INTO trader_performance_snapshots (
                      trader_id, snapshot_date, window_name, roi_pct, pnl_usd, volume_usd,
                      win_rate_pct, impressiveness_score, source, created_at
                    )
                    VALUES (%s, %s, 'all', %s, %s, %s, %s, %s, %s, NOW())
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
                        feed.snapshot_date,
                        trader.roi_pct,
                        trader.pnl_usd,
                        trader.volume_usd,
                        trader.win_rate_pct,
                        trader.impressiveness_score,
                        trader.source,
                    ),
                )
                snapshots_written += int(cur.rowcount or 0)

            for event in feed.activity:
                leader_id = trader_map.get(event.leader_account_address)
                if leader_id is None:
                    continue

                cur.execute(
                    """
                    SELECT 1
                    FROM copy_activity_events
                    WHERE source = %s
                      AND event_time::date = %s
                      AND follower_alias = %s
                      AND leader_trader_id = %s
                      AND market_ticker = %s
                      AND side = %s
                      AND contracts = %s
                    LIMIT 1
                    """,
                    (
                        event.source,
                        event.event_time.date(),
                        event.follower_alias,
                        leader_id,
                        event.market_ticker,
                        event.side,
                        event.contracts,
                    ),
                )
                if cur.fetchone() is not None:
                    continue

                cur.execute(
                    """
                    INSERT INTO copy_activity_events (
                      event_time, follower_alias, leader_trader_id, market_ticker, side, contracts, pnl_usd, source
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        event.event_time,
                        event.follower_alias,
                        leader_id,
                        event.market_ticker,
                        event.side,
                        event.contracts,
                        event.pnl_usd,
                        event.source,
                    ),
                )
                events_written += int(cur.rowcount or 0)
    except errors.UndefinedTable as exc:
        raise BotIntelRepositoryError(
            "Bot intel schema missing. Apply infra/migrations/002_bot_intel.sql."
        ) from exc
    except Exception as exc:
        raise BotIntelRepositoryError(f"Failed bot intel ingest: {exc}") from exc

    return (
        "Bot intel ingest complete: "
        f"traders={len(feed.traders)}, snapshots={snapshots_written}, "
        f"events={events_written}, source={feed.source}, purged_synthetic={purged}."
    )


def _purge_synthetic_rows(cur) -> int:
    total = 0
    cur.execute(
        """
        DELETE FROM copy_activity_events
        WHERE source ILIKE '%demo%'
           OR source ILIKE '%seed%'
        """
    )
    total += int(cur.rowcount or 0)

    cur.execute(
        """
        DELETE FROM trader_performance_snapshots
        WHERE source ILIKE '%demo%'
           OR source ILIKE '%seed%'
        """
    )
    total += int(cur.rowcount or 0)

    cur.execute(
        """
        DELETE FROM tracked_traders t
        WHERE (t.source ILIKE '%demo%' OR t.source ILIKE '%seed%')
          AND NOT EXISTS (
            SELECT 1 FROM trader_performance_snapshots s WHERE s.trader_id = t.id
          )
          AND NOT EXISTS (
            SELECT 1 FROM copy_activity_events c WHERE c.leader_trader_id = t.id
          )
        """
    )
    total += int(cur.rowcount or 0)
    return total


def _load_feed_for_date(run_date: date, settings: Settings) -> BotIntelFeed | None:
    payload: dict[str, Any] | None = None

    if settings.bot_intel_feed_path:
        path = Path(settings.bot_intel_feed_path)
        if not path.exists():
            raise BotIntelRepositoryError(f"Bot intel feed file not found: {path}")
        payload = json.loads(path.read_text(encoding="utf-8"))
    elif settings.bot_intel_feed_url:
        req = Request(
            url=settings.bot_intel_feed_url,
            headers={"User-Agent": "kalbot-bot-intel/1.0", "Accept": "application/json"},
            method="GET",
        )
        with urlopen(req, timeout=20) as response:
            payload = json.loads(response.read().decode("utf-8"))
    elif settings.bot_intel_allow_demo_seed:
        payload = _build_demo_seed_payload(run_date)

    if payload is None:
        return None

    return _parse_feed_payload(
        payload=payload,
        default_source=settings.bot_intel_source_name,
        run_date=run_date,
    )


def _parse_feed_payload(payload: dict[str, Any], default_source: str, run_date: date) -> BotIntelFeed:
    source = str(payload.get("source") or default_source).strip()
    if not source:
        source = "external_feed"

    snapshot_text = str(payload.get("snapshot_date") or run_date.isoformat())
    try:
        snapshot_date = date.fromisoformat(snapshot_text)
    except ValueError:
        snapshot_date = run_date

    traders = _parse_traders(payload.get("traders"), source)
    activity = _parse_activity(payload.get("activity"), source, run_date)
    return BotIntelFeed(snapshot_date=snapshot_date, source=source, traders=traders, activity=activity)


def _parse_traders(raw: Any, source: str) -> list[TraderSnapshotInput]:
    if not isinstance(raw, list):
        return []

    rows: list[TraderSnapshotInput] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        account = str(item.get("account_address") or "").strip()
        display = str(item.get("display_name") or "").strip()
        if not account or not display:
            continue

        roi_pct = _as_float(item.get("roi_pct"), 0.0)
        rows.append(
            TraderSnapshotInput(
                platform=str(item.get("platform") or "KALSHI").upper(),
                account_address=account,
                display_name=display,
                entity_type=str(item.get("entity_type") or "bot"),
                roi_pct=roi_pct,
                pnl_usd=_as_float(item.get("pnl_usd"), 0.0),
                volume_usd=_as_float(item.get("volume_usd"), 0.0),
                win_rate_pct=_as_float_or_none(item.get("win_rate_pct")),
                impressiveness_score=_as_float(item.get("impressiveness_score"), roi_pct),
                source=str(item.get("source") or source),
            )
        )
    return rows


def _parse_activity(raw: Any, source: str, run_date: date) -> list[CopyEventInput]:
    if not isinstance(raw, list):
        return []

    rows: list[CopyEventInput] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        leader_account = str(item.get("leader_account_address") or "").strip()
        follower = str(item.get("follower_alias") or "").strip()
        ticker = str(item.get("market_ticker") or "").strip()
        side = str(item.get("side") or "").strip().lower()
        contracts = _as_int(item.get("contracts"), 0)
        if not leader_account or not follower or not ticker or side not in {"yes", "no"} or contracts <= 0:
            continue

        rows.append(
            CopyEventInput(
                event_time=_parse_event_time(item.get("event_time"), run_date),
                follower_alias=follower,
                leader_account_address=leader_account,
                market_ticker=ticker,
                side=side,
                contracts=contracts,
                pnl_usd=_as_float(item.get("pnl_usd"), 0.0),
                source=str(item.get("source") or source),
            )
        )
    return rows


def _parse_event_time(raw: Any, run_date: date) -> datetime:
    if isinstance(raw, str) and raw.strip():
        text = raw.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except ValueError:
            pass
    return datetime(run_date.year, run_date.month, run_date.day, 12, 0, tzinfo=timezone.utc)


def _as_float(raw: Any, default: float) -> float:
    try:
        return float(raw)
    except (TypeError, ValueError):
        return default


def _as_float_or_none(raw: Any) -> float | None:
    try:
        if raw is None:
            return None
        return float(raw)
    except (TypeError, ValueError):
        return None


def _as_int(raw: Any, default: int) -> int:
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _build_demo_seed_payload(run_date: date) -> dict[str, Any]:
    return {
        "source": "kalbot_demo_seed",
        "snapshot_date": run_date.isoformat(),
        "traders": [
            {
                "platform": "KALSHI",
                "account_address": "0x4a1f...91bd",
                "display_name": "TempEdge_Atlas",
                "entity_type": "bot",
                "roi_pct": 42.7,
                "pnl_usd": 12840.21,
                "volume_usd": 30092.33,
                "win_rate_pct": 63.5,
                "impressiveness_score": 42.7,
            }
        ],
        "activity": [
            {
                "event_time": f"{run_date.isoformat()}T12:00:00Z",
                "follower_alias": "RainRunner",
                "leader_account_address": "0x4a1f...91bd",
                "market_ticker": f"WEATHER-NYC-{run_date.isoformat()}-HIGH-GT-45F",
                "side": "yes",
                "contracts": 8,
                "pnl_usd": 23.5,
            }
        ],
    }


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
          c.source,
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
                source=row["source"],
                side=row["side"],
                contracts=int(row["contracts"]),
                pnl_usd=float(row["pnl_usd"]),
            )
        )
    return events
