from __future__ import annotations

import csv
import json
from io import StringIO
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
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
            purged = _purge_synthetic_rows(cur)

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
           OR source ILIKE '%sample%'
           OR source ILIKE '%example%'
           OR source ILIKE '%synthetic%'
           OR source ILIKE '%test%'
        """
    )
    total += int(cur.rowcount or 0)

    cur.execute(
        """
        DELETE FROM trader_performance_snapshots
        WHERE source ILIKE '%demo%'
           OR source ILIKE '%seed%'
           OR source ILIKE '%sample%'
           OR source ILIKE '%example%'
           OR source ILIKE '%synthetic%'
           OR source ILIKE '%test%'
        """
    )
    total += int(cur.rowcount or 0)

    cur.execute(
        """
        DELETE FROM tracked_traders t
        WHERE (
          t.source ILIKE '%demo%'
          OR t.source ILIKE '%seed%'
          OR t.source ILIKE '%sample%'
          OR t.source ILIKE '%example%'
          OR t.source ILIKE '%synthetic%'
          OR t.source ILIKE '%test%'
        )
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
    feed_format = (settings.bot_intel_feed_format or "auto").strip().lower()

    if settings.bot_intel_feed_path:
        path = Path(settings.bot_intel_feed_path)
        if not path.exists():
            raise BotIntelRepositoryError(f"Bot intel feed file not found: {path}")
        raw = path.read_text(encoding="utf-8")
        inferred = feed_format if feed_format != "auto" else _infer_feed_format(path.suffix, "")
        payload = _parse_raw_feed_payload(
            raw=raw,
            feed_format=inferred,
            default_source=settings.bot_intel_source_name,
            run_date=run_date,
        )
    elif settings.bot_intel_feed_url:
        headers = _parse_headers(settings.bot_intel_feed_headers_json)
        headers.setdefault("User-Agent", "kalbot-bot-intel/1.0")
        headers.setdefault("Accept", "application/json,text/csv;q=0.9,*/*;q=0.8")
        req = Request(
            url=settings.bot_intel_feed_url,
            headers=headers,
            method="GET",
        )
        with urlopen(req, timeout=max(1, settings.bot_intel_feed_timeout_seconds)) as response:
            raw = response.read().decode("utf-8")
            content_type = str(response.headers.get("Content-Type", ""))
            inferred = (
                feed_format
                if feed_format != "auto"
                else _infer_feed_format(settings.bot_intel_feed_url, content_type)
            )
            payload = _parse_raw_feed_payload(
                raw=raw,
                feed_format=inferred,
                default_source=settings.bot_intel_source_name,
                run_date=run_date,
            )
    else:
        provider = (settings.bot_intel_provider or "").strip().lower()
        if provider in {"", "none"}:
            return None
        if provider == "polymarket":
            payload = _load_polymarket_leaderboard_payload(run_date=run_date, settings=settings)
        else:
            raise BotIntelRepositoryError(f"Unsupported bot intel provider: {settings.bot_intel_provider}")

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


def _parse_raw_feed_payload(
    raw: str,
    feed_format: str,
    default_source: str,
    run_date: date,
) -> dict[str, Any]:
    fmt = feed_format.strip().lower()
    if fmt == "json":
        return json.loads(raw)
    if fmt == "csv":
        return _payload_from_csv(raw=raw, default_source=default_source, run_date=run_date)
    raise BotIntelRepositoryError(f"Unsupported bot intel feed format: {feed_format}")


def _infer_feed_format(source_hint: str, content_type: str) -> str:
    lower_hint = source_hint.lower()
    lower_ct = content_type.lower()
    if ".csv" in lower_hint or "text/csv" in lower_ct:
        return "csv"
    return "json"


def _parse_headers(raw_json: str | None) -> dict[str, str]:
    if not raw_json:
        return {}
    try:
        payload = json.loads(raw_json)
    except json.JSONDecodeError as exc:
        raise BotIntelRepositoryError(
            "KALBOT_BOT_INTEL_FEED_HEADERS_JSON is not valid JSON."
        ) from exc
    if not isinstance(payload, dict):
        raise BotIntelRepositoryError("KALBOT_BOT_INTEL_FEED_HEADERS_JSON must be a JSON object.")
    headers: dict[str, str] = {}
    for key, value in payload.items():
        text_key = str(key).strip()
        if not text_key:
            continue
        headers[text_key] = str(value)
    return headers


def _payload_from_csv(raw: str, default_source: str, run_date: date) -> dict[str, Any]:
    rows = list(csv.DictReader(StringIO(raw)))
    traders: list[dict[str, Any]] = []
    activity: list[dict[str, Any]] = []
    snapshot_date = run_date.isoformat()
    source_name = default_source

    for row in rows:
        normalized = {str(k or "").strip().lower(): (v or "").strip() for k, v in row.items()}
        if not any(normalized.values()):
            continue
        row_type = (normalized.get("record_type") or "").lower()
        row_source = normalized.get("source")
        if row_source:
            source_name = row_source
        row_snapshot = normalized.get("snapshot_date")
        if row_snapshot:
            snapshot_date = row_snapshot

        if row_type == "activity" or (
            normalized.get("leader_account_address")
            and normalized.get("follower_alias")
            and normalized.get("market_ticker")
        ):
            activity.append(
                {
                    "event_time": normalized.get("event_time"),
                    "follower_alias": normalized.get("follower_alias"),
                    "leader_account_address": normalized.get("leader_account_address"),
                    "market_ticker": normalized.get("market_ticker"),
                    "side": normalized.get("side"),
                    "contracts": normalized.get("contracts"),
                    "pnl_usd": normalized.get("pnl_usd"),
                    "source": row_source or source_name,
                }
            )
            continue

        if normalized.get("account_address") and normalized.get("display_name"):
            traders.append(
                {
                    "platform": normalized.get("platform") or "KALSHI",
                    "account_address": normalized.get("account_address"),
                    "display_name": normalized.get("display_name"),
                    "entity_type": normalized.get("entity_type") or "bot",
                    "roi_pct": normalized.get("roi_pct") or "0",
                    "pnl_usd": normalized.get("pnl_usd") or "0",
                    "volume_usd": normalized.get("volume_usd") or "0",
                    "win_rate_pct": normalized.get("win_rate_pct") or None,
                    "impressiveness_score": normalized.get("impressiveness_score")
                    or normalized.get("roi_pct")
                    or "0",
                    "source": row_source or source_name,
                }
            )

    return {
        "source": source_name,
        "snapshot_date": snapshot_date,
        "traders": traders,
        "activity": activity,
    }


def _load_polymarket_leaderboard_payload(run_date: date, settings: Settings) -> dict[str, Any]:
    api_base = settings.polymarket_api_base.rstrip("/")
    query = urlencode(
        {
            "timeFrame": settings.polymarket_leaderboard_timeframe,
            "category": settings.polymarket_leaderboard_category,
            "limit": max(1, settings.polymarket_leaderboard_limit),
            "offset": 0,
            "sortBy": settings.polymarket_leaderboard_sort_by,
        }
    )
    url = f"{api_base}/v1/leaderboard?{query}"
    request = Request(
        url=url,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            "Accept": "application/json,text/plain,*/*",
            "Referer": "https://www.bastionai.app/",
        },
        method="GET",
    )

    try:
        with urlopen(request, timeout=max(1, settings.bot_intel_feed_timeout_seconds)) as response:
            raw = response.read().decode("utf-8")
    except Exception as exc:
        raise BotIntelRepositoryError(f"Failed to fetch Polymarket leaderboard: {exc}") from exc

    try:
        records = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise BotIntelRepositoryError("Polymarket leaderboard returned invalid JSON.") from exc

    if not isinstance(records, list):
        raise BotIntelRepositoryError("Polymarket leaderboard response was not a list.")

    traders: list[dict[str, Any]] = []
    min_volume = max(0.0, settings.polymarket_min_volume_usd)
    seen_accounts: set[str] = set()
    for item in records:
        if not isinstance(item, dict):
            continue
        account_address = str(item.get("proxyWallet") or "").strip().lower()
        if not account_address or account_address in seen_accounts:
            continue

        volume_usd = _as_float(item.get("vol"), 0.0)
        if volume_usd < min_volume:
            continue

        pnl_usd = _as_float(item.get("pnl"), 0.0)
        roi_pct = (pnl_usd / volume_usd * 100.0) if volume_usd > 0 else 0.0
        seen_accounts.add(account_address)
        traders.append(
            {
                "platform": "POLYMARKET",
                "account_address": account_address,
                "display_name": _coerce_polymarket_name(item, account_address),
                "entity_type": "wallet",
                "roi_pct": roi_pct,
                "pnl_usd": pnl_usd,
                "volume_usd": volume_usd,
                "win_rate_pct": None,
                "impressiveness_score": roi_pct,
                "source": "polymarket_public_leaderboard",
            }
        )

    return {
        "source": "polymarket_public_leaderboard",
        "snapshot_date": run_date.isoformat(),
        "traders": traders,
        "activity": [],
    }


def _coerce_polymarket_name(item: dict[str, Any], account_address: str) -> str:
    user_name = str(item.get("userName") or "").strip()
    if user_name:
        return user_name
    x_name = str(item.get("xUsername") or "").strip()
    if x_name:
        return x_name
    if len(account_address) >= 10:
        return f"{account_address[:6]}...{account_address[-4:]}"
    return account_address or "unknown"


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
