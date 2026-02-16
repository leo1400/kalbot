from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from psycopg import errors

from kalbot.db import get_connection
from kalbot.settings import Settings


class KalshiIngestError(RuntimeError):
    pass


@dataclass
class KalshiIngestSummary:
    series_scanned: int = 0
    markets_written: int = 0
    snapshots_written: int = 0
    failures: list[str] | None = None

    def __post_init__(self) -> None:
        if self.failures is None:
            self.failures = []


def ingest_kalshi_weather_markets(settings: Settings) -> KalshiIngestSummary:
    if not settings.kalshi_ingest_enabled:
        raise KalshiIngestError("Kalshi ingestion disabled by config.")

    summary = KalshiIngestSummary()
    headers = {"Accept": "application/json"}

    series = _fetch_weather_series(settings, headers=headers)
    summary.series_scanned = len(series)

    try:
        with get_connection() as conn, conn.cursor() as cur:
            for series_ticker in series:
                try:
                    markets = _fetch_markets_for_series(
                        settings=settings,
                        headers=headers,
                        series_ticker=series_ticker,
                    )
                    for market in markets:
                        market_id = _upsert_market(cur=cur, market=market)
                        summary.markets_written += 1
                        _insert_market_snapshot(cur=cur, market_id=market_id, market=market)
                        summary.snapshots_written += 1
                except Exception as exc:
                    summary.failures.append(f"{series_ticker}: {exc}")
    except errors.UndefinedTable as exc:
        raise KalshiIngestError(
            "Market tables missing. Apply infra/migrations/001_initial_schema.sql."
        ) from exc
    except Exception as exc:
        raise KalshiIngestError(f"Failed Kalshi ingestion: {exc}") from exc

    return summary


def _fetch_weather_series(settings: Settings, headers: dict[str, str]) -> list[str]:
    cursor: str | None = None
    found: list[str] = []
    seen: set[str] = set()

    while len(found) < settings.kalshi_weather_series_limit:
        params: dict[str, str | int] = {"limit": min(200, settings.kalshi_series_page_size)}
        if cursor:
            params["cursor"] = cursor
        url = f"{settings.kalshi_api_base}/series?{urlencode(params)}"
        payload = _fetch_json(url, headers=headers, timeout_seconds=20)
        rows = payload.get("series", [])
        if not rows:
            break

        for row in rows:
            ticker = row.get("ticker")
            category = (row.get("category") or "").lower()
            if not ticker:
                continue
            if category != settings.kalshi_weather_category.lower():
                continue
            if ticker in seen:
                continue
            found.append(ticker)
            seen.add(ticker)
            if len(found) >= settings.kalshi_weather_series_limit:
                break

        cursor = payload.get("cursor")
        if not cursor:
            break

    return found


def _fetch_markets_for_series(
    settings: Settings, headers: dict[str, str], series_ticker: str
) -> list[dict[str, Any]]:
    params = {
        "series_ticker": series_ticker,
        "status": "open",
        "limit": settings.kalshi_markets_per_series,
    }
    url = f"{settings.kalshi_api_base}/markets?{urlencode(params)}"
    payload = _fetch_json(url, headers=headers, timeout_seconds=25)
    return payload.get("markets", [])


def _upsert_market(cur: Any, market: dict[str, Any]) -> int:
    close_time = _parse_time(market.get("close_time"))
    settle_time = _parse_time(market.get("expiration_time"))
    kalshi_market_id = str(market.get("ticker") or "")
    event_ticker = str(market.get("event_ticker") or "")
    title = str(market.get("title") or kalshi_market_id)

    if not kalshi_market_id or not event_ticker:
        raise KalshiIngestError("Kalshi market missing required ticker fields.")

    cur.execute(
        """
        INSERT INTO markets (
          kalshi_market_id, event_ticker, market_ticker, title, close_time, settle_time
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (kalshi_market_id)
        DO UPDATE SET
          event_ticker = EXCLUDED.event_ticker,
          market_ticker = EXCLUDED.market_ticker,
          title = EXCLUDED.title,
          close_time = EXCLUDED.close_time,
          settle_time = EXCLUDED.settle_time,
          updated_at = NOW()
        RETURNING id
        """,
        (kalshi_market_id, event_ticker, kalshi_market_id, title, close_time, settle_time),
    )
    return int(cur.fetchone()["id"])


def _insert_market_snapshot(cur: Any, market_id: int, market: dict[str, Any]) -> None:
    bid_yes = _price_as_float(market, "yes_bid_dollars", "yes_bid")
    ask_yes = _price_as_float(market, "yes_ask_dollars", "yes_ask")
    last_price_yes = _price_as_float(market, "last_price_dollars", "last_price")
    volume = _int_or_none(market.get("volume"))

    cur.execute(
        """
        INSERT INTO market_snapshots (
          market_id, bid_yes, ask_yes, last_price_yes, volume, captured_at
        )
        VALUES (%s, %s, %s, %s, %s, NOW())
        """,
        (market_id, bid_yes, ask_yes, last_price_yes, volume),
    )


def _price_as_float(
    market: dict[str, Any], dollars_field: str, cents_field: str
) -> float | None:
    dollars = market.get(dollars_field)
    if dollars not in (None, ""):
        try:
            return float(dollars)
        except (TypeError, ValueError):
            pass

    cents = market.get(cents_field)
    if cents is None:
        return None
    try:
        value = float(cents)
    except (TypeError, ValueError):
        return None
    if value > 1:
        return value / 100.0
    return value


def _int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _parse_time(text: Any) -> datetime | None:
    if not text:
        return None
    value = str(text)
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value)


def _fetch_json(
    url: str, headers: dict[str, str], timeout_seconds: int = 20
) -> dict[str, Any]:
    req = Request(url=url, headers=headers, method="GET")
    with urlopen(req, timeout=timeout_seconds) as response:
        raw = response.read().decode("utf-8")
    return json.loads(raw)
