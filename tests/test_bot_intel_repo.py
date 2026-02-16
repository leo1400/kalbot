from datetime import date

from kalbot.bot_intel_repo import _parse_feed_payload, _parse_raw_feed_payload


def test_parse_feed_payload_builds_traders_and_activity() -> None:
    payload = {
        "source": "third_party_feed",
        "snapshot_date": "2026-02-16",
        "traders": [
            {
                "platform": "kalshi",
                "account_address": "0xabc",
                "display_name": "StormPilot",
                "entity_type": "bot",
                "roi_pct": 12.5,
                "pnl_usd": 1032.21,
                "volume_usd": 8812.00,
            }
        ],
        "activity": [
            {
                "event_time": "2026-02-16T12:34:56Z",
                "follower_alias": "CloudRunner",
                "leader_account_address": "0xabc",
                "market_ticker": "KXLOWTLAX-26FEB16-B53.5",
                "side": "yes",
                "contracts": 5,
                "pnl_usd": 7.2,
            }
        ],
    }

    feed = _parse_feed_payload(payload=payload, default_source="fallback_feed", run_date=date(2026, 2, 16))

    assert feed.source == "third_party_feed"
    assert feed.snapshot_date == date(2026, 2, 16)
    assert len(feed.traders) == 1
    assert feed.traders[0].platform == "KALSHI"
    assert feed.traders[0].impressiveness_score == 12.5
    assert len(feed.activity) == 1
    assert feed.activity[0].side == "yes"


def test_parse_feed_payload_skips_invalid_rows() -> None:
    payload = {
        "traders": [{"display_name": "Missing account"}],
        "activity": [{"leader_account_address": "0xabc"}],
    }
    feed = _parse_feed_payload(payload=payload, default_source="fallback_feed", run_date=date(2026, 2, 16))
    assert feed.source == "fallback_feed"
    assert feed.traders == []
    assert feed.activity == []


def test_parse_raw_csv_payload_builds_trader_and_activity_rows() -> None:
    raw = "\n".join(
        [
            "record_type,source,snapshot_date,platform,account_address,display_name,entity_type,roi_pct,pnl_usd,volume_usd,win_rate_pct,impressiveness_score,event_time,follower_alias,leader_account_address,market_ticker,side,contracts",
            "trader,api_source,2026-02-16,KALSHI,0xabc,StormPilot,bot,11.2,500,1200,59.5,12.0,,,,,,",
            "activity,api_source,2026-02-16,,,,,,,,,,2026-02-16T10:00:00Z,CopyCat,0xabc,KXLOWTLAX-26FEB16-B53.5,yes,3",
        ]
    )
    payload = _parse_raw_feed_payload(
        raw=raw,
        feed_format="csv",
        default_source="fallback_feed",
        run_date=date(2026, 2, 16),
    )

    feed = _parse_feed_payload(payload=payload, default_source="fallback_feed", run_date=date(2026, 2, 16))
    assert feed.source == "api_source"
    assert len(feed.traders) == 1
    assert feed.traders[0].account_address == "0xabc"
    assert len(feed.activity) == 1
    assert feed.activity[0].leader_account_address == "0xabc"
