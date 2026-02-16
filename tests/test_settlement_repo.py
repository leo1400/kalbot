from datetime import datetime, timezone

from kalbot.settlement_repo import _market_result_to_bool, _market_settled_at


def test_market_result_to_bool_handles_yes_no() -> None:
    assert _market_result_to_bool("yes") is True
    assert _market_result_to_bool("no") is False
    assert _market_result_to_bool("pending") is None


def test_market_settled_at_prefers_settlement_ts() -> None:
    payload = {
        "settlement_ts": "2026-02-16T12:01:02Z",
        "expiration_time": "2026-02-16T13:00:00Z",
        "close_time": "2026-02-16T11:00:00Z",
    }
    settled_at = _market_settled_at(payload)
    assert settled_at == datetime(2026, 2, 16, 12, 1, 2, tzinfo=timezone.utc)


def test_market_settled_at_falls_back_to_expiration_time() -> None:
    payload = {
        "settlement_ts": "",
        "expiration_time": "2026-02-16T13:00:00Z",
    }
    settled_at = _market_settled_at(payload)
    assert settled_at == datetime(2026, 2, 16, 13, 0, 0, tzinfo=timezone.utc)
