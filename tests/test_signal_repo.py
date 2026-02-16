from kalbot.signals_repo import (
    _condition_probability,
    _extract_low_temp_city_code,
    _extract_temperature_threshold,
    _parse_low_temp_condition,
)


def test_extract_temperature_threshold_from_ticker() -> None:
    value = _extract_temperature_threshold("KXLOWTNYC-26FEB17-T35")
    assert value == 35.0


def test_extract_temperature_threshold_returns_none_for_nonmatch() -> None:
    assert _extract_temperature_threshold("KXLOWTNYC-26FEB17") is None


def test_extract_low_temp_city_code() -> None:
    assert _extract_low_temp_city_code("KXLOWTLAX-26FEB17-T51") == "LAX"


def test_parse_low_temp_condition_range() -> None:
    c = _parse_low_temp_condition(
        "Will the minimum temperature be 50-51\u00B0 on Feb 17, 2026?"
    )
    assert c is not None
    assert c["kind"] == "range"
    assert c["low"] == 50.0
    assert c["high"] == 51.0


def test_parse_low_temp_condition_gt_with_proper_degree_symbol() -> None:
    c = _parse_low_temp_condition(
        "Will the minimum temperature be >54\u00B0 on Feb 17, 2026?"
    )
    assert c is not None
    assert c["kind"] == "gt"
    assert c["low"] == 54.0


def test_condition_probability_gt_is_small_if_mu_below_threshold() -> None:
    c = {"kind": "gt", "low": 54.0, "high": None}
    p = _condition_probability(c, mu_f=50.0, sigma_f=2.0)
    assert p < 0.1
