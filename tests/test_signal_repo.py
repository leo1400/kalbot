from kalbot.signals_repo import _extract_low_temp_city_code, _extract_temperature_threshold


def test_extract_temperature_threshold_from_ticker() -> None:
    value = _extract_temperature_threshold("KXLOWTNYC-26FEB17-T35")
    assert value == 35.0


def test_extract_temperature_threshold_returns_none_for_nonmatch() -> None:
    assert _extract_temperature_threshold("KXLOWTNYC-26FEB17") is None


def test_extract_low_temp_city_code() -> None:
    assert _extract_low_temp_city_code("KXLOWTLAX-26FEB17-T51") == "LAX"
