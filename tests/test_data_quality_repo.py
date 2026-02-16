from kalbot.data_quality_repo import _freshness_component, _ratio


def test_ratio_clamps_to_valid_range() -> None:
    assert _ratio(5, 10) == 0.5
    assert _ratio(12, 10) == 1.0
    assert _ratio(-2, 10) == 0.0


def test_freshness_component_returns_zero_when_missing() -> None:
    assert _freshness_component(None, 60.0) == 0.0


def test_freshness_component_decays_with_age() -> None:
    assert _freshness_component(0.0, 60.0) == 1.0
    assert _freshness_component(30.0, 60.0) == 0.5
    assert _freshness_component(90.0, 60.0) == 0.0
