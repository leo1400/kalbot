from kalbot.provenance_repo import _city_coverage_status, _fresh_status


def test_fresh_status_thresholds() -> None:
    assert _fresh_status(5.0, good_max=10.0, degraded_max=30.0) == "good"
    assert _fresh_status(20.0, good_max=10.0, degraded_max=30.0) == "degraded"
    assert _fresh_status(40.0, good_max=10.0, degraded_max=30.0) == "stale"


def test_city_coverage_status_variants() -> None:
    assert _city_coverage_status(snapshot_age=5.0, forecast_age=60.0) == "model_ready"
    assert _city_coverage_status(snapshot_age=5.0, forecast_age=None) == "market_only"
    assert _city_coverage_status(snapshot_age=120.0, forecast_age=60.0) == "stale_market"
