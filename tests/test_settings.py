from kalbot.settings import Settings


def test_default_execution_mode_is_paper() -> None:
    settings = Settings()
    assert settings.execution_mode == "paper"


def test_default_model_name_is_set() -> None:
    settings = Settings()
    assert settings.model_name == "baseline-logit-v1"


def test_default_signal_publish_limit() -> None:
    settings = Settings()
    assert settings.signal_publish_limit == 4


def test_default_paper_risk_caps() -> None:
    settings = Settings()
    assert settings.paper_edge_threshold == 0.03
    assert settings.max_notional_per_signal_usd == 125.0
    assert settings.max_daily_notional_usd == 500.0
    assert settings.max_contracts_per_order == 25
