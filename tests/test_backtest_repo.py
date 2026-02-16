from kalbot.backtest_repo import _brier, _clip, _log_loss


def test_clip_bounds_probability() -> None:
    assert _clip(-1.0) == 0.000001
    assert _clip(2.0) == 0.999999
    assert _clip(0.42) == 0.42


def test_brier_score_math() -> None:
    assert round(_brier(0.8, 1.0), 6) == 0.04
    assert round(_brier(0.2, 0.0), 6) == 0.04


def test_log_loss_prefers_confident_correct_predictions() -> None:
    good = _log_loss(0.9, 1.0)
    bad = _log_loss(0.6, 1.0)
    assert good < bad
