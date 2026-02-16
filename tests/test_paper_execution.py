from kalbot.paper_execution import _contracts_for_notional, _edge_to_order


def test_contracts_for_notional_caps_at_max_contracts() -> None:
    contracts = _contracts_for_notional(entry_price=0.32, max_notional=300.0, max_contracts=12)
    assert contracts == 12


def test_contracts_for_notional_returns_zero_for_invalid_inputs() -> None:
    assert _contracts_for_notional(entry_price=0.0, max_notional=50.0, max_contracts=5) == 0
    assert _contracts_for_notional(entry_price=0.2, max_notional=0.0, max_contracts=5) == 0


def test_edge_to_order_maps_positive_edge_to_yes_side() -> None:
    side, price = _edge_to_order(edge=0.11, market_yes=0.41)
    assert side == "yes"
    assert price == 0.41


def test_edge_to_order_maps_negative_edge_to_no_side() -> None:
    side, price = _edge_to_order(edge=-0.07, market_yes=0.41)
    assert side == "no"
    assert round(price, 2) == 0.59
