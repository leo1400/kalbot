from kalbot.kalshi_ingest import _price_as_float


def test_price_as_float_prefers_dollars_field() -> None:
    market = {"yes_bid_dollars": "0.5400", "yes_bid": 54}
    assert _price_as_float(market, "yes_bid_dollars", "yes_bid") == 0.54


def test_price_as_float_falls_back_to_cents() -> None:
    market = {"yes_bid": 61}
    assert _price_as_float(market, "yes_bid_dollars", "yes_bid") == 0.61
