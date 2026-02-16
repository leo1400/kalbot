from kalbot.weather_ingest import parse_weather_targets


def test_parse_weather_targets_parses_valid_items() -> None:
    targets = parse_weather_targets("nyc:40.7,-74.0;chi:41.8,-87.6")
    assert len(targets) == 2
    assert targets[0].name == "nyc"
    assert targets[1].longitude == -87.6


def test_parse_weather_targets_skips_invalid_items() -> None:
    targets = parse_weather_targets("bad_item;mia:25.7,-80.1")
    assert len(targets) == 1
    assert targets[0].name == "mia"
