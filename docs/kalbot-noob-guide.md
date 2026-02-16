# Kalbot Noob Guide

This page explains Kalbot in plain English.

## What Kalbot does

1. Pulls weather forecasts/observations from NOAA/NWS.
2. Pulls weather market prices from Kalshi.
3. Compares model probability vs market probability.
4. Ranks the best opportunities by edge and confidence.
5. Publishes signals and a simple paper-trading playbook.

## What is real vs demo right now

- Real:
  - NWS weather data (`weather.gov`).
  - Kalshi market price/volume snapshots for weather markets.
  - Kalbot's own paper orders/positions.
- Demo:
  - Bot intel leaderboard/copy feed is seeded demo data unless replaced by a real collector.

## Fast way to follow (paper mode)

1. Run `scripts\run-daily.cmd`.
2. Open `http://localhost:5173`.
3. Check `Data Status` first:
   - `GOOD`: okay to trust signals more.
   - `DEGRADED`: lower confidence.
   - `STALE`: skip trading until data refreshes.
4. Go to `How To Follow Today (Paper)`:
   - `Lean YES` means buy YES contracts.
   - `Lean NO` means buy NO contracts.
   - `Pass` means skip.
5. Keep size small:
   - Start with paper sizing shown in the playbook.
   - Do not increase size until you have many days of stable performance.

## Why all signals can come from one city

If one city has both fresh Kalshi snapshots and fresh weather forecasts, while others only have market data, the model may publish mostly from that city. Use the `Source Integrity` city table to verify which cities are `model_ready` vs `market_only`.

## Terms

- `YES share`: pays $1 if event happens.
- `NO share`: pays $1 if event does not happen.
- `Model YES`: model probability of event happening.
- `Market YES`: implied probability from market price.
- `Edge`: model minus market.
- `Confidence`: model trust score for this signal.
- `Notional`: dollars at risk.
- `PnL`: profit/loss.
- `Pass`: no trade signal.

## What “good enough” looks like

- Forecast freshness: ideally <= 60 minutes old.
- Market snapshot freshness: ideally <= 10 minutes old.
- Station coverage: ideally close to all configured stations.
- Enough sample size: do not judge strategy from 1-2 days.

## Risk reminder

This is an experimental system. Use paper mode first. Never risk money you cannot afford to lose.
