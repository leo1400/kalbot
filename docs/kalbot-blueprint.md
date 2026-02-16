# Kalbot Blueprint (v0.1)

## Goal

Build a transparent Kalshi weather trading system that:

- predicts event probabilities better than market consensus,
- retrains and recalibrates daily,
- tracks performance rigorously,
- starts in paper mode and graduates to live execution with strict risk limits.

## Product shape (replicating the idea)

Kalbot should copy the useful product pattern we observed:

- public transparency dashboard (current positions, confidence, rationale),
- data-source links for each thesis,
- historical performance log,
- admin/ops controls for publishing and overrides.

But Kalbot should add a real model-and-execution backbone from day one.

## System architecture

## 1. Data layer

- Market data (Kalshi):
  - event metadata, bids/asks, traded prices, volume, settlement outcomes.
- Weather data:
  - forecast grids and point forecasts (hourly/daily),
  - observed weather outcomes for label truth.
- Macro/context features:
  - station metadata, seasonality, region effects, update latency.

## 2. Feature + modeling layer

- Candidate model family:
  - baseline calibrated logistic model,
  - gradient-boosted model (tabular),
  - optional ensemble average with reliability weighting.
- Output:
  - `P(event settles YES)` with confidence intervals.
- Calibration:
  - isotonic or Platt scaling on rolling validation windows.

## 3. Decision layer

- Edge computation:
  - `edge = model_probability - market_implied_probability`.
- Trade filter:
  - only trade when edge exceeds fees + safety margin.
- Sizing:
  - capped fractional Kelly with hard per-market, per-day, and total exposure limits.

## 4. Execution layer

- Mode switch:
  - `paper` and `live`.
- Live execution:
  - place/cancel orders through Kalshi API adapter.
- Safety:
  - kill-switch, max daily loss, stale-data checks, spread/latency guards.

## 5. Transparency layer

- Dashboard pages:
  - active positions,
  - model signal and confidence,
  - rationale + source links,
  - realized PnL and calibration metrics.
- Audit trail:
  - every model run, score, order decision, and override is stored.

## Daily improvement loop (required)

Run every day after key settlement/observation windows:

1. Ingest newest weather observations and newly settled Kalshi markets.
2. Rebuild training dataset with rolling windows.
3. Retrain model candidates.
4. Recalibrate probabilities.
5. Backtest on recent holdout.
6. Compare champion vs challenger.
7. Promote only if improvement thresholds are met.
8. Generate fresh predictions and trading signals.
9. Publish metrics and drift alerts.

## Minimum database schema

- `markets` (Kalshi market metadata)
- `market_snapshots` (quotes/orderbook snapshots)
- `weather_observations` (truth labels)
- `weather_forecasts` (forecast inputs by source/time)
- `model_runs` (version, params, metrics)
- `predictions` (market_id, run_id, prob_yes, ci_low, ci_high)
- `trade_decisions` (edge, threshold, approved/rejected reason)
- `orders` (paper/live orders)
- `positions` (open/closed state)
- `settlements` (resolved outcomes)
- `daily_metrics` (PnL, Brier, log-loss, calibration)
- `published_signals` (frontend transparency cards)

## Phased build plan

## Phase 1: Foundation (now)

- Create data schema and ingestion jobs.
- Build paper-trading decision engine.
- Build dashboard with manual publish controls.

Exit criteria:
- daily pipeline runs end-to-end in paper mode.

## Phase 2: Model hardening

- Add challenger/champion training workflow.
- Add calibration and drift monitoring.
- Add robust backtesting reports.

Exit criteria:
- stable paper performance over a full evaluation window.

## Phase 3: Controlled live trading

- Enable live order adapter with low caps.
- Enforce risk controls and kill-switch.
- Increase size only after metrics gates pass.

Exit criteria:
- risk-adjusted live performance acceptable for sustained operation.

## Success metrics

- Forecast quality:
  - Brier score
  - log-loss
  - calibration error
- Trading quality:
  - net PnL after fees
  - Sharpe-like risk-adjusted return
  - drawdown
  - hit rate by edge bucket
- Operations quality:
  - daily run reliability
  - data freshness SLA
  - model promotion discipline

## First implementation tasks

1. Set up project skeleton (`backend`, `workers`, `frontend`, `infra`).
2. Implement Kalshi market ingestion and storage.
3. Implement weather ingestion and outcome labeling.
4. Implement baseline probability model + calibration.
5. Implement paper execution simulator.
6. Implement dashboard for active signals and historical performance.
