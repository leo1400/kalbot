# kalbot

Kalshi prediction and execution system focused on weather markets.

## Current status (February 16, 2026)

Blueprint and reverse-engineering notes are documented in:

- `docs/purplesalmon-analysis-2026-02-16.md`
- `docs/kalbot-blueprint.md`
- `docs/bastion-inspiration-2026-02-16.md`

## Immediate objective

Build a transparent, data-driven weather trading engine with daily retraining, paper trading first, then controlled live execution.

## Project layout

- `backend/` FastAPI service (health + signal endpoints).
- `workers/` Daily pipeline runner for ingestion/training/decision/publish workflow.
- `kalbot/` Shared config and schema models.
- `infra/` Docker and SQL migrations.
- `frontend/` React dashboard starter.
- `scripts/` PowerShell helpers for local runs.

## Quick start

1. Create and activate a virtual environment.
2. Install Python dependencies from `pyproject.toml`.
3. Copy `.env.example` to `.env` and set values.
4. Start Postgres:
   - `docker compose up -d`
   - or `docker compose -f infra/docker-compose.yml up -d`
5. Apply migration:
   - `scripts\apply-migrations.cmd`
   - or `powershell -ExecutionPolicy Bypass -File .\scripts\apply-migrations.ps1`
   - Optional (if local `psql` is installed):
     - `psql "postgresql://postgres:postgres@localhost:5432/kalbot" -f infra/migrations/001_initial_schema.sql`
     - `psql "postgresql://postgres:postgres@localhost:5432/kalbot" -f infra/migrations/002_bot_intel.sql`
6. Start backend:
   - `scripts\start-backend.cmd`
7. Run daily pipeline stub:
   - `scripts\run-daily.cmd`
8. Start frontend:
   - `scripts\start-frontend.cmd`
   - If `npm` is not installed, this script auto-starts frontend in Docker.

## Live local view

- Frontend dashboard: `http://localhost:5173`
- API docs: `http://localhost:8000/docs`
- Current signals endpoint: `http://localhost:8000/v1/signals/current`
- Signal playbook endpoint: `http://localhost:8000/v1/signals/playbook?limit=6`
- Dashboard summary endpoint: `http://localhost:8000/v1/dashboard/summary`
- Bot intel leaderboard: `http://localhost:8000/v1/intel/leaderboard?sort=impressiveness&window=all&limit=10`
- Copy activity endpoint: `http://localhost:8000/v1/intel/activity?limit=12`
- Performance summary endpoint: `http://localhost:8000/v1/performance/summary`
- Performance history endpoint: `http://localhost:8000/v1/performance/history?days=14`
- Performance orders endpoint: `http://localhost:8000/v1/performance/orders?limit=12`
- Data quality endpoint: `http://localhost:8000/v1/data/quality`

## Beginner guide

- `docs/kalbot-noob-guide.md` explains the dashboard and terms in plain language, plus a safe paper-trading follow routine.

## No npm on Windows

If you don't have Node/npm installed, use:

- `scripts\start-frontend.cmd`

The script will run the frontend in Docker via profile `ui`.

Daily run summaries are written to `artifacts/daily/<YYYY-MM-DD>/run-summary.json`.

## Weather ingestion

- Daily worker now ingests live NWS weather data into:
  - `weather_forecasts`
  - `weather_observations`
- Configure targets with:
  - `KALBOT_WEATHER_TARGETS=nyc:40.7128,-74.0060;chi:41.8781,-87.6298;mia:25.7617,-80.1918`

## Kalshi ingestion

- Daily worker now ingests live Kalshi weather-category markets into:
  - `markets`
  - `market_snapshots`
- Signal publishing now attempts a live NYC low-temperature heuristic using:
  - Kalshi `KXLOWT*` market prices (city-derived)
  - NWS forecast temperatures for corresponding station candidates (`K<city>`, `<city>`)
- Active signal publishing now ranks multiple low-temp candidates by edge, forecast coverage, and liquidity, then publishes a top set each run.
- Configure category/limits with:
  - `KALBOT_KALSHI_WEATHER_CATEGORY`
  - `KALBOT_KALSHI_WEATHER_SERIES_LIMIT`
  - `KALBOT_KALSHI_MARKETS_PER_SERIES`

## Baseline model loop

- `build_features` step writes low-temp training examples to:
  - `artifacts/features/<date>/low_temp_training_examples.json`
- `train_and_calibrate` step trains a baseline low-temp uncertainty model and writes:
  - `artifacts/models/low_temp_model_latest.json`
- Live signal publishing uses this model (with market-title condition parsing) before falling back.

## Paper execution loop

- `simulate_execution` now places paper orders from active signals using risk caps:
  - `KALBOT_PAPER_EDGE_THRESHOLD`
  - `KALBOT_MAX_NOTIONAL_PER_SIGNAL_USD`
  - `KALBOT_MAX_DAILY_NOTIONAL_USD`
  - `KALBOT_MAX_CONTRACTS_PER_ORDER`
- Orders and positions are written to:
  - `orders`
  - `positions`
