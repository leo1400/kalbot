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
   - `psql "postgresql://postgres:postgres@localhost:5432/kalbot" -f infra/migrations/001_initial_schema.sql`
   - `psql "postgresql://postgres:postgres@localhost:5432/kalbot" -f infra/migrations/002_bot_intel.sql`
6. Start backend:
   - `./scripts/start-backend.ps1`
7. Run daily pipeline stub:
   - `./scripts/run-daily.ps1`
8. Start frontend:
   - `./scripts/start-frontend.ps1`

## Live local view

- Frontend dashboard: `http://localhost:5173`
- API docs: `http://localhost:8000/docs`
- Current signals endpoint: `http://localhost:8000/v1/signals/current`
- Bot intel leaderboard: `http://localhost:8000/v1/intel/leaderboard?sort=impressiveness&window=all&limit=10`

Daily run summaries are written to `artifacts/daily/<YYYY-MM-DD>/run-summary.json`.
