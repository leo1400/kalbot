CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS markets (
  id BIGSERIAL PRIMARY KEY,
  kalshi_market_id TEXT UNIQUE NOT NULL,
  event_ticker TEXT NOT NULL,
  market_ticker TEXT NOT NULL,
  title TEXT NOT NULL,
  close_time TIMESTAMPTZ,
  settle_time TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS market_snapshots (
  id BIGSERIAL PRIMARY KEY,
  market_id BIGINT NOT NULL REFERENCES markets(id),
  bid_yes NUMERIC(6, 4),
  ask_yes NUMERIC(6, 4),
  last_price_yes NUMERIC(6, 4),
  volume BIGINT,
  captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS weather_observations (
  id BIGSERIAL PRIMARY KEY,
  station_id TEXT NOT NULL,
  observed_at TIMESTAMPTZ NOT NULL,
  metric TEXT NOT NULL,
  value NUMERIC(10, 3) NOT NULL,
  unit TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(station_id, observed_at, metric)
);

CREATE TABLE IF NOT EXISTS weather_forecasts (
  id BIGSERIAL PRIMARY KEY,
  source TEXT NOT NULL,
  station_id TEXT NOT NULL,
  issued_at TIMESTAMPTZ NOT NULL,
  valid_at TIMESTAMPTZ NOT NULL,
  metric TEXT NOT NULL,
  value NUMERIC(10, 3) NOT NULL,
  unit TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(source, station_id, issued_at, valid_at, metric)
);

CREATE TABLE IF NOT EXISTS model_runs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  model_name TEXT NOT NULL,
  run_type TEXT NOT NULL,
  training_start TIMESTAMPTZ,
  training_end TIMESTAMPTZ,
  validation_score NUMERIC(10, 6),
  calibration_error NUMERIC(10, 6),
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS predictions (
  id BIGSERIAL PRIMARY KEY,
  model_run_id UUID NOT NULL REFERENCES model_runs(id),
  market_id BIGINT NOT NULL REFERENCES markets(id),
  prob_yes NUMERIC(6, 4) NOT NULL,
  ci_low NUMERIC(6, 4),
  ci_high NUMERIC(6, 4),
  predicted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(model_run_id, market_id)
);

CREATE TABLE IF NOT EXISTS trade_decisions (
  id BIGSERIAL PRIMARY KEY,
  prediction_id BIGINT NOT NULL REFERENCES predictions(id),
  edge NUMERIC(6, 4) NOT NULL,
  threshold NUMERIC(6, 4) NOT NULL,
  approved BOOLEAN NOT NULL,
  reason TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS orders (
  id BIGSERIAL PRIMARY KEY,
  decision_id BIGINT NOT NULL REFERENCES trade_decisions(id),
  execution_mode TEXT NOT NULL,
  side TEXT NOT NULL,
  contracts INTEGER NOT NULL,
  limit_price NUMERIC(6, 4),
  status TEXT NOT NULL,
  external_order_id TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS positions (
  id BIGSERIAL PRIMARY KEY,
  market_id BIGINT NOT NULL REFERENCES markets(id),
  execution_mode TEXT NOT NULL,
  side TEXT NOT NULL,
  entry_price NUMERIC(6, 4) NOT NULL,
  contracts INTEGER NOT NULL,
  opened_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  closed_at TIMESTAMPTZ,
  realized_pnl NUMERIC(12, 2),
  status TEXT NOT NULL DEFAULT 'open'
);

CREATE TABLE IF NOT EXISTS settlements (
  id BIGSERIAL PRIMARY KEY,
  market_id BIGINT NOT NULL REFERENCES markets(id),
  settled_yes BOOLEAN NOT NULL,
  settled_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(market_id)
);

CREATE TABLE IF NOT EXISTS daily_metrics (
  id BIGSERIAL PRIMARY KEY,
  metric_date DATE NOT NULL,
  execution_mode TEXT NOT NULL,
  brier_score NUMERIC(10, 6),
  log_loss NUMERIC(10, 6),
  calibration_error NUMERIC(10, 6),
  gross_pnl NUMERIC(12, 2),
  net_pnl NUMERIC(12, 2),
  max_drawdown NUMERIC(12, 2),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(metric_date, execution_mode)
);

CREATE TABLE IF NOT EXISTS published_signals (
  id BIGSERIAL PRIMARY KEY,
  market_id BIGINT NOT NULL REFERENCES markets(id),
  model_run_id UUID NOT NULL REFERENCES model_runs(id),
  confidence NUMERIC(6, 4) NOT NULL,
  rationale TEXT NOT NULL,
  data_source_url TEXT NOT NULL,
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  published_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_market_snapshots_market_id_captured_at
  ON market_snapshots (market_id, captured_at DESC);

CREATE INDEX IF NOT EXISTS idx_predictions_market_id_predicted_at
  ON predictions (market_id, predicted_at DESC);

CREATE INDEX IF NOT EXISTS idx_orders_status_created_at
  ON orders (status, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_positions_status_opened_at
  ON positions (status, opened_at DESC);
