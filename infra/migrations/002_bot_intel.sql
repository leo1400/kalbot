CREATE TABLE IF NOT EXISTS tracked_traders (
  id BIGSERIAL PRIMARY KEY,
  platform TEXT NOT NULL,
  account_address TEXT NOT NULL,
  display_name TEXT NOT NULL,
  entity_type TEXT NOT NULL DEFAULT 'bot',
  source TEXT NOT NULL DEFAULT 'manual',
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(platform, account_address)
);

CREATE TABLE IF NOT EXISTS trader_performance_snapshots (
  id BIGSERIAL PRIMARY KEY,
  trader_id BIGINT NOT NULL REFERENCES tracked_traders(id),
  snapshot_date DATE NOT NULL,
  window TEXT NOT NULL,
  roi_pct NUMERIC(12, 4) NOT NULL,
  pnl_usd NUMERIC(14, 2) NOT NULL,
  volume_usd NUMERIC(14, 2) NOT NULL,
  win_rate_pct NUMERIC(8, 4),
  impressiveness_score NUMERIC(12, 4) NOT NULL,
  source TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(trader_id, snapshot_date, window, source)
);

CREATE TABLE IF NOT EXISTS copy_activity_events (
  id BIGSERIAL PRIMARY KEY,
  event_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  follower_alias TEXT NOT NULL,
  leader_trader_id BIGINT NOT NULL REFERENCES tracked_traders(id),
  market_ticker TEXT NOT NULL,
  side TEXT NOT NULL,
  contracts INTEGER NOT NULL,
  pnl_usd NUMERIC(12, 2) NOT NULL,
  source TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_trader_perf_window_score
  ON trader_performance_snapshots(window, impressiveness_score DESC);

CREATE INDEX IF NOT EXISTS idx_copy_events_event_time
  ON copy_activity_events(event_time DESC);
