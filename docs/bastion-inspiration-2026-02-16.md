# Bastion-Inspired Additions (2026-02-16)

## What was observed on bastionai.app

From the public page content on 2026-02-16:

- Leaderboard-centric UX for trader/bot discovery.
- Ranking modes based on ROI, PnL, and volume.
- Search/filter framing around wallet identities and entity type.
- "Copy/mirror" positioning and activity feed language.
- Explicit ranking explanation ("How We Rank") and source/refresh messaging.

## What was added to Kalbot

- Bot Intel data model:
  - `tracked_traders`
  - `trader_performance_snapshots`
  - `copy_activity_events`
- API endpoint:
  - `GET /v1/intel/leaderboard`
  - supports `sort` (`impressiveness|roi|pnl|volume`) and `window` (`all|1m|1w|1d`)
- Worker pipeline step:
  - `update_bot_intel` seeds demo rows each daily run (placeholder for real connectors).
- Frontend:
  - Added `Bot Intel Leaderboard` section with sort/window controls and ranking table.

## Notes for next iteration

- Replace demo seeding with real Kalshi identity/performance ingestion.
- Add search by wallet/name.
- Add copy-follow simulation and alerts from `copy_activity_events`.
