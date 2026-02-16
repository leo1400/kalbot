import React, { useEffect, useMemo, useState } from "react";

const API_BASE = import.meta.env.VITE_API_BASE ?? "http://localhost:8000";
const EDGE_APPROVE_THRESHOLD = 0.03;

function toPercent(value, digits = 1) {
  return `${(value * 100).toFixed(digits)}%`;
}

function toUsd(value) {
  return `$${Number(value).toLocaleString(undefined, {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  })}`;
}

function toLocalTime(value) {
  if (!value) {
    return "n/a";
  }
  const ts = new Date(value);
  return Number.isNaN(ts.getTime()) ? "n/a" : ts.toLocaleString();
}

function edgeTone(edge) {
  if (edge >= EDGE_APPROVE_THRESHOLD) {
    return { label: "Lean YES", cls: "pos" };
  }
  if (edge <= -EDGE_APPROVE_THRESHOLD) {
    return { label: "Lean NO", cls: "neg" };
  }
  return { label: "No edge", cls: "flat" };
}

async function fetchJson(url) {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`Failed request: ${url}`);
  }
  return response.json();
}

export function App() {
  const [health, setHealth] = useState(null);
  const [summary, setSummary] = useState(null);
  const [signals, setSignals] = useState([]);
  const [leaderboard, setLeaderboard] = useState([]);
  const [activity, setActivity] = useState([]);
  const [sort, setSort] = useState("impressiveness");
  const [window, setWindow] = useState("all");
  const [error, setError] = useState("");

  useEffect(() => {
    async function load() {
      setError("");
      const requests = await Promise.allSettled([
        fetchJson(`${API_BASE}/health`),
        fetchJson(`${API_BASE}/v1/dashboard/summary`),
        fetchJson(`${API_BASE}/v1/signals/current`),
        fetchJson(
          `${API_BASE}/v1/intel/leaderboard?sort=${encodeURIComponent(sort)}&window=${encodeURIComponent(window)}&limit=10`
        ),
        fetchJson(`${API_BASE}/v1/intel/activity?limit=12`),
      ]);

      if (requests[0].status === "fulfilled") {
        setHealth(requests[0].value);
      }
      if (requests[1].status === "fulfilled") {
        setSummary(requests[1].value);
      }
      if (requests[2].status === "fulfilled") {
        setSignals(requests[2].value);
      }
      if (requests[3].status === "fulfilled") {
        setLeaderboard(requests[3].value);
      }
      if (requests[4].status === "fulfilled") {
        setActivity(requests[4].value);
      }

      const hasFailure = requests.some((item) => item.status === "rejected");
      if (hasFailure) {
        setError("Some dashboard data is unavailable. Keep backend + worker running.");
      }
    }

    load();
  }, [sort, window]);

  const topTrader = useMemo(() => leaderboard[0] ?? null, [leaderboard]);

  return (
    <main className="page">
      <section className="panel hero">
        <div className="hero-head">
          <p className="kicker">Kalbot // Weather Alpha Desk</p>
          <span className={`mode-pill ${health?.execution_mode === "paper" ? "paper" : "live"}`}>
            {health?.execution_mode ?? "loading"}
          </span>
        </div>

        <h1>Weather Trading Engine</h1>
        <p className="subtitle">
          Daily-trained weather model + Kalshi market pricing + bot-intel context.
        </p>
        {error ? <p className="error">{error}</p> : null}

        <div className="kpi-grid">
          <article className="kpi-card">
            <p className="label">Active Signals</p>
            <p className="value">{summary?.active_signal_count ?? 0}</p>
          </article>
          <article className="kpi-card">
            <p className="label">Avg Confidence</p>
            <p className="value">{toPercent(summary?.avg_confidence ?? 0)}</p>
          </article>
          <article className="kpi-card">
            <p className="label">Mean Edge</p>
            <p className={`value ${(summary?.avg_edge ?? 0) >= 0 ? "up" : "down"}`}>
              {(summary?.avg_edge ?? 0) >= 0 ? "+" : ""}
              {toPercent(summary?.avg_edge ?? 0)}
            </p>
          </article>
          <article className="kpi-card">
            <p className="label">Strongest Edge</p>
            <p className="value">{toPercent(summary?.strongest_edge ?? 0)}</p>
          </article>
          <article className="kpi-card stretch">
            <p className="label">Top Bot Right Now</p>
            <p className="value small-value">{topTrader?.display_name ?? "No intel rows"}</p>
            {topTrader ? <p className="small">ROI {topTrader.roi_pct.toFixed(2)}%</p> : null}
          </article>
        </div>
      </section>

      <section className="panel">
        <div className="section-head">
          <h2>Current Signals</h2>
          <p className="small">Last publish: {toLocalTime(summary?.updated_at_utc)}</p>
        </div>

        <div className="signal-grid">
          {signals.map((signal) => {
            const tone = edgeTone(signal.edge);
            const confidencePct = Math.max(0, Math.min(100, signal.confidence * 100));
            return (
              <article className="signal-card" key={signal.market_ticker}>
                <div className="signal-head">
                  <p className="title">{signal.title}</p>
                  <span className={`edge-pill ${tone.cls}`}>{tone.label}</span>
                </div>

                <div className="metric-grid">
                  <div>
                    <p className="label">Model YES</p>
                    <p className="metric">{toPercent(signal.probability_yes)}</p>
                  </div>
                  <div>
                    <p className="label">Market YES</p>
                    <p className="metric">{toPercent(signal.market_implied_yes)}</p>
                  </div>
                  <div>
                    <p className="label">Edge</p>
                    <p className={`metric ${signal.edge >= 0 ? "up" : "down"}`}>
                      {signal.edge >= 0 ? "+" : ""}
                      {toPercent(signal.edge)}
                    </p>
                  </div>
                </div>

                <div className="confidence-row">
                  <span className="small">Confidence {toPercent(signal.confidence)}</span>
                  <div className="confidence-track">
                    <span className="confidence-fill" style={{ width: `${confidencePct}%` }} />
                  </div>
                </div>

                <p className="small rationale">{signal.rationale}</p>
                <p className="small">
                  Source:{" "}
                  <a href={signal.data_source_url} target="_blank" rel="noreferrer">
                    {signal.data_source_url}
                  </a>
                </p>
              </article>
            );
          })}
        </div>

        {signals.length === 0 && !error ? <p className="small">No active signals yet.</p> : null}
      </section>

      <section className="intel-layout">
        <section className="panel">
          <div className="section-head">
            <h2>Bot Intel Leaderboard</h2>
            <div className="filters">
              <label>
                Sort
                <select value={sort} onChange={(event) => setSort(event.target.value)}>
                  <option value="impressiveness">Most Impressive</option>
                  <option value="roi">Highest ROI</option>
                  <option value="pnl">Highest PnL</option>
                  <option value="volume">Highest Volume</option>
                </select>
              </label>
              <label>
                Window
                <select value={window} onChange={(event) => setWindow(event.target.value)}>
                  <option value="all">All</option>
                  <option value="1m">1M</option>
                  <option value="1w">1W</option>
                  <option value="1d">1D</option>
                </select>
              </label>
            </div>
          </div>

          <p className="small">
            Bastion-style intel: rank by performance, then use this leaderboard as a secondary decision input.
          </p>

          <div className="table-wrap">
            <table className="table">
              <thead>
                <tr>
                  <th>Rank</th>
                  <th>Name</th>
                  <th>Type</th>
                  <th>ROI</th>
                  <th>PnL</th>
                  <th>Volume</th>
                </tr>
              </thead>
              <tbody>
                {leaderboard.map((entry) => (
                  <tr key={`${entry.platform}-${entry.account_address}`}>
                    <td>#{entry.rank}</td>
                    <td>
                      {entry.display_name}
                      <div className="small mono">{entry.account_address}</div>
                    </td>
                    <td>{entry.entity_type}</td>
                    <td>{entry.roi_pct.toFixed(2)}%</td>
                    <td>{toUsd(entry.pnl_usd)}</td>
                    <td>{toUsd(entry.volume_usd)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
            {leaderboard.length === 0 && !error ? (
              <p className="small">No leaderboard rows yet. Run `scripts\\run-daily.cmd`.</p>
            ) : null}
          </div>
        </section>

        <section className="panel activity-panel">
          <h2>Copy Activity Tape</h2>
          <p className="small">Recent follow events from tracked leaders.</p>

          <div className="activity-list">
            {activity.map((event, index) => (
              <article className="activity-item" key={`${event.event_time}-${event.market_ticker}-${index}`}>
                <p className="activity-line">
                  <span className="mono">{event.follower_alias}</span> copied{" "}
                  <strong>{event.leader_display_name}</strong>
                </p>
                <p className="small mono">{event.market_ticker}</p>
                <p className="small">
                  {event.side.toUpperCase()} x{event.contracts}
                </p>
                <p className={`activity-pnl ${event.pnl_usd >= 0 ? "up" : "down"}`}>
                  {event.pnl_usd >= 0 ? "+" : ""}
                  {toUsd(event.pnl_usd)}
                </p>
                <p className="small">{toLocalTime(event.event_time)}</p>
              </article>
            ))}

            {activity.length === 0 && !error ? (
              <p className="small">No activity yet. The worker will seed starter events.</p>
            ) : null}
          </div>
        </section>
      </section>
    </main>
  );
}
