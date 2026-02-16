import React, { useEffect, useMemo, useState } from "react";

const API_BASE = import.meta.env.VITE_API_BASE ?? "http://localhost:8000";
const EDGE_APPROVE_THRESHOLD = 0.03;
const NOOB_TERMS = [
  { term: "YES Share", definition: "A contract that pays $1 if the event happens." },
  { term: "NO Share", definition: "A contract that pays $1 if the event does not happen." },
  { term: "Model YES", definition: "What Kalbot thinks the chance is (in %)." },
  { term: "Market YES", definition: "What current market prices imply (in %)." },
  { term: "Edge", definition: "Model minus market. Bigger absolute edge means stronger disagreement." },
  { term: "Confidence", definition: "How trustworthy this model estimate is right now." },
  { term: "Notional", definition: "Total dollars at risk (price x contracts)." },
  { term: "PnL", definition: "Profit and loss. Positive means winning, negative means losing." },
  { term: "Pass", definition: "No trade. Edge/confidence is too weak." },
];

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

function toAge(value) {
  if (value === null || value === undefined) {
    return "n/a";
  }
  const rounded = Math.round(value);
  return `${rounded}m`;
}

function edgeTone(edge) {
  if (edge >= EDGE_APPROVE_THRESHOLD) {
    return { label: "Lean YES", cls: "pos" };
  }
  if (edge <= -EDGE_APPROVE_THRESHOLD) {
    return { label: "Lean NO", cls: "neg" };
  }
  return { label: "No Edge", cls: "flat" };
}

function actionLabel(action) {
  if (action === "lean_yes") {
    return "Lean YES";
  }
  if (action === "lean_no") {
    return "Lean NO";
  }
  return "Pass";
}

function statusClass(status) {
  if (status === "good" || status === "model_ready") {
    return "status-good";
  }
  if (status === "degraded") {
    return "status-degraded";
  }
  return "status-stale";
}

function modeClass(mode) {
  if (mode === "real") {
    return "status-good";
  }
  if (mode === "demo") {
    return "status-degraded";
  }
  return "status-stale";
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
  const [playbook, setPlaybook] = useState([]);
  const [leaderboard, setLeaderboard] = useState([]);
  const [activity, setActivity] = useState([]);
  const [quality, setQuality] = useState(null);
  const [provenance, setProvenance] = useState(null);
  const [performance, setPerformance] = useState(null);
  const [history, setHistory] = useState([]);
  const [orders, setOrders] = useState([]);
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
        fetchJson(`${API_BASE}/v1/signals/playbook?limit=6`),
        fetchJson(
          `${API_BASE}/v1/intel/leaderboard?sort=${encodeURIComponent(sort)}&window=${encodeURIComponent(window)}&limit=10`
        ),
        fetchJson(`${API_BASE}/v1/intel/activity?limit=8`),
        fetchJson(`${API_BASE}/v1/data/quality`),
        fetchJson(`${API_BASE}/v1/data/provenance`),
        fetchJson(`${API_BASE}/v1/performance/summary`),
        fetchJson(`${API_BASE}/v1/performance/history?days=14`),
        fetchJson(`${API_BASE}/v1/performance/orders?limit=8`),
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
        setPlaybook(requests[3].value);
      }
      if (requests[4].status === "fulfilled") {
        setLeaderboard(requests[4].value);
      }
      if (requests[5].status === "fulfilled") {
        setActivity(requests[5].value);
      }
      if (requests[6].status === "fulfilled") {
        setQuality(requests[6].value);
      }
      if (requests[7].status === "fulfilled") {
        setProvenance(requests[7].value);
      }
      if (requests[8].status === "fulfilled") {
        setPerformance(requests[8].value);
      }
      if (requests[9].status === "fulfilled") {
        setHistory(requests[9].value);
      }
      if (requests[10].status === "fulfilled") {
        setOrders(requests[10].value);
      }

      const hasFailure = requests.some((item) => item.status === "rejected");
      if (hasFailure) {
        setError("Some dashboard data is unavailable. Keep backend + worker running.");
      }
    }

    load();
  }, [sort, window]);

  const topTrader = useMemo(() => leaderboard[0] ?? null, [leaderboard]);
  const qualityTone = useMemo(() => {
    const status = quality?.status ?? "stale";
    if (status === "good") {
      return "good";
    }
    if (status === "degraded") {
      return "degraded";
    }
    return "stale";
  }, [quality]);
  const historyScale = useMemo(() => {
    const max = Math.max(...history.map((row) => row.notional_usd), 1);
    return max;
  }, [history]);
  const sourceMap = useMemo(() => {
    const rows = provenance?.sources ?? [];
    return Object.fromEntries(rows.map((row) => [row.source_key, row]));
  }, [provenance]);

  return (
    <main className="page">
      <section className="hero">
        <div className="hero-top">
          <p className="kicker">Kalbot // Aegean Tape</p>
          <div className="badges">
            <span className={`mode-pill ${health?.execution_mode === "paper" ? "paper" : "live"}`}>
              {health?.execution_mode ?? "loading"}
            </span>
            <span className="mode-pill outline">Signals {summary?.active_signal_count ?? 0}</span>
          </div>
        </div>

        <h1>Weather Trading Engine</h1>
        <p className="subtitle">
          NWS ingestion, Kalshi market discovery, model ranking, and paper execution in one daily loop.
        </p>
        {error ? <p className="error">{error}</p> : null}

        <div className="hero-kpis">
          <article className="chip">
            <p className="label">Avg Confidence</p>
            <p className="value">{toPercent(summary?.avg_confidence ?? 0)}</p>
          </article>
          <article className="chip">
            <p className="label">Strongest Edge</p>
            <p className="value">{toPercent(summary?.strongest_edge ?? 0)}</p>
          </article>
          <article className="chip">
            <p className="label">Orders 24h</p>
            <p className="value">{performance?.orders_24h ?? 0}</p>
          </article>
          <article className="chip">
            <p className="label">Notional 24h</p>
            <p className="value">{toUsd(performance?.notional_24h_usd ?? 0)}</p>
          </article>
          <article className={`chip ${qualityTone}`}>
            <p className="label">Data Status</p>
            <p className="value small-value">{(quality?.status ?? "stale").toUpperCase()}</p>
            <p className="small">Quality {((quality?.quality_score ?? 0) * 100).toFixed(0)}%</p>
          </article>
          <article className="chip wide">
            <p className="label">Top Bot Intel</p>
            <p className="value small-value">{topTrader?.display_name ?? "No intel rows"}</p>
            {topTrader ? <p className="small">ROI {topTrader.roi_pct.toFixed(2)}%</p> : null}
          </article>
        </div>
        <div className="source-badges">
          <span className={`source-pill ${modeClass(sourceMap.weather_nws?.mode)}`}>
            Weather: {(sourceMap.weather_nws?.mode ?? "unknown").toUpperCase()}
          </span>
          <span className={`source-pill ${modeClass(sourceMap.kalshi_market_data?.mode)}`}>
            Kalshi: {(sourceMap.kalshi_market_data?.mode ?? "unknown").toUpperCase()}
          </span>
          <span className={`source-pill ${modeClass(sourceMap.bot_intel_feed?.mode)}`}>
            Bot Intel: {(sourceMap.bot_intel_feed?.mode ?? "unknown").toUpperCase()}
          </span>
        </div>
      </section>

      <section className="panel execution-panel">
        <div className="section-head">
          <h2>Execution Pulse</h2>
          <p className="small">Last signal publish: {toLocalTime(summary?.updated_at_utc)}</p>
        </div>

        <div className="execution-grid">
          <article className="metric-card">
            <p className="label">Open Positions</p>
            <p className="metric-main">{performance?.open_positions ?? 0}</p>
            <p className="small">Open Notional: {toUsd(performance?.open_notional_usd ?? 0)}</p>
          </article>
          <article className="metric-card">
            <p className="label">Approved Decisions (24h)</p>
            <p className="metric-main">{performance?.approved_decisions_24h ?? 0}</p>
            <p className="small">Total Orders: {performance?.total_orders ?? 0}</p>
          </article>
          <article className="metric-card">
            <p className="label">Realized PnL</p>
            <p className={`metric-main ${(performance?.realized_pnl_usd ?? 0) >= 0 ? "up" : "down"}`}>
              {toUsd(performance?.realized_pnl_usd ?? 0)}
            </p>
            <p className="small">Paper mode aggregate</p>
          </article>
        </div>

        <div className="history-block">
          <div className="history-head">
            <p className="label">14-day Notional Tape</p>
            <p className="small">Execution notional by day</p>
          </div>
          <div className="history-bars">
            {history.map((point) => {
              const height = Math.max(8, Math.round((point.notional_usd / historyScale) * 88));
              return (
                <div className="bar-col" key={point.day} title={`${point.day} ${toUsd(point.notional_usd)}`}>
                  <span className="bar" style={{ height: `${height}px` }} />
                  <span className="bar-label">{point.day.slice(5)}</span>
                </div>
              );
            })}
          </div>
        </div>

        <div className="orders-block">
          <div className="history-head">
            <p className="label">Recent Paper Orders</p>
            <p className="small">These are Kalbot's actual paper bets.</p>
          </div>
          <div className="orders-grid">
            {orders.map((order) => (
              <article className="order-item" key={`${order.created_at}-${order.market_ticker}-${order.side}`}>
                <p className="small mono">{order.market_ticker}</p>
                <p className="small">
                  {order.side.toUpperCase()} x{order.contracts} @ {toUsd(order.limit_price)}
                </p>
                <p className={`small ${order.edge >= 0 ? "up" : "down"}`}>
                  decision edge: {order.edge >= 0 ? "+" : ""}
                  {toPercent(order.edge)}
                </p>
                <p className="small">{toLocalTime(order.created_at)}</p>
              </article>
            ))}
            {orders.length === 0 && !error ? (
              <p className="small">No paper orders yet.</p>
            ) : null}
          </div>
        </div>
      </section>

      <section className="panel">
        <div className="section-head">
          <h2>Data Reliability</h2>
          <p className="small">Forecast + market freshness gates signal trust.</p>
        </div>
        <div className="quality-grid">
          <article className="metric-card">
            <p className="label">Stations Covered (6h)</p>
            <p className="metric-main">
              {quality?.stations_with_forecast_6h ?? 0}/{quality?.target_stations ?? 0}
            </p>
          </article>
          <article className="metric-card">
            <p className="label">Latest Forecast Age</p>
            <p className="metric-main">{toAge(quality?.latest_forecast_age_min)}</p>
            <p className="small">Observation age: {toAge(quality?.latest_observation_age_min)}</p>
          </article>
          <article className="metric-card">
            <p className="label">Latest Snapshot Age</p>
            <p className="metric-main">{toAge(quality?.latest_snapshot_age_min)}</p>
            <p className="small">Kalshi snapshot recency</p>
          </article>
          <article className="metric-card">
            <p className="label">Rows (24h)</p>
            <p className="metric-main">
              {quality?.forecast_rows_24h ?? 0}/{quality?.snapshot_rows_24h ?? 0}
            </p>
            <p className="small">forecast/snapshot writes</p>
          </article>
        </div>
      </section>

      <section className="panel">
        <div className="section-head">
          <h2>Source Integrity</h2>
          <p className="small">Shows source freshness and whether each feed is real, unavailable, or synthetic.</p>
        </div>
        <div className="source-grid">
          {(provenance?.sources ?? []).map((source) => (
            <article className="metric-card" key={source.source_key}>
              <p className="label">{source.source_key.replaceAll("_", " ")}</p>
              <p className={`metric-main ${statusClass(source.status)}`}>{source.status.toUpperCase()}</p>
              <p className="small">
                mode: <strong>{source.mode}</strong>
              </p>
              <p className="small">last event: {toLocalTime(source.last_event_utc)}</p>
              <p className="small">{source.note}</p>
            </article>
          ))}
        </div>
        <div className="table-wrap">
          <table className="table">
            <thead>
              <tr>
                <th>City</th>
                <th>Open Mkts</th>
                <th>Signal</th>
                <th>Snapshot Age</th>
                <th>Forecast Age</th>
                <th>Coverage</th>
              </tr>
            </thead>
            <tbody>
              {(provenance?.cities ?? []).map((city) => (
                <tr key={city.city_code}>
                  <td>
                    {city.city_name} <span className="small mono">({city.city_code})</span>
                  </td>
                  <td>{city.open_market_count}</td>
                  <td>{city.has_active_signal ? "yes" : "no"}</td>
                  <td>{toAge(city.latest_snapshot_age_min)}</td>
                  <td>{toAge(city.latest_forecast_age_min)}</td>
                  <td>
                    <span className={`source-pill ${statusClass(city.coverage_status)}`}>
                      {city.coverage_status}
                    </span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      <section className="panel">
        <div className="section-head">
          <h2>How To Follow Today (Paper)</h2>
          <p className="small">Simple plan from the model. Start here if you are new.</p>
        </div>
        <div className="playbook-grid">
          {playbook.map((item) => (
            <article className={`playbook-card ${item.action}`} key={item.market_ticker}>
              <div className="signal-head">
                <p className="title">{item.title}</p>
                <span className={`edge-pill ${item.action === "pass" ? "flat" : item.action === "lean_yes" ? "pos" : "neg"}`}>
                  {actionLabel(item.action)}
                </span>
              </div>
              <p className="small">
                City: <strong>{item.city_name ?? "Unknown"}</strong>
                {item.city_code ? ` (${item.city_code})` : ""}
              </p>
              <div className="metric-grid">
                <div>
                  <p className="label">Edge</p>
                  <p className={`metric ${item.edge >= 0 ? "up" : "down"}`}>
                    {item.edge >= 0 ? "+" : ""}
                    {toPercent(item.edge)}
                  </p>
                </div>
                <div>
                  <p className="label">Confidence</p>
                  <p className="metric">{toPercent(item.confidence)}</p>
                </div>
                <div>
                  <p className="label">Paper Size</p>
                  <p className="metric">
                    {item.suggested_contracts} @ {toUsd(item.entry_price)}
                  </p>
                </div>
              </div>
              <p className="small">
                Suggested notional: <strong>{toUsd(item.suggested_notional_usd)}</strong>
              </p>
              <p className="small">{item.note}</p>
            </article>
          ))}
        </div>
        {playbook.length === 0 && !error ? (
          <p className="small">No playbook rows yet. Run `scripts\\run-daily.cmd`.</p>
        ) : null}
      </section>

      <section className="panel">
        <div className="section-head">
          <h2>Current Signals</h2>
          <p className="small">Active ranked entries</p>
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
                <p className="small">
                  City: <strong>{signal.city_name ?? "Unknown"}</strong>
                  {signal.city_code ? ` (${signal.city_code})` : ""}
                </p>

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
            <h2>
              Bot Intel Leaderboard{" "}
              <span className={`source-pill ${modeClass(sourceMap.bot_intel_feed?.mode)}`}>
                {(sourceMap.bot_intel_feed?.mode ?? "unknown").toUpperCase()}
              </span>
            </h2>
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

          <p className="small">Bastion-style intel view: rank and monitor leaders before adapting signal confidence.</p>

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
          <h2>
            Copy Activity Tape{" "}
            <span className={`source-pill ${modeClass(sourceMap.bot_intel_feed?.mode)}`}>
              {(sourceMap.bot_intel_feed?.mode ?? "unknown").toUpperCase()}
            </span>
          </h2>
          <p className="small">Recent follow events from tracked leaders.</p>

          <div className="activity-list">
            {activity.map((event, index) => (
              <article className="activity-item" key={`${event.event_time}-${event.market_ticker}-${index}`}>
                <p className="activity-line">
                  <span className="mono">{event.follower_alias}</span> copied <strong>{event.leader_display_name}</strong>
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
                <p className="small mono">source: {event.source}</p>
              </article>
            ))}

            {activity.length === 0 && !error ? (
              <p className="small">No activity yet. Configure a bot intel feed path or URL to populate this tape.</p>
            ) : null}
          </div>
        </section>
      </section>

      <section className="panel">
        <div className="section-head">
          <h2>Noob Glossary</h2>
          <p className="small">Quick definitions so the dashboard is easier to read.</p>
        </div>
        <div className="glossary-grid">
          {NOOB_TERMS.map((item) => (
            <article className="glossary-item" key={item.term}>
              <p className="label">{item.term}</p>
              <p className="small">{item.definition}</p>
            </article>
          ))}
        </div>
      </section>
    </main>
  );
}
