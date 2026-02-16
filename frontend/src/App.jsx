import React, { useEffect, useState } from "react";

const API_BASE = import.meta.env.VITE_API_BASE ?? "http://localhost:8000";

export function App() {
  const [health, setHealth] = useState(null);
  const [signals, setSignals] = useState([]);
  const [leaderboard, setLeaderboard] = useState([]);
  const [sort, setSort] = useState("impressiveness");
  const [window, setWindow] = useState("all");
  const [error, setError] = useState("");

  useEffect(() => {
    async function load() {
      try {
        const [healthRes, signalsRes, leaderboardRes] = await Promise.all([
          fetch(`${API_BASE}/health`),
          fetch(`${API_BASE}/v1/signals/current`),
          fetch(
            `${API_BASE}/v1/intel/leaderboard?sort=${encodeURIComponent(sort)}&window=${encodeURIComponent(window)}&limit=10`
          ),
        ]);

        if (!healthRes.ok || !signalsRes.ok || !leaderboardRes.ok) {
          throw new Error("Failed to load API data.");
        }

        setHealth(await healthRes.json());
        setSignals(await signalsRes.json());
        setLeaderboard(await leaderboardRes.json());
      } catch (err) {
        setError(err.message);
      }
    }

    load();
  }, [sort, window]);

  return (
    <main className="page">
      <section className="panel">
        <p className="kicker">Kalbot</p>
        <h1>Weather Trading Engine</h1>
        <p className="subtitle">
          API mode: <strong>{health?.execution_mode ?? "loading..."}</strong>
        </p>
        {error ? <p className="error">{error}</p> : null}
      </section>

      <section className="panel">
        <h2>Current Signals</h2>
        <div className="cards">
          {signals.map((signal) => (
            <article className="card" key={signal.market_ticker}>
              <p className="title">{signal.title}</p>
              <p>Model YES: {(signal.probability_yes * 100).toFixed(1)}%</p>
              <p>Market YES: {(signal.market_implied_yes * 100).toFixed(1)}%</p>
              <p>Edge: {(signal.edge * 100).toFixed(1)} pts</p>
              <p>Confidence: {(signal.confidence * 100).toFixed(1)}%</p>
              <p className="small">{signal.rationale}</p>
              <p className="small">
                Source:{" "}
                <a href={signal.data_source_url} target="_blank" rel="noreferrer">
                  {signal.data_source_url}
                </a>
              </p>
            </article>
          ))}
          {signals.length === 0 && !error ? <p>No signals yet.</p> : null}
        </div>
      </section>

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
          Inspired by public leaderboard products: rank by ROI/PnL/volume, then use
          this intel as a second input for Kalbot decisions.
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
                  <td>${entry.pnl_usd.toLocaleString()}</td>
                  <td>${entry.volume_usd.toLocaleString()}</td>
                </tr>
              ))}
            </tbody>
          </table>
          {leaderboard.length === 0 && !error ? (
            <p className="small">No leaderboard rows yet. Run the daily worker first.</p>
          ) : null}
        </div>
      </section>
    </main>
  );
}
