import { useEffect, useState } from "react";

const API_BASE = import.meta.env.VITE_API_BASE ?? "http://localhost:8000";

export function App() {
  const [health, setHealth] = useState(null);
  const [signals, setSignals] = useState([]);
  const [error, setError] = useState("");

  useEffect(() => {
    async function load() {
      try {
        const [healthRes, signalsRes] = await Promise.all([
          fetch(`${API_BASE}/health`),
          fetch(`${API_BASE}/v1/signals/current`),
        ]);

        if (!healthRes.ok || !signalsRes.ok) {
          throw new Error("Failed to load API data.");
        }

        setHealth(await healthRes.json());
        setSignals(await signalsRes.json());
      } catch (err) {
        setError(err.message);
      }
    }

    load();
  }, []);

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
            </article>
          ))}
          {signals.length === 0 && !error ? <p>No signals yet.</p> : null}
        </div>
      </section>
    </main>
  );
}
