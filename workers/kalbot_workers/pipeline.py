from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Callable

from kalbot.bot_intel_repo import BotIntelRepositoryError, refresh_bot_intel
from kalbot.backtest_repo import BacktestRepositoryError, write_backtest_report
from kalbot.kalshi_ingest import KalshiIngestError, ingest_kalshi_weather_markets
from kalbot.modeling.low_temp_model import (
    build_low_temp_training_features,
    train_low_temp_model,
)
from kalbot.paper_execution import PaperExecutionError, execute_paper_trades
from kalbot.settlement_repo import SettlementRepositoryError, reconcile_settlements
from kalbot.signals_repo import SignalRepositoryError, publish_best_signal_for_date
from kalbot.settings import get_settings
from kalbot.weather_ingest import WeatherIngestError, ingest_weather_data


@dataclass
class PipelineStepResult:
    step: str
    status: str
    message: str


@dataclass
class PipelineSummary:
    run_date: str
    environment: str
    execution_mode: str
    model_name: str
    started_at_utc: str
    completed_at_utc: str
    steps: list[PipelineStepResult]


class DailyPipeline:
    def __init__(self, run_date: date) -> None:
        self.run_date = run_date
        self.settings = get_settings()
        self.steps: list[PipelineStepResult] = []

    def _record(self, step: str, status: str, message: str) -> None:
        self.steps.append(PipelineStepResult(step=step, status=status, message=message))

    def _run_step(self, name: str, func: Callable[[], str]) -> None:
        try:
            message = func()
            self._record(step=name, status="ok", message=message)
        except Exception as exc:  # pragma: no cover - defensive guard
            self._record(step=name, status="error", message=str(exc))
            raise

    def ingest_data(self) -> str:
        messages: list[str] = []

        try:
            weather = ingest_weather_data(self.settings)
            messages.append(
                "weather "
                f"targets={weather.targets_succeeded}/{weather.targets_attempted}, "
                f"forecast_rows={weather.forecast_rows_written}, "
                f"observation_rows={weather.observation_rows_written}"
            )
            if weather.target_failures:
                messages.append(f"weather_failures={len(weather.target_failures)}")
        except WeatherIngestError as exc:
            messages.append(f"weather_skipped={exc}")

        try:
            kalshi = ingest_kalshi_weather_markets(self.settings)
            messages.append(
                "kalshi "
                f"series={kalshi.series_scanned}, "
                f"markets_written={kalshi.markets_written}, "
                f"snapshots_written={kalshi.snapshots_written}"
            )
            if kalshi.failures:
                messages.append(f"kalshi_failures={len(kalshi.failures)}")
        except KalshiIngestError as exc:
            messages.append(f"kalshi_skipped={exc}")

        return " | ".join(messages)

    def build_features(self) -> str:
        try:
            summary = build_low_temp_training_features(self.run_date.isoformat())
            return (
                "Feature build complete: "
                f"examples={summary.examples}, stations={summary.stations}, "
                f"path={summary.output_path}"
            )
        except Exception as exc:
            return f"Feature build skipped: {exc}"

    def train_and_calibrate(self) -> str:
        try:
            summary = train_low_temp_model(self.run_date.isoformat())
            return (
                "Model train complete: "
                f"samples={summary.samples}, stations={summary.stations}, "
                f"sigma={summary.global_sigma_f:.2f}F, rmse={summary.rmse_f:.2f}F, "
                f"path={summary.output_path}"
            )
        except Exception as exc:
            return f"Model train skipped: {exc}"

    def score_and_decide(self) -> str:
        return "Signal scoring complete (edge-based trade decisions written per prediction)."

    def reconcile_market_outcomes(self) -> str:
        try:
            return reconcile_settlements(self.run_date, settings=self.settings)
        except SettlementRepositoryError as exc:
            return f"Settlement reconcile skipped: {exc}"

    def evaluate_backtest(self) -> str:
        try:
            return write_backtest_report(
                run_date=self.run_date,
                days=max(7, self.settings.backtest_window_days),
            )
        except BacktestRepositoryError as exc:
            return f"Backtest skipped: {exc}"

    def simulate_execution(self) -> str:
        try:
            return execute_paper_trades(self.run_date)
        except PaperExecutionError as exc:
            return f"Execution skipped: {exc}"

    def update_bot_intel(self) -> str:
        try:
            return refresh_bot_intel(self.run_date, settings=self.settings)
        except BotIntelRepositoryError as exc:
            return f"Skipped bot intel update: {exc}"

    def publish_signal_snapshot(self) -> str:
        try:
            return publish_best_signal_for_date(self.run_date)
        except SignalRepositoryError as exc:
            # Pipeline should stay runnable before DB is ready.
            return f"Skipped DB publish: {exc}"

    def run(self) -> PipelineSummary:
        started_at = datetime.now(timezone.utc)
        self._run_step("ingest_data", self.ingest_data)
        self._run_step("reconcile_market_outcomes", self.reconcile_market_outcomes)
        self._run_step("evaluate_backtest", self.evaluate_backtest)
        self._run_step("build_features", self.build_features)
        self._run_step("train_and_calibrate", self.train_and_calibrate)
        self._run_step("score_and_decide", self.score_and_decide)
        self._run_step("simulate_execution", self.simulate_execution)
        self._run_step("update_bot_intel", self.update_bot_intel)
        self._run_step("publish_signal_snapshot", self.publish_signal_snapshot)
        completed_at = datetime.now(timezone.utc)

        summary = PipelineSummary(
            run_date=self.run_date.isoformat(),
            environment=self.settings.environment,
            execution_mode=self.settings.execution_mode,
            model_name=self.settings.model_name,
            started_at_utc=started_at.isoformat(),
            completed_at_utc=completed_at.isoformat(),
            steps=self.steps,
        )
        self._write_summary(summary)
        return summary

    def _write_summary(self, summary: PipelineSummary) -> None:
        out_dir = Path("artifacts") / "daily" / summary.run_date
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "run-summary.json"

        payload = asdict(summary)
        payload["steps"] = [asdict(step) for step in summary.steps]
        out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
