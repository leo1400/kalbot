from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Callable

from kalbot.settings import get_settings


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
        return "Ingestion stubs complete (Kalshi + weather connectors pending)."

    def build_features(self) -> str:
        return "Feature builder stub complete (rolling windows pending)."

    def train_and_calibrate(self) -> str:
        return (
            "Training stub complete (baseline model placeholder, calibration pending)."
        )

    def score_and_decide(self) -> str:
        return "Decision stub complete (edge thresholds and risk caps pending)."

    def simulate_execution(self) -> str:
        mode = self.settings.execution_mode
        return f"Execution stub complete (current mode={mode})."

    def publish_signal_snapshot(self) -> str:
        return "Publishing stub complete (DB-backed signal cards pending)."

    def run(self) -> PipelineSummary:
        started_at = datetime.now(timezone.utc)
        self._run_step("ingest_data", self.ingest_data)
        self._run_step("build_features", self.build_features)
        self._run_step("train_and_calibrate", self.train_and_calibrate)
        self._run_step("score_and_decide", self.score_and_decide)
        self._run_step("simulate_execution", self.simulate_execution)
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
