from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from datetime import date

from workers.kalbot_workers.pipeline import DailyPipeline


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Kalbot worker CLI.")
    parser.add_argument(
        "--date",
        default=date.today().isoformat(),
        help="Pipeline run date (YYYY-MM-DD). Defaults to today.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_date = date.fromisoformat(args.date)
    summary = DailyPipeline(run_date=run_date).run()

    print(json.dumps(asdict(summary), indent=2))


if __name__ == "__main__":
    main()
