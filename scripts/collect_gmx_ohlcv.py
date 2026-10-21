#!/usr/bin/env python3
"""Refresh and audit GMX's recently available OHLCV candles."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path


def _write_incident(incident_dir: Path, *, failures: list[str], gaps: list[dict]) -> Path:
    incident_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    path = incident_dir / f"{timestamp}-gmx-candle-gap.json"
    path.write_text(
        json.dumps(
            {
                "created_at": datetime.now(UTC).isoformat(),
                "failed_symbol_timeframes": failures,
                "recent_cadence_breaks": gaps,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    return path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--network", choices=["arbitrum", "avalanche"], default="arbitrum")
    parser.add_argument(
        "--window-hours",
        type=float,
        default=160,
        help="Recent continuity window, kept below the observed 1m API history window.",
    )
    parser.add_argument("--incident-dir", type=Path, default=Path("/srv/gmx/incidents"))
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if args.window_hours <= 0:
        parser.error("--window-hours must be positive")

    # Import after argument parsing so --help is usable without collector extras.
    from eth_defi.gmx.api import GMXAPI

    from gmx_historical_data.recent_candle_audit import audit_recent_candles
    from scripts.collect_daily_snapshot import _fetch_all_markets, collect_and_save_ohlcv

    started_at = datetime.now(UTC)
    futures_dir = args.output_dir / "data" / "gmx" / "futures"
    api = GMXAPI(chain=args.network)
    markets = _fetch_all_markets(api)
    if not markets:
        print("No market data returned from GMX API", file=sys.stderr)
        return 1

    saved, failures, _coverage = collect_and_save_ohlcv(
        api,
        markets,
        futures_dir,
        timeframes=["1m", "5m", "15m", "1h", "4h"],
        repair=True,
        now=datetime.now(UTC),
    )
    window_end = datetime.now(UTC)
    gaps = audit_recent_candles(
        futures_dir,
        window_end - timedelta(hours=args.window_hours),
        window_end,
    )
    gap_records = [
        {
            "before": gap.before.isoformat(),
            "after": gap.after.isoformat(),
            "missing_bars": gap.missing_bars,
        }
        for gap in gaps
    ]
    print(
        f"Saved/merged {saved} OHLCV files; {len(failures)} failed pairs; {len(gaps)} recent gaps"
    )
    if failures or gaps:
        incident = _write_incident(args.incident_dir, failures=failures, gaps=gap_records)
        print(f"Unresolved candle continuity issue recorded: {incident}", file=sys.stderr)
        return 1
    print(f"Continuity audit passed in {(window_end - started_at).total_seconds():.1f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
