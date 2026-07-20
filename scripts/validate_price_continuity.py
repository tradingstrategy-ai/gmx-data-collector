"""Validate OHLCV candle files for unit-scaling corruption and grid gaps.

Detects the three defect classes found in the PEPE/BONK/SATS/SHIB forensics:

* decade folds — adjacent closes jumping by ~10x/100x (piecewise-wrong divisor)
* unit cliffs — a single 1e10-style regime switch mid-series
* grid gaps — missing bars on the forward-filled timestamp grid

Accepts freqtrade files (``date`` column, ``PAIR-1h-futures.feather``) and
source candle parquets (``timestamp`` column, ``1h.parquet``).

Usage::

    poetry run python scripts/validate_price_continuity.py FILE [FILE ...] \
        [--jump-ratio 5.0] [--timeframe 1h] [--json]

Exit code 0 if every file passes, 1 otherwise.

:author: gmx_historical_data maintainers
"""

import argparse
import dataclasses
import json
import sys
from collections import defaultdict
from pathlib import Path

import pandas as pd
import polars as pl

from gmx_historical_data.ohlcv_validation import (
    assert_export_parity,
    count_open_outside_envelope,
    validate_ohlcv,
)

TIMEFRAME_DELTAS = {
    "1m": pd.Timedelta(minutes=1),
    "5m": pd.Timedelta(minutes=5),
    "15m": pd.Timedelta(minutes=15),
    "1h": pd.Timedelta(hours=1),
    "4h": pd.Timedelta(hours=4),
    "1d": pd.Timedelta(days=1),
}


@dataclasses.dataclass
class ValidationReport:
    """Result of validating one candle DataFrame.

    :param path: source file path (empty for in-memory frames)
    :param rows: row count
    :param decade_jumps: adjacent-close ratios beyond the jump threshold
    :param missing_bars: bars absent from the regular timeframe grid
    :param zero_or_nan: closes that are 0, negative, or NaN
    :param duplicate_ts: duplicated timestamps
    :param malformed_ohlcv: 1 when the frame fails OHLCV validation
    :param parity_mismatch: 1 when the paired feather/parquet frames diverge
    :param open_outside: carried-forward opens sitting outside their own
        high/low envelope (reported, not a failure)
    :param first: first timestamp (ISO) or ""
    :param last: last timestamp (ISO) or ""
    :param ok: True when all defect counters are zero
    """

    path: str
    rows: int
    decade_jumps: int
    missing_bars: int
    zero_or_nan: int
    duplicate_ts: int
    malformed_ohlcv: int
    parity_mismatch: int
    open_outside: int
    first: str
    last: str
    ok: bool


def validate_frame(
    df: pd.DataFrame,
    timeframe: str,
    jump_ratio: float = 5.0,
    path: str = "",
    allow_gaps: bool = False,
) -> ValidationReport:
    """Validate one OHLCV frame.

    :param df: candle frame with a UTC ``date`` or ``timestamp`` column
    :param timeframe: one of 1m/5m/15m/1h/4h/1d
    :param jump_ratio: adjacent-close ratio treated as a decade jump
    :param path: label for the report
    :return: populated :class:`ValidationReport`
    """
    ts_col = "date" if "date" in df.columns else "timestamp"
    # Validate the file in its stored order before sorting for the numerical
    # continuity calculations below.  Sorting first would conceal a malformed
    # non-monotonic export.
    malformed_ohlcv = 0
    frame_pl = pl.from_pandas(df)
    try:
        validate_ohlcv(
            frame_pl,
            timestamp_column=ts_col,
            location=path or "<in-memory>",
        )
    except ValueError:
        malformed_ohlcv = 1
    # Carried-forward opens sitting outside their own high/low envelope are an
    # export convention, not a defect: reported for visibility, never fatal.
    open_outside = count_open_outside_envelope(frame_pl)

    df = df.sort_values(ts_col).reset_index(drop=True)
    close = df["close"]
    zero_or_nan = int(((close <= 0) | close.isna()).sum())
    duplicate_ts = int(df[ts_col].duplicated().sum())

    valid = close[close > 0]
    ratio = valid / valid.shift(1)
    decade_jumps = int(((ratio > jump_ratio) | (ratio < 1 / jump_ratio)).sum())

    delta = TIMEFRAME_DELTAS[timeframe]
    if len(df) > 1:
        span = df[ts_col].iloc[-1] - df[ts_col].iloc[0]
        expected = int(span / delta) + 1
        missing_bars = max(0, expected - df[ts_col].nunique())
    else:
        missing_bars = 0

    ok = (
        decade_jumps == 0
        and (allow_gaps or missing_bars == 0)
        and zero_or_nan == 0
        and duplicate_ts == 0
        and malformed_ohlcv == 0
    )
    return ValidationReport(
        path=path,
        rows=len(df),
        decade_jumps=decade_jumps,
        missing_bars=missing_bars,
        zero_or_nan=zero_or_nan,
        duplicate_ts=duplicate_ts,
        malformed_ohlcv=malformed_ohlcv,
        parity_mismatch=0,
        open_outside=open_outside,
        first=str(df[ts_col].iloc[0]) if len(df) else "",
        last=str(df[ts_col].iloc[-1]) if len(df) else "",
        ok=ok,
    )


def _timeframe_from_name(path: Path) -> str:
    """Infer timeframe from a candle filename.

    :param path: e.g. ``PEPE_USDC_USDC-1h-futures.feather`` or ``1h.parquet``
    :return: timeframe token
    :raises ValueError: when no known timeframe token is present
    """
    stem = path.stem
    for tf in TIMEFRAME_DELTAS:
        if stem == tf or f"-{tf}-" in stem or stem.endswith(f"-{tf}"):
            return tf
    raise ValueError(f"cannot infer timeframe from {path.name}")


def main() -> int:
    """CLI entry point.

    :return: process exit code (0 = all files ok)
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("files", nargs="+", type=Path)
    parser.add_argument("--jump-ratio", type=float, default=5.0)
    parser.add_argument("--timeframe", default=None, help="override inferred timeframe")
    parser.add_argument(
        "--allow-gaps",
        action="store_true",
        help="Report missing bars but do not fail the run on documented depth gaps",
    )
    parser.add_argument("--json", action="store_true", dest="as_json")
    args = parser.parse_args()

    reports = []
    for f in args.files:
        df = pd.read_feather(f) if f.suffix == ".feather" else pd.read_parquet(f)
        tf = args.timeframe or _timeframe_from_name(f)
        reports.append(
            validate_frame(df, tf, args.jump_ratio, path=str(f), allow_gaps=args.allow_gaps)
        )

    reports_by_path = {Path(r.path): r for r in reports}
    grouped: dict[str, list[Path]] = defaultdict(list)
    for f in args.files:
        grouped[f.stem].append(f)

    parity_failures = []
    for stem, paths in grouped.items():
        feather = next((p for p in paths if p.suffix == ".feather"), None)
        parquet = next((p for p in paths if p.suffix == ".parquet"), None)
        if not feather or not parquet:
            continue
        left = pl.from_pandas(pd.read_feather(feather))
        right = pl.from_pandas(pd.read_parquet(parquet))
        try:
            assert_export_parity(left, right, location=stem)
        except ValueError as exc:
            parity_failures.append(str(exc))
            for path in (feather, parquet):
                report = reports_by_path.get(path)
                if report is not None:
                    report.parity_mismatch = 1
                    report.ok = False

    if args.as_json:
        print(json.dumps([dataclasses.asdict(r) for r in reports], indent=2))
    else:
        for r in reports:
            flag = "OK  " if r.ok else "FAIL"
            print(
                f"{flag} {Path(r.path).name}: rows={r.rows} jumps={r.decade_jumps} "
                f"missing={r.missing_bars} zero/nan={r.zero_or_nan} dupes={r.duplicate_ts} "
                f"malformed={r.malformed_ohlcv} parity={r.parity_mismatch} "
                f"open_outside={r.open_outside} "
                f"[{r.first} .. {r.last}]"
            )
    return 0 if all(r.ok for r in reports) and not parity_failures else 1


if __name__ == "__main__":
    sys.exit(main())
