#!/usr/bin/env python3
"""Read-only regression checks for a local GMX collector data lake."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import tempfile
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import polars as pl

CHECKS = {"G1a", "G1b", "G1c", "G1d", "G1e", "G1f", "G1g", "G2", "G3", "G4"}
TF_SECONDS = {"m": 60, "h": 3600, "d": 86400, "w": 604800}


@dataclass
class Finding:
    check: str
    symbol: str
    message: str
    failed: bool = True


def utc_now():
    return datetime.now(UTC)


def detect_root(root: Path) -> Path:
    root = root.expanduser().resolve()
    if (root / "funding/arbitrum").is_dir() or (root / "futures").is_dir():
        return root
    nested = root / "user_data/data/gmx"
    if (nested / "funding/arbitrum").is_dir() or (nested / "futures").is_dir():
        return nested
    raise ValueError(f"unrecognized GMX lake layout: {root}")


def cols(path):
    try:
        return pl.scan_parquet(path).collect_schema().names()
    except Exception:
        return []


def read(path, columns=None):
    try:
        frame = pl.read_parquet(path)
        return frame.select([c for c in (columns or frame.columns) if c in frame.columns])
    except Exception:
        return None


def to_dt(series):
    # Polars timestamp values are datetimes; tolerate integer epoch seconds/ms.
    values = series.to_list()
    if not values:
        return []
    if isinstance(values[0], datetime):
        return [v.replace(tzinfo=UTC) if v.tzinfo is None else v.astimezone(UTC) for v in values]
    divisor = 1000 if abs(int(values[0])) > 10**11 else 1
    return [datetime.fromtimestamp(int(v) / divisor, UTC) for v in values]


def checkpoint_dt(value):
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, UTC)
    stamp = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return stamp.replace(tzinfo=UTC) if stamp.tzinfo is None else stamp.astimezone(UTC)


def _sign(value):
    number = float(value)
    if number > 0:
        return 1
    if number < 0:
        return -1
    return 0


def issue(out, check, symbol, message, failed=True):
    out.append(Finding(check, symbol, message, failed))


def run(
    root: Path,
    checks: set[str],
    now: datetime,
    null_share: float,
    max_lag_bars: int,
    tail_expected: bool | None,
    max_checkpoint_age_hours: float = 6,
    max_tail_status_age_hours: float = 36,
    check_8h: bool = False,
):
    out = []
    net = root / "funding/arbitrum"
    rates = net / "rates"
    checkpoint = net / "checkpoints"
    futures = root / "futures"
    canonical = sorted(rates.glob("*/1h.parquet"))
    active = {}
    frames = {}
    since30, since7 = now - timedelta(days=30), now - timedelta(days=7)
    listing_dates = {}
    if "G1c" in checks or "G2" in checks:
        from gmx_historical_data.funding_release import load_listing_dates

        listing_dates = load_listing_dates(root / "snapshots")
    for candle in sorted(futures.glob("*-1h-futures.feather")):
        match = re.match(r"(.+)_USDC_USDC-1h-futures\.feather$", candle.name)
        if match:
            symbol = match.group(1)
            active[symbol] = (
                symbol.upper() not in _delisted(root)
                and _newest(candle) is not None
                and _newest(candle) >= now - timedelta(hours=72)
            )
            listing_date = listing_dates.get(symbol.upper()) or _oldest(candle)
            in_listing_grace = listing_date is not None and now - listing_date <= timedelta(days=7)
            if (
                "G1c" in checks
                and active[symbol]
                and not in_listing_grace
                and not (rates / symbol / "1h.parquet").is_file()
            ):
                issue(out, "G1c", symbol, "active market has no canonical 1h series")
    for path in canonical:
        sym = path.parent.name
        frame = read(path)
        if frame is None or "timestamp" not in frame.columns:
            continue
        ts = to_dt(frame["timestamp"])
        frame = frame.with_columns(pl.Series("__dt", ts))
        frames[sym] = frame
        if sym not in active:
            candle = futures / f"{sym}_USDC_USDC-1h-futures.feather"
            active[sym] = (
                candle.exists()
                and sym.upper() not in _delisted(root)
                and _newest(candle) is not None
                and _newest(candle) >= now - timedelta(hours=72)
            )
        if "G1b" in checks:
            dup = len(ts) != len(set(ts))
            unsorted = ts != sorted(ts)
            if dup or unsorted:
                issue(
                    out,
                    "G1b",
                    sym,
                    f"canonical timestamps {'duplicate' if dup else ''}{' and ' if dup and unsorted else ''}{'unsorted' if unsorted else ''}",
                )
        candle = futures / f"{sym}_USDC_USDC-1h-futures.feather"
        listing_date = listing_dates.get(sym.upper()) or _oldest(candle)
        in_listing_grace = listing_date is not None and now - listing_date <= timedelta(days=7)
        if "G1c" in checks and active[sym] and not in_listing_grace:
            recent = sorted(t for t in ts if since30 <= t < now)
            if recent:
                expected = int((recent[-1] - max(since30, recent[0])).total_seconds() // 3600) + 1
                if len(set(recent)) < expected:
                    issue(
                        out,
                        "G1c",
                        sym,
                        f"hourly holes in trailing 30d ({expected - len(set(recent))} missing)",
                    )
        rate_column = next(
            (
                column
                for column in ("funding_rate_hourly", "rate", "funding_rate")
                if column in frame.columns
            ),
            None,
        )
        if "G1d" in checks and "longs_pay_shorts" in frame.columns and rate_column:
            mask = [t >= since7 for t in ts]
            indexes = [i for i, yes in enumerate(mask) if yes]
            if indexes:
                rates7 = frame[rate_column].to_list()
                dirs = frame["longs_pay_shorts"].to_list()
                valid = [i for i in indexes if rates7[i] is not None and rates7[i] != 0]
                frac = sum(dirs[i] is None for i in valid) / len(valid) if valid else 0
                if frac > null_share:
                    issue(out, "G1d", sym, f"unknown direction share {frac:.1%} > {null_share:.1%}")
    if "G1a" in checks:
        for path in sorted(rates.glob("*/1h_factor.parquet")):
            if "source" in cols(path):
                issue(out, "G1a", path.parent.name, "1h_factor.parquet has source column")
    if "G1e" in checks:
        expected = (
            tail_expected if tail_expected is not None else bool(os.getenv("JSON_RPC_ARBITRUM"))
        )
        if expected:
            p = checkpoint / "funding_datastore_tail_status.json"
            try:
                status = json.loads(p.read_text())
                stamp = checkpoint_dt(status.get("updated_at", status.get("generated_at", "")))
                if status.get("status") == "failed" or status.get("outcome") == "failed":
                    issue(out, "G1e", "-", "tail status failed")
                elif now - stamp.astimezone(UTC) > timedelta(hours=max_tail_status_age_hours):
                    issue(
                        out,
                        "G1e",
                        "-",
                        f"tail status older than {max_tail_status_age_hours:g}h",
                    )
            except Exception:
                issue(out, "G1e", "-", "tail status missing or unreadable")
    if "G1f" in checks:
        for name in ("funding_factor_checkpoint.json", "fee_per_size_checkpoint.json"):
            try:
                data = json.loads((checkpoint / name).read_text())
                stamp = checkpoint_dt(data["last_timestamp"])
                lag = now - stamp.astimezone(UTC)
                if lag > timedelta(hours=max_checkpoint_age_hours):
                    issue(
                        out,
                        "G1f",
                        name.removesuffix("_checkpoint.json"),
                        f"checkpoint {lag.total_seconds() / 3600:.1f}h behind (limit {max_checkpoint_age_hours:g}h)",
                    )
            except Exception:
                issue(
                    out,
                    "G1f",
                    name.removesuffix("_checkpoint.json"),
                    "checkpoint missing or unreadable",
                )
    if "G1g" in checks:
        symbols = sorted(
            {p.parent.name for p in rates.glob("*/1h_datastore.parquet")}
            | {p.parent.name for p in canonical}
        )
        for sym in symbols:
            checked_rows = fee_mismatches = direction_mismatches = 0
            sources = (
                (rates / sym / "1h_datastore.parquet", False),
                (rates / sym / "1h.parquet", True),
            )
            for path, canonical_file in sources:
                if not path.is_file():
                    continue
                source_frame = read(path)
                if source_frame is None:
                    continue
                if "source" in source_frame.columns:
                    source_frame = source_frame.filter(pl.col("source") == "datastore")
                elif canonical_file:
                    continue
                rate_column = next(
                    (
                        c
                        for c in ("funding_rate_hourly", "funding_rate")
                        if c in source_frame.columns
                    ),
                    None,
                )
                if rate_column is None or "funding_fee_long" not in source_frame.columns:
                    if source_frame.height:
                        issue(
                            out, "G1g", sym, f"{path.name} lacks rate or funding_fee_long columns"
                        )
                    continue
                rate_values = source_frame[rate_column].to_list()
                fees = source_frame["funding_fee_long"].to_list()
                directions = (
                    source_frame["longs_pay_shorts"].to_list()
                    if "longs_pay_shorts" in source_frame.columns
                    else [None] * source_frame.height
                )
                for rate_value, fee_value, direction in zip(rate_values, fees, directions):
                    if rate_value is None:
                        continue
                    checked_rows += 1
                    rate_sign = _sign(rate_value)
                    if fee_value is None or _sign(fee_value) != rate_sign:
                        fee_mismatches += 1
                    if rate_sign and direction is not (rate_sign > 0):
                        direction_mismatches += 1
            issue(
                out,
                "G1g",
                sym,
                f"datastore rows={checked_rows}; fee/rate sign mismatches={fee_mismatches}; direction/rate sign mismatches={direction_mismatches}",
                failed=bool(fee_mismatches or direction_mismatches),
            )
    if "G2" in checks:
        try:
            from gmx_historical_data.funding_release import companion_dir, stage_companions
            from gmx_historical_data.funding_state import assess_funding_state
            from gmx_historical_data.series_coverage import (
                SeriesThresholds,
                TailStatus,
                assess_series_coverage,
            )

            with tempfile.TemporaryDirectory(prefix="verify-gmx-lake-") as temp:
                report = stage_companions(root, Path(temp) / "stage")
                state = assess_funding_state(root).value
                coverage = assess_series_coverage(
                    futures_dir=futures,
                    companion_dir=companion_dir(Path(temp) / "stage"),
                    checkpoint_dir=checkpoint,
                    delisted=_delisted(root),
                    now=now,
                    state=state,
                    extract_outcome="success",
                    export_report=report,
                    previous=None,
                    thresholds=SeriesThresholds(head_lag_hours=max_checkpoint_age_hours),
                    listing_dates=listing_dates,
                )
                for pair in coverage.pairs:
                    if pair.tail is TailStatus.STALE:
                        issue(out, "G2", pair.symbol, "STALE funding tail", failed=False)
                if not coverage.passed:
                    for reason in coverage.reasons:
                        issue(out, "G2", "-", reason)
        except Exception as e:
            issue(out, "G2", "-", f"coverage staging/assessment failed: {e}")
    if "G3" in checks:
        funding_paths = sorted(futures.glob("*-1h-funding_rate.feather"))
        if check_8h:
            funding_paths += sorted(futures.glob("*-8h-funding_rate.feather"))
        for path in funding_paths:
            m = re.match(r"(.+)_USDC_USDC-(1h|8h)-funding_rate\.feather$", path.name)
            if not m:
                continue
            sym, tf = m.groups()
            direction = frames.get(sym)
            if direction is None or "longs_pay_shorts" not in direction.columns:
                continue
            try:
                exported = pl.read_ipc(path)
            except Exception:
                continue
            if not {"date", "open"}.issubset(exported.columns):
                continue
            dates = to_dt(exported["date"])
            opens = exported["open"].to_list()
            dts = direction["__dt"].to_list()
            dirs = direction["longs_pay_shorts"].to_list()
            lookup = {t: d for t, d in zip(dts, dirs)}
            for t, value in zip(dates, opens):
                if t < since30 or t >= now or value is None:
                    continue
                # The 8h normalizer preserves each hourly export timestamp; compare
                # direction at that same timestamp for both exported timeframes.
                direction_value = lookup.get(t)
                if direction_value is None or value == 0:
                    continue
                if (value > 0) != bool(direction_value):
                    issue(out, "G3", sym, f"{tf} exported sign disagrees at {t.isoformat()}")
    if "G4" in checks:
        for path in sorted(futures.glob("*-futures.feather")):
            match = re.match(r"(.+)_USDC_USDC-(\d+[mhdw])-futures\.feather$", path.name)
            if not match:
                continue
            sym, tf = match.groups()
            timeframe = re.fullmatch(r"(\d+)([mhdw])", tf)
            seconds = (
                int(timeframe.group(1)) * TF_SECONDS[timeframe.group(2)] if timeframe else None
            )
            if seconds is None:
                continue
            newest_closed = _newest_closed(path, seconds, now)
            if newest_closed is None:
                continue
            # Fast candles are refreshed in batches during the nightly collector
            # run, so a few minutes of natural lag should not fail the daily gate.
            lag_limit_seconds = max(max_lag_bars * seconds, 4 * 60 * 60)
            if (
                sym in active
                and active[sym]
                and (now - (newest_closed + timedelta(seconds=seconds))).total_seconds()
                > lag_limit_seconds
            ):
                issue(
                    out,
                    "G4",
                    sym,
                    f"newest closed {tf} bar is more than {lag_limit_seconds:g} seconds behind",
                )
    return out


def _delisted(root):
    try:
        data = json.loads((root / "delisted_markets.json").read_text())
        values = data if isinstance(data, list) else data.get("symbols", data.get("markets", []))
        return {str(x).upper() for x in values}
    except Exception:
        return set()


def _newest(path):
    try:
        frame = pl.read_ipc(path, columns=["date"])
        return max(to_dt(frame["date"])) if frame.height else None
    except Exception:
        return None


def _newest_closed(path, seconds, now):
    """Return the latest candle open timestamp whose full interval has closed."""
    try:
        frame = pl.read_ipc(path, columns=["date"])
        closed = [
            stamp
            for stamp in to_dt(frame["date"])
            if stamp + timedelta(seconds=seconds) <= now
        ]
        return max(closed) if closed else None
    except Exception:
        return None


def _oldest(path):
    try:
        frame = pl.read_ipc(path, columns=["date"])
        return min(to_dt(frame["date"])) if frame.height else None
    except Exception:
        return None


def _is_within(path: Path, directory: Path) -> bool:
    return path == directory or path.is_relative_to(directory)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_root", nargs="?", default="user_data/data/gmx")
    parser.add_argument("--report-only", action="store_true")
    parser.add_argument("--json", dest="json_path", type=Path)
    parser.add_argument("--checks", nargs="+", choices=sorted(CHECKS), default=sorted(CHECKS))
    parser.add_argument("--null-direction-share", type=float, default=0.05)
    parser.add_argument("--max-lag-bars", type=int, default=3)
    parser.add_argument("--tail-fill-expected", action="store_true", default=None)
    parser.add_argument("--max-checkpoint-age-hours", type=float, default=6)
    parser.add_argument("--max-tail-status-age-hours", type=float, default=36)
    parser.add_argument(
        "--check-8h",
        action="store_true",
        help="also validate externally maintained 8h funding feathers",
    )
    args = parser.parse_args(argv)
    supplied_root = Path(args.data_root).expanduser().resolve()
    report_path = args.json_path.expanduser().resolve() if args.json_path is not None else None
    if report_path is not None and _is_within(report_path, supplied_root):
        print("VERIFY_LAKE: FAIL G0 - --json path must be outside the data root")
        print("VERIFY_LAKE: FAIL 1 findings")
        return 2
    try:
        root = detect_root(supplied_root)
    except Exception as e:
        findings = [Finding("G0", "-", str(e))]
    else:
        if report_path is not None and _is_within(report_path, root):
            print("VERIFY_LAKE: FAIL G0 - --json path must be outside the data root")
            print("VERIFY_LAKE: FAIL 1 findings")
            return 2
        try:
            findings = run(
                root,
                set(args.checks),
                utc_now(),
                args.null_direction_share,
                args.max_lag_bars,
                args.tail_fill_expected,
                args.max_checkpoint_age_hours,
                args.max_tail_status_age_hours,
                args.check_8h,
            )
        except Exception as e:
            findings = [Finding("G0", "-", str(e))]
    for item in findings:
        print(
            f"VERIFY_LAKE: {'FAIL' if item.failed else 'STALE' if item.check == 'G2' else 'OK'} {item.check} {item.symbol} {item.message}"
        )
    failed = any(f.failed for f in findings)
    print(f"VERIFY_LAKE: {'FAIL' if failed else 'OK'} {len(findings)} findings")
    if report_path is not None and not any(f.check == "G0" for f in findings):
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(
            json.dumps(
                {
                    "root": str(root) if "root" in locals() else args.data_root,
                    "findings": [asdict(f) for f in findings],
                    "failed": failed,
                },
                indent=2,
            )
            + "\n"
        )
    return 0 if args.report_only or not failed else 1


if __name__ == "__main__":
    sys.exit(main())
