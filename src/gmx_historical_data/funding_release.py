"""Stage -> gate -> promote for the daily funding and mark companions.

The release must never ship a funding or mark file nobody has graded, and must
never replace a good one with an ungraded one. So the new files are built in a
staging directory, graded by :func:`~gmx_historical_data.series_coverage.assess_series_coverage`
and only on a pass copied over ``futures/``. On a fail ``futures/`` keeps the
companions it was restored with -- the last ones that passed this gate -- and
``funding_coverage.json`` says plainly that funding was not refreshed. The raw
funding state under ``funding/`` is never modified by any step here.
"""

from __future__ import annotations

import json
import logging
import os
import re
import shutil
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import polars as pl

from gmx_historical_data.atomic_parquet import atomic_write_ipc
from gmx_historical_data.cadence_gate import load_exempt_symbols
from gmx_historical_data.freqtrade_exporter import FreqtradeExporter
from gmx_historical_data.funding_state import funding_network_dir
from gmx_historical_data.series_coverage import (
    FUNDING_EPOCH,
    PairCoverage,
    SeriesThresholds,
    TailStatus,
    _read_series,
    assess_series_coverage,
)

logger = logging.getLogger(__name__)

TIMEFRAME = "1h"
COVERAGE_FILE = "funding_coverage.json"
EXPORT_REPORT = "export_report.json"
REPORT_HEADER = "## Funding & Mark Companions"
_FUTURES_1H = re.compile(r"^(?P<symbol>.+)_USDC_USDC-1h-futures\.feather$")
_COMPANION_KINDS = ("funding_rate", "mark")


def companion_dir(stage_root: Path) -> Path:
    """Directory inside ``stage_root`` that holds the staged feathers."""
    return Path(stage_root) / "gmx" / "futures"


def _futures_symbols(futures_dir: Path) -> list[str]:
    out = []
    for path in sorted(futures_dir.glob("*-1h-futures.feather")):
        match = _FUTURES_1H.match(path.name)
        if match and not path.name.startswith("._"):
            out.append(match.group("symbol"))
    return out


def stage_companions(gmx_root: Path, stage_root: Path) -> dict[str, Any]:
    """Build the 1h funding and mark feathers into ``stage_root``.

    Starts from the companions already published in ``futures/`` so the
    exporter's merge keeps every previously shipped bar, then exports funding
    through the shared signed exporter and derives mark from the futures
    feathers. Only symbols that have a ``-1h-futures.feather`` are exported:
    the funding source also holds alt-pool markets and deprecated aliases that
    are not freqtrade pairs. ``futures/`` itself is not written.

    :param gmx_root: ``user_data/data/gmx``.
    :param stage_root: Scratch directory (recreated).
    :returns: The export report (also written to ``stage_root/export_report.json``).
    """
    gmx_root, stage_root = Path(gmx_root), Path(stage_root)
    futures = gmx_root / "futures"
    if stage_root.exists():
        shutil.rmtree(stage_root)
    staged = companion_dir(stage_root)
    staged.mkdir(parents=True)

    symbols = _futures_symbols(futures)
    for symbol in symbols:
        for kind in _COMPANION_KINDS:
            existing = futures / f"{symbol}_USDC_USDC-{TIMEFRAME}-{kind}.feather"
            if existing.is_file():
                shutil.copy2(existing, staged / existing.name)

    exporter = FreqtradeExporter(
        data_dir=gmx_root,
        output_dir=stage_root,
        funding_dir=funding_network_dir(gmx_root) / "rates",
    )
    _, failed_symbols, failures = exporter.export_funding(
        symbols=symbols, timeframes=[TIMEFRAME], output_format="feather"
    )
    _, mark_failures = exporter.export_mark_from_futures(
        futures, symbols=symbols, timeframes=(TIMEFRAME,), out_dir=staged
    )

    # The exporter merges the restored history with the fresh export, so a
    # restored value would survive at an hour the current source rejects (null
    # rate, or a nonzero rate with unknown direction). Whatever its age, a
    # rejected hour must not appear in the output: drop it from the staged
    # history. The gate accounts for it (blocks if recent, `rejected_older`
    # otherwise).
    dropped: dict[str, int] = {}
    for rec in exporter.funding_reconciliations:
        if rec.timeframe != TIMEFRAME:
            continue
        rejected = set(rec.unknown_direction_hours) | set(rec.null_rate_hours)
        target = staged / f"{rec.symbol}_USDC_USDC-{TIMEFRAME}-funding_rate.feather"
        if not rejected or not target.is_file():
            continue
        frame = pl.read_ipc(target, memory_map=False)
        drop = pl.DataFrame({"date": pl.Series(sorted(rejected), dtype=frame.schema["date"])})
        kept = frame.join(drop, on="date", how="anti")
        if len(kept) != len(frame):
            dropped[rec.symbol] = len(frame) - len(kept)
            atomic_write_ipc(kept, target)

    def iso(hours) -> list[str]:
        return [h.isoformat() for h in hours]

    report = {
        "failed_symbols": sorted(failed_symbols),
        "dropped_rejected_hours": dropped,
        "failures": [
            {"symbol": f.symbol, "timeframe": f.timeframe, "reason": f.reason, "message": f.message}
            for f in [*failures, *mark_failures]
        ],
        "reconciliations": {
            r.symbol: {
                "missing_hours": iso(r.missing_hours),
                "unknown_direction_hours": iso(r.unknown_direction_hours),
                "null_rate_hours": iso(r.null_rate_hours),
            }
            for r in exporter.funding_reconciliations
            if r.timeframe == TIMEFRAME
        },
    }
    (stage_root / EXPORT_REPORT).write_text(json.dumps(report, indent=2))
    return report


def promote_staged(staged: Path, futures_dir: Path) -> int:
    """Copy every staged companion over ``futures_dir``, one atomic replace each.

    :returns: Number of files promoted.
    """
    futures_dir.mkdir(parents=True, exist_ok=True)
    count = 0
    for path in sorted(staged.glob("*.feather")):
        if not any(path.name.endswith(f"-{TIMEFRAME}-{k}.feather") for k in _COMPANION_KINDS):
            continue
        tmp = futures_dir / f".{path.name}.tmp"
        shutil.copy2(path, tmp)
        os.replace(tmp, futures_dir / path.name)
        count += 1
    return count


def _last_bar(path: Path) -> str | None:
    frame = _read_series(path, ["date"])
    return None if frame is None else frame["date"][-1].astimezone(UTC).isoformat()


def _iso(value: datetime | None) -> str | None:
    return None if value is None else value.isoformat()


def _pair_json(pair: PairCoverage) -> dict[str, Any]:
    return {
        "tail": pair.tail.value if pair.tail else None,
        "tail_age_hours": None if pair.tail_age_hours is None else round(pair.tail_age_hours, 1),
        "first_bar": _iso(pair.first_bar),
        "last_bar": _iso(pair.last_bar),
        "funding_rows": pair.funding_rows,
        "futures_rows": pair.futures_rows,
        "mark_rows": pair.mark_rows,
        "holes_recent": pair.holes_recent,
        "holes_older": pair.holes_older,
        "rejected_older": pair.rejected_older,
        "newly_stale": pair.newly_stale,
        "active": pair.active,
        "delisted": pair.delisted,
        "pending": pair.pending,
        "pre_epoch": pair.pre_epoch,
        "blockers": list(pair.blockers),
    }


def run_gate(
    gmx_root: Path,
    stage_root: Path,
    *,
    state: str,
    extract_outcome: str,
    now: datetime | None = None,
    thresholds: SeriesThresholds | None = None,
    report_path: Path | None = None,
) -> dict[str, Any]:
    """Grade the staged companions, promote them on a pass, record the verdict.

    Fails closed: any exception while grading is a ``WITHHELD`` verdict.

    :param gmx_root: ``user_data/data/gmx``.
    :param stage_root: The directory given to :func:`stage_companions`.
    :param state: ``FundingState`` value of the restored funding state.
    :param extract_outcome: How the extraction step ended.
    :param now: Run start; defaults to the current time.
    :param thresholds: Gate thresholds.
    :param report_path: ``data_report.txt`` to append the section to, if any.
    :returns: The ``funding_coverage.json`` content (also written to ``gmx_root``).
    """
    gmx_root, stage_root = Path(gmx_root), Path(stage_root)
    futures = gmx_root / "futures"
    staged = companion_dir(stage_root)
    now = now or datetime.now(UTC)
    thresholds = thresholds or SeriesThresholds()
    previous = None
    try:
        previous = json.loads((gmx_root / COVERAGE_FILE).read_text())
    except (OSError, ValueError):
        pass
    try:
        export_report = json.loads((stage_root / EXPORT_REPORT).read_text())
    except (OSError, ValueError):
        export_report = None

    pairs_json: dict[str, dict[str, Any]] = {}
    reasons: list[str]
    checkpoints: dict[str, Any] = {}
    cutoff: datetime | None = None
    passed = False
    try:
        report = assess_series_coverage(
            futures_dir=futures,
            companion_dir=staged,
            checkpoint_dir=funding_network_dir(gmx_root) / "checkpoints",
            delisted=load_exempt_symbols([gmx_root / "delisted_markets.json"]),
            now=now,
            state=state,
            extract_outcome=extract_outcome,
            export_report=export_report,
            previous=previous,
            thresholds=thresholds,
        )
        passed, reasons = report.passed, report.reasons
        checkpoints, cutoff = dict(report.checkpoints), report.cutoff
        pairs_json = {p.symbol: _pair_json(p) for p in report.pairs}
        if export_report is None:
            passed, reasons = False, ["B1: no staged export report -- the export step did not run"]
    except Exception as exc:  # noqa: BLE001 - fail closed on any grading error
        logger.exception("funding gate crashed")
        passed, reasons = False, [f"gate crashed: {type(exc).__name__}: {exc}"]
        pairs_json = {s: {} for s in _futures_symbols(futures)}

    if passed:
        promote_staged(staged, futures)

    return _publish_verdict(
        gmx_root,
        passed=passed,
        reasons=reasons,
        pairs_json=pairs_json,
        checkpoints=checkpoints,
        cutoff=cutoff,
        state=state,
        extract_outcome=extract_outcome,
        now=now,
        thresholds=thresholds,
        report_path=report_path,
    )


def _publish_verdict(
    gmx_root: Path,
    *,
    passed: bool,
    reasons: list[str],
    pairs_json: dict[str, dict[str, Any]],
    checkpoints: dict[str, Any],
    cutoff: datetime | None,
    state: str,
    extract_outcome: str,
    now: datetime,
    thresholds: SeriesThresholds,
    report_path: Path | None,
) -> dict[str, Any]:
    """Record what is actually shipped in ``futures/`` and write the verdict."""
    futures = gmx_root / "futures"
    for symbol, entry in pairs_json.items():
        entry["shipped_funding_last_bar"] = _last_bar(
            futures / f"{symbol}_USDC_USDC-{TIMEFRAME}-funding_rate.feather"
        )
        entry["shipped_mark_last_bar"] = _last_bar(
            futures / f"{symbol}_USDC_USDC-{TIMEFRAME}-mark.feather"
        )

    tails = [p.get("tail") for p in pairs_json.values()]
    coverage: dict[str, Any] = {
        "schema": 1,
        "status": "PASS" if passed else "WITHHELD",
        "refreshed": passed,
        "generated_at": now.astimezone(UTC).isoformat(),
        "cutoff": _iso(cutoff),
        "funding_epoch": FUNDING_EPOCH.isoformat(),
        "state": state,
        "extract_outcome": extract_outcome,
        "checkpoints": checkpoints,
        "reasons": reasons,
        "thresholds": {k: getattr(thresholds, k) for k in thresholds.__dataclass_fields__},
        "summary": {
            "pairs": len(pairs_json),
            **{t.value.lower(): tails.count(t.value) for t in TailStatus},
            "newly_stale": sum(1 for p in pairs_json.values() if p.get("newly_stale")),
        },
        "pairs": pairs_json,
    }
    coverage_path = gmx_root / COVERAGE_FILE
    tmp = coverage_path.with_name(coverage_path.name + ".tmp")
    tmp.write_text(json.dumps(coverage, indent=2, sort_keys=True))
    os.replace(tmp, coverage_path)

    if report_path is not None:
        write_report_section(Path(report_path), coverage)
    return coverage


def write_withheld_verdict(
    gmx_root: Path,
    reason: str,
    *,
    state: str,
    extract_outcome: str,
    now: datetime,
    report_path: Path | None = None,
) -> dict[str, Any]:
    """Record a ``WITHHELD`` verdict without grading anything.

    For when the gate did not produce a current verdict of its own (it crashed,
    was killed, or exited without writing one). Replaces whatever verdict was
    restored from the previous release, so the coverage record, the report and
    the release notes can never claim a refresh that did not happen. ``futures/``
    is left exactly as it is.
    """
    gmx_root = Path(gmx_root)
    return _publish_verdict(
        gmx_root,
        passed=False,
        reasons=[reason],
        pairs_json={s: {} for s in _futures_symbols(gmx_root / "futures")},
        checkpoints={},
        cutoff=None,
        state=state,
        extract_outcome=extract_outcome,
        now=now,
        thresholds=SeriesThresholds(),
        report_path=report_path,
    )


def verdict_is_current(gmx_root: Path, run_start: datetime) -> dict[str, Any] | None:
    """Return the verdict iff this run wrote it (``generated_at`` is the run start)."""
    try:
        data = json.loads((Path(gmx_root) / COVERAGE_FILE).read_text())
        stamp = datetime.fromisoformat(data["generated_at"])
    except (OSError, ValueError, KeyError, TypeError):
        return None
    return data if stamp.astimezone(UTC) == run_start.astimezone(UTC) else None


def _flagged_pairs(coverage: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    """LAGGING/STALE pairs, stalest first."""
    flagged = [
        (s, p)
        for s, p in coverage.get("pairs", {}).items()
        if p.get("tail") in ("LAGGING", "STALE")
    ]
    return sorted(flagged, key=lambda item: -(item[1].get("tail_age_hours") or 0))


def _fmt_bar(value: str | None) -> str:
    return "none" if not value else value[:16].replace("T", " ") + "Z"


def _status_line(coverage: dict[str, Any]) -> str:
    if coverage.get("status") == "PASS":
        return "Funding and mark companions refreshed and published."
    return "Funding and mark were NOT refreshed in this release (WITHHELD)."


def format_report_lines(coverage: dict[str, Any]) -> list[str]:
    """Report section lines for ``data_report.txt``."""
    summary = coverage.get("summary", {})
    lines = [
        "",
        REPORT_HEADER,
        f"  Status: {coverage.get('status')} -- {_status_line(coverage)}",
        f"  Funding state: {coverage.get('state')}; extraction: {coverage.get('extract_outcome')}",
        f"  Pairs: {summary.get('pairs', 0)} -- CURRENT {summary.get('current', 0)}, "
        f"LAGGING {summary.get('lagging', 0)}, STALE {summary.get('stale', 0)}, "
        f"newly stale {summary.get('newly_stale', 0)}",
    ]
    for reason in coverage.get("reasons", []):
        lines.append(f"  BLOCKED: {reason}")
    flagged = _flagged_pairs(coverage)
    if flagged:
        lines.append("  Per-market tails (newest funding bar vs cutoff; never forward-filled):")
        for symbol, pair in flagged:
            note = " (newly stale)" if pair.get("newly_stale") else ""
            lines.append(
                f"    {symbol}: {pair['tail']} -- newest bar {_fmt_bar(pair.get('last_bar'))}, "
                f"{pair.get('tail_age_hours'):.0f}h behind{note}"
            )
    if not coverage.get("refreshed"):
        lines.append("  Last published bars (previous release's files, unchanged):")
        for symbol, pair in sorted(coverage.get("pairs", {}).items()):
            lines.append(
                f"    {symbol}: funding {_fmt_bar(pair.get('shipped_funding_last_bar'))}, "
                f"mark {_fmt_bar(pair.get('shipped_mark_last_bar'))}"
            )
    return lines


def write_report_section(report_path: Path, coverage: dict[str, Any]) -> None:
    """Append (or replace) the funding section of ``data_report.txt``."""
    existing = report_path.read_text().splitlines() if report_path.exists() else []
    kept: list[str] = []
    skipping = False
    for line in existing:
        if line == REPORT_HEADER:
            skipping = True
            continue
        if skipping and line.startswith("## "):
            skipping = False
        if not skipping:
            kept.append(line)
    while kept and kept[-1] == "":
        kept.pop()
    report_path.write_text("\n".join([*kept, *format_report_lines(coverage)]) + "\n")


def format_release_notes(coverage: dict[str, Any]) -> str:
    """Markdown section for the GitHub release notes."""
    lines = ["### Funding & mark companions", ""]
    if coverage.get("status") == "PASS":
        lines.append(
            "`futures/*-1h-funding_rate.feather` and `*-1h-mark.feather` were refreshed in this release."
        )
    else:
        lines.append(
            "**Funding and mark were NOT refreshed in this release (WITHHELD).** The "
            "companions in `futures/` are the previous release's, unchanged; the first-ever "
            "run ships none."
        )
        lines.extend(f"- {reason}" for reason in coverage.get("reasons", []))
        lines += ["", "Last published bars:", ""]
        lines += ["| Pair | Funding | Mark |", "|---|---|---|"]
        for symbol, pair in sorted(coverage.get("pairs", {}).items()):
            lines.append(
                f"| {symbol} | {_fmt_bar(pair.get('shipped_funding_last_bar'))} | "
                f"{_fmt_bar(pair.get('shipped_mark_last_bar'))} |"
            )
    flagged = _flagged_pairs(coverage)
    if flagged:
        lines += [
            "",
            f"{len(flagged)} market(s) have a lagging or stale funding tail (no on-chain "
            "event since; the tail is not forward-filled):",
            "",
            "| Pair | Tail | Newest funding bar | Behind cutoff |",
            "|---|---|---|---|",
        ]
        for symbol, pair in flagged:
            mark = " (newly stale)" if pair.get("newly_stale") else ""
            lines.append(
                f"| {symbol} | {pair['tail']}{mark} | {_fmt_bar(pair.get('last_bar'))} | "
                f"{pair.get('tail_age_hours'):.0f}h |"
            )
    return "\n".join(lines) + "\n"


def github_warnings(coverage: dict[str, Any]) -> list[str]:
    """One ``::warning::`` line per lagging/stale pair."""
    return [
        f"::warning::funding tail {pair['tail']}: {symbol} newest bar "
        f"{_fmt_bar(pair.get('last_bar'))}, {pair.get('tail_age_hours'):.0f}h behind cutoff"
        + (" (newly stale)" if pair.get("newly_stale") else "")
        for symbol, pair in _flagged_pairs(coverage)
    ]
