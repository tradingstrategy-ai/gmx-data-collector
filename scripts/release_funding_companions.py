#!/usr/bin/env python3
"""Daily-release helper for the hourly funding + mark companions (#56).

The workflow calls these subcommands; the logic lives in
``gmx_historical_data.funding_state`` / ``funding_release`` / ``series_coverage``.

* ``seed``   one-off: seed the funding state from the ``data/full-history`` branch.
* ``state``  classify the restored funding state; export ``FUNDING_STATE``.
* ``export`` stage the 1h funding + mark feathers (never writes ``futures/``).
* ``gate``   grade the staged files; promote on pass; write ``funding_coverage.json``.
* ``ensure-verdict`` backstop: no verdict written by this run means WITHHELD.
* ``notes``  print the release-notes section for a ``funding_coverage.json``.
* ``checkpoints`` / ``catchup-report``  funding-catchup.yml: snapshot the extraction
  checkpoints before the run, then report progress and whether the state is worth uploading.

Exit codes: ``seed``/``state``/``export``/``notes`` are fail-soft (0) so the rest of
the release proceeds; ``gate`` exits 1 when the companions were withheld;
``ensure-verdict`` exits 0 once a current verdict is in place, whatever it says.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
from datetime import UTC, datetime
from pathlib import Path

from gmx_historical_data import funding_catchup, funding_release
from gmx_historical_data.funding_state import (
    NETWORK,
    SeedError,
    assess_funding_state,
    seed_funding_state,
)
from gmx_historical_data.series_coverage import SeriesThresholds

BRANCH_FUNDING_PATH = f"historical/funding/{NETWORK}"


def _github_env(**values: str) -> None:
    path = os.environ.get("GITHUB_ENV")
    if path:
        with open(path, "a") as handle:
            handle.writelines(f"{k}={v}\n" for k, v in values.items())


def _fetch_branch_tree(ref: str, dest: Path) -> dict[str, str]:
    """Fetch ``ref`` and extract ``historical/funding/arbitrum`` into ``dest``."""
    subprocess.run(["git", "fetch", "--depth=1", "origin", ref], check=True)
    sha = subprocess.run(
        ["git", "rev-parse", "FETCH_HEAD"], check=True, capture_output=True, text=True
    ).stdout.strip()
    archive = subprocess.Popen(
        ["git", "archive", "FETCH_HEAD", BRANCH_FUNDING_PATH], stdout=subprocess.PIPE
    )
    subprocess.run(["tar", "-x", "-C", str(dest)], stdin=archive.stdout, check=True)
    if archive.wait() != 0:
        raise SeedError(f"git archive of {BRANCH_FUNDING_PATH} on {ref} failed")
    return {"ref": ref, "commit": sha, "path": BRANCH_FUNDING_PATH}


def cmd_seed(args: argparse.Namespace) -> int:
    gmx_root = Path(args.gmx_root)
    work: Path | None = None
    try:
        if args.source_dir:
            source, info = Path(args.source_dir), {"source_dir": str(args.source_dir)}
        else:
            work = Path(tempfile.mkdtemp(prefix="funding-seed-src-", dir=gmx_root.parent.parent))
            info = _fetch_branch_tree(args.ref, work)
            source = work / BRANCH_FUNDING_PATH
        result = seed_funding_state(source, gmx_root, source_info=info)
    except (SeedError, subprocess.CalledProcessError, OSError) as exc:
        print(f"::error::funding seed failed: {exc}")
        print("Continuing unseeded; the companions will be withheld.")
        return 0
    finally:
        if work is not None:
            shutil.rmtree(work, ignore_errors=True)
    if result.status == "skipped_existing":
        print("::notice::funding state already present; seed skipped (newer state wins).")
    else:
        manifest = result.manifest or {}
        print(
            f"Seeded funding state: {manifest.get('symbols')} symbols, "
            f"{manifest.get('files')} files, checkpoints {manifest.get('checkpoints')}"
        )
    return 0


def cmd_state(args: argparse.Namespace) -> int:
    state = assess_funding_state(Path(args.gmx_root))
    print(f"funding state: {state.value}")
    if state.value != "ready":
        print(f"::warning::funding state is {state.value}; incremental extraction will not run")
    _github_env(FUNDING_STATE=state.value)
    return 0


def cmd_export(args: argparse.Namespace) -> int:
    report = funding_release.stage_companions(Path(args.gmx_root), Path(args.stage_dir))
    print(
        f"staged companions: {len(report['failures'])} failure(s), "
        f"{len(report['reconciliations'])} funding file(s) reconciled"
    )
    for failure in report["failures"]:
        print(
            f"::warning::export failure {failure['symbol']}/{failure['timeframe']}: "
            f"{failure['reason']}"
        )
    return 0


def _run_start(value: str | None) -> datetime:
    return datetime.fromisoformat(value) if value else datetime.now(UTC)


def cmd_gate(args: argparse.Namespace) -> int:
    gmx_root = Path(args.gmx_root)
    now = _run_start(args.run_start)
    report = Path(args.report) if args.report else None
    try:
        coverage = funding_release.run_gate(
            gmx_root,
            Path(args.stage_dir),
            state=args.state,
            extract_outcome=args.extract_outcome,
            now=now,
            thresholds=SeriesThresholds(stale_tail_blocks=args.stale_tail_blocks),
            report_path=report,
        )
    except Exception as exc:  # noqa: BLE001 - fail closed, and never leave a stale verdict
        print(f"::error::funding gate crashed: {type(exc).__name__}: {exc}")
        _withhold(args, now, f"gate crashed: {type(exc).__name__}: {exc}", report)
        return 1
    for line in funding_release.github_warnings(coverage):
        print(line)
    ok = coverage["status"] == "PASS"
    if ok:
        print(f"funding gate PASS: companions promoted ({coverage['summary']})")
    else:
        for reason in coverage["reasons"]:
            print(f"::error::funding gate: {reason}")
        print("funding gate WITHHELD: futures/ keeps the previously published companions.")
    _github_env(FUNDING_GATE="pass" if ok else "fail")
    return 0 if ok else 1


def _withhold(args: argparse.Namespace, now: datetime, reason: str, report: Path | None) -> bool:
    """Replace any restored verdict with a WITHHELD one. ``True`` if it was recorded."""
    _github_env(FUNDING_GATE="fail")
    try:
        funding_release.write_withheld_verdict(
            Path(args.gmx_root),
            reason,
            state=args.state,
            extract_outcome=args.extract_outcome,
            now=now,
            report_path=report,
        )
    except Exception as exc:  # noqa: BLE001 - last resort: the caller removes the stale file
        print(f"::error::could not record the withheld verdict: {exc}")
        return False
    return True


def cmd_ensure_verdict(args: argparse.Namespace) -> int:
    """Backstop after ``gate``: no current verdict means WITHHELD, never yesterday's."""
    gmx_root = Path(args.gmx_root)
    now = _run_start(args.run_start)
    report = Path(args.report) if args.report else None
    verdict = funding_release.verdict_is_current(gmx_root, now)
    if verdict is not None and args.exit_code in (0, 1):
        ok = verdict.get("status") == "PASS"
        _github_env(FUNDING_GATE="pass" if ok else "fail")
        return 0
    print(
        f"::error::funding gate exit {args.exit_code} left no current verdict; companions withheld."
    )
    recorded = _withhold(
        args, now, f"gate did not produce a verdict (exit {args.exit_code})", report
    )
    # 0 = a correct, current WITHHELD verdict is in place (FUNDING_GATE=fail carries
    # the failure); nonzero only if even that could not be written.
    return 0 if recorded else 1


def cmd_notes(args: argparse.Namespace) -> int:
    try:
        coverage = json.loads(Path(args.coverage).read_text())
    except (OSError, ValueError):
        print(
            "### Funding & mark companions\n\n**Funding and mark were NOT refreshed in this "
            "release:** no funding coverage record was produced. The companions in `futures/` "
            "are the previous release's, unchanged.\n"
        )
        return 0
    print(funding_release.format_release_notes(coverage))
    return 0


def _github_output(**values: str) -> None:
    path = os.environ.get("GITHUB_OUTPUT")
    if path:
        with open(path, "a") as handle:
            handle.writelines(f"{k}={v}\n" for k, v in values.items())


def cmd_checkpoints(args: argparse.Namespace) -> int:
    positions = funding_catchup.snapshot(Path(args.gmx_root))
    Path(args.out).write_text(json.dumps(positions, indent=2))
    print(f"checkpoints before: {json.dumps(positions)}")
    return 0


def cmd_catchup_report(args: argparse.Namespace) -> int:
    try:
        before = json.loads(Path(args.before).read_text())
    except (OSError, ValueError):
        before = None
    if before is None:
        # Nothing was recorded, so nothing can be shown to have advanced.
        print("::warning::no checkpoint snapshot from before the run; nothing will be uploaded.")
        _github_output(head_reached="false")
        return 0
    report = funding_catchup.build_report(
        before,
        Path(args.gmx_root),
        now=datetime.now(UTC),
        extract_outcome=args.extract_outcome,
    )
    print(report.markdown)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a") as handle:
            handle.write(report.markdown)
    _github_output(head_reached=str(report.head_reached).lower())
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)

    seed = sub.add_parser("seed", help="seed the funding state from a branch")
    seed.add_argument("--gmx-root", required=True)
    seed.add_argument("--ref", default="data/full-history")
    seed.add_argument("--source-dir", help="use a local directory instead of fetching --ref")
    seed.set_defaults(func=cmd_seed)

    state = sub.add_parser("state", help="classify the restored funding state")
    state.add_argument("--gmx-root", required=True)
    state.set_defaults(func=cmd_state)

    export = sub.add_parser("export", help="stage funding + mark feathers")
    export.add_argument("--gmx-root", required=True)
    export.add_argument("--stage-dir", required=True)
    export.set_defaults(func=cmd_export)

    gate = sub.add_parser("gate", help="grade, promote or withhold")
    gate.add_argument("--gmx-root", required=True)
    gate.add_argument("--stage-dir", required=True)
    gate.add_argument("--state", required=True)
    gate.add_argument("--extract-outcome", required=True)
    gate.add_argument("--run-start", help="ISO timestamp of the run start (default: now)")
    gate.add_argument("--report", help="data_report.txt to add the funding section to")
    gate.add_argument("--stale-tail-blocks", action="store_true")
    gate.set_defaults(func=cmd_gate)

    ensure = sub.add_parser("ensure-verdict", help="withhold unless gate wrote a current verdict")
    ensure.add_argument("--gmx-root", required=True)
    ensure.add_argument("--state", required=True)
    ensure.add_argument("--extract-outcome", required=True)
    ensure.add_argument("--run-start", required=True)
    ensure.add_argument("--exit-code", type=int, required=True)
    ensure.add_argument("--report")
    ensure.set_defaults(func=cmd_ensure_verdict)

    notes = sub.add_parser("notes", help="print the release-notes section")
    notes.add_argument("--coverage", required=True)
    notes.set_defaults(func=cmd_notes)

    checkpoints = sub.add_parser("checkpoints", help="record the extraction checkpoints")
    checkpoints.add_argument("--gmx-root", required=True)
    checkpoints.add_argument("--out", required=True)
    checkpoints.set_defaults(func=cmd_checkpoints)

    catchup = sub.add_parser("catchup-report", help="summarise a catch-up run")
    catchup.add_argument("--gmx-root", required=True)
    catchup.add_argument("--before", required=True)
    catchup.add_argument("--extract-outcome", required=True)
    catchup.set_defaults(func=cmd_catchup_report)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
