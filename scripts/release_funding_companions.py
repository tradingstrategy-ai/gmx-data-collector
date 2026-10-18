#!/usr/bin/env python3
"""Daily-release helper for the hourly funding + mark companions (#56).

The workflow calls these subcommands; the logic lives in
``gmx_historical_data.funding_state`` / ``funding_release`` / ``series_coverage``.

* ``seed``   one-off: seed the funding state from the ``data/full-history`` branch.
* ``state``  classify the restored funding state; export ``FUNDING_STATE``.
* ``export`` stage the 1h funding + mark feathers (never writes ``futures/``).
* ``gate``   grade the staged files; promote on pass; write ``funding_coverage.json``.
* ``notes``  print the release-notes section for a ``funding_coverage.json``.

Exit codes: ``seed``/``state``/``export``/``notes`` are fail-soft (0) so the rest of
the release proceeds; ``gate`` exits 1 when the companions were withheld.
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

from gmx_historical_data import funding_release
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


def cmd_gate(args: argparse.Namespace) -> int:
    gmx_root = Path(args.gmx_root)
    now = datetime.fromisoformat(args.run_start) if args.run_start else datetime.now(UTC)
    coverage = funding_release.run_gate(
        gmx_root,
        Path(args.stage_dir),
        state=args.state,
        extract_outcome=args.extract_outcome,
        now=now,
        thresholds=SeriesThresholds(stale_tail_blocks=args.stale_tail_blocks),
        report_path=Path(args.report) if args.report else None,
    )
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


def cmd_notes(args: argparse.Namespace) -> int:
    try:
        coverage = json.loads(Path(args.coverage).read_text())
    except (OSError, ValueError):
        print("### Funding & mark companions\n\nNo funding coverage record was produced.\n")
        return 0
    print(funding_release.format_release_notes(coverage))
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

    notes = sub.add_parser("notes", help="print the release-notes section")
    notes.add_argument("--coverage", required=True)
    notes.set_defaults(func=cmd_notes)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
