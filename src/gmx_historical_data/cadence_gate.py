"""Release-time cadence manifest generation and regression gate.

The daily release workflow does not run ``export-freqtrade`` -- it writes
its futures feathers via ``scripts/collect_daily_snapshot.py`` -- so the
manifest that :meth:`~gmx_historical_data.freqtrade_exporter.FreqtradeExporter.export_candles`
publishes for local exports is regenerated here from the files that are
actually about to ship, then diffed against the previous release's copy.

This lives in the package rather than inline in ``release-data.yml``
because a heredoc cannot be unit-tested: the first cut of the gate
shipped a suppression rule that silently excluded every fresh outage --
exactly the defect it exists to catch (issue #29) -- and no YAML-substring
test could have seen it.

The gate never blocks the release.  Detection runs before packaging, and a
surviving regression annotates the manifest, files an issue, and defers the
job's failure to the last step, so consumers still get the day's data.

:data:`EXIT_REGRESSION` is kept distinct from :data:`EXIT_ERROR` so the
workflow can retry a check that could not *run* while shipping-and-alerting
on a check that ran and found a real regression.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import polars as pl

from gmx_historical_data.atomic_parquet import DATA_DEFECT_ERRORS
from gmx_historical_data.freqtrade_exporter import CADENCE_MANIFEST_NAME, FreqtradeExporter
from gmx_historical_data.ohlcv_validation import find_cadence_breaks, parse_timeframe_interval

logger = logging.getLogger(__name__)

#: The check ran and found nothing new.
EXIT_OK = 0

#: The check could not run (missing/unreadable manifest).  The workflow
#: retries this; it is not evidence about the data either way.
EXIT_ERROR = 1

#: The check ran and found a genuine new break.  Deterministic across
#: retries, so the workflow stops retrying and ships-and-alerts instead.
EXIT_REGRESSION = 2

#: Default locations used by ``release-data.yml``.  Overridable so the
#: tests never touch ``/tmp`` or the real data tree.
DEFAULT_FUTURES_DIR = Path("./user_data/data/gmx/futures")
DEFAULT_BASELINE_PATH = Path("/tmp/gmx-cadence-baseline.json")
DEFAULT_REGRESSIONS_PATH = Path("/tmp/gmx-cadence-regressions.json")

#: Candle files only.  ``-mark``/``-index`` duplicate the candle series and
#: ``-funding_rate`` is deliberately not cadence-checked (its transform
#: drops null rates by design).
_FUTURES_FILE_PATTERN = re.compile(r"-(1m|5m|15m|1h|4h|1d)-futures\.feather$")

#: Symbol extractor, character-for-character the one the workflow's
#: ``Record delisted markets`` and ``Validate futures candle file
#: integrity`` steps already use, so the cadence gate exempts exactly the
#: same files those do rather than inventing a second convention.
_SYMBOL_FROM_FILENAME = re.compile(r"^(.+?)_[^_]+_[^_]+-(?:1m|5m|15m|1h|4h|1d)-")

#: Roster of markets whose history is frozen, written by the workflow's
#: ``Record delisted markets`` step.
DEFAULT_DELISTED_ROSTER = Path("./user_data/data/gmx/delisted_markets.json")


@dataclass(frozen=True, slots=True)
class CadenceRegression:
    """One file that gained a cadence break this run.

    :param file: Feather filename, as keyed in the manifest.
    :param detail: Human-readable one-liner for the log and the issue body.
    """

    file: str
    detail: str

    def as_dict(self) -> dict[str, str]:
        """:returns: JSON-serialisable form, as written to the regressions file."""
        return {"file": self.file, "detail": self.detail}


@dataclass(frozen=True, slots=True)
class CadenceScan:
    """The outcome of scanning a futures directory.

    :param entries: Manifest entries keyed by feather filename.
    :param skipped: Filenames that could not be read this run.  They are
        removed from the published manifest rather than left carrying a
        previous run's entry, so "present" keeps meaning "checked on the
        run that stamped ``generated_at``".
    """

    entries: dict[str, dict] = field(default_factory=dict)
    skipped: set[str] = field(default_factory=set)


def symbol_from_filename(filename: str) -> str | None:
    """Extract the market symbol from an exported feather's filename.

    :param filename: e.g. ``'MEGA_USDC_USDC-1h-futures.feather'``.
    :returns: The upper-cased symbol, or ``None`` when the name does not
        match the exported-candle convention.
    """
    match = _SYMBOL_FROM_FILENAME.match(filename)
    return match.group(1).upper() if match else None


def load_exempt_symbols(roster_paths: Iterable[Path]) -> set[str]:
    """Union every delisted-market roster into one exemption set.

    More than one roster matters because the workflow rewrites
    ``delisted_markets.json`` *before* collection, and a market that has
    just relisted drops off it at that moment -- which is exactly the run
    whose relisting seam needs exempting.  Unioning the previous release's
    roster covers that single run, after which the market is live and the
    ordinary no-growth rule applies again.

    An unreadable roster yields no exemptions rather than raising: a
    missing exemption files a false-alarm issue, while a raise here would
    block the release, and this gate never blocks the release.

    :param roster_paths: Roster files in ``{"symbols": [...]}`` form.
    :returns: Upper-cased symbols exempt from regression comparison.
    """
    exempt: set[str] = set()
    for path in roster_paths:
        if not path.exists():
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            exempt.update(str(symbol).upper() for symbol in payload.get("symbols", []))
        except (json.JSONDecodeError, OSError, AttributeError, TypeError) as exc:
            logger.warning("%s: unreadable delisted roster, ignoring: %s", path, exc)
    return exempt


def _instant_key(value: object) -> object:
    """Normalise a manifest timestamp so two spellings of one instant match.

    The manifest serialises whole-bar UTC timestamps consistently today, so
    raw string equality happens to work -- but a future change (``Z`` versus
    ``+00:00``, or added precision) would make every inherited break look
    new at once, and there is no second filter left to catch that.

    :param value: An ISO-8601 string from a manifest break entry.
    :returns: A POSIX timestamp when parseable, else the value unchanged so
        comparison degrades to the previous string behaviour.
    """
    if not isinstance(value, str):
        return value
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return value


def _gap_key(gap: dict) -> tuple[object, object]:
    return (_instant_key(gap.get("before")), _instant_key(gap.get("after")))


def regression_marker(filename: str) -> str:
    """Build the hidden marker that identifies a file's tracking issue.

    Matching on this exact string is what makes issue dedupe safe.
    GitHub's issue search tokenises, and every candle filename shares
    ``usdc``/``futures``/``feather`` and a timeframe token with every other
    one, so relevance ranking can hand back a different symbol's ticket.

    :param filename: Feather filename the issue tracks.
    :returns: An HTML comment marker, embedded in the issue body.
    """
    return f"<!-- cadence-regression:{filename} -->"


def find_issue_number_for_marker(issues: Iterable[dict], marker: str) -> int | None:
    """Find the open issue whose body carries ``marker``.

    :param issues: Issues as returned by ``gh issue list --json number,body``.
    :param marker: Marker from :func:`regression_marker`.
    :returns: The issue number, or ``None`` when no issue carries it.
    """
    for issue in issues:
        if marker in (issue.get("body") or ""):
            return issue.get("number")
    return None


def find_cadence_regressions(
    baseline: dict,
    current: dict,
    *,
    exempt_symbols: Iterable[str] = (),
) -> list[CadenceRegression]:
    """Diff two cadence manifests and report every newly-introduced break.

    The rule is one sentence: **for a file the previous release already
    carried, the set of cadence breaks must not grow.**  Both comparison
    branches below enforce that same rule; they differ only in what
    evidence they have.

    There is deliberately **no timestamp-based exclusion**.  An earlier cut
    of this gate skipped any gap whose ``before`` fell at or after the
    baseline's newest bar, on the theory that the previous release could
    not have covered it.  Because releases run daily, that timestamp is
    always ~24h old, so the rule suppressed every gap an overnight outage
    could produce -- the entire class of defect this gate exists to catch.
    A hole in freshly-collected data is the regression; a hole that appears
    inside already-published history is a worse one.  Only a file the
    baseline never carried is skipped, because there is genuinely nothing
    to compare it against.

    Delisted markets are exempt.  Their history freezes at delisting and
    resumes at relisting, leaving one large legitimate seam (MEGA 1h, 213
    bars -- PR #23 deliberately retains delisted market history), which is
    a listing artifact rather than a collection defect.  The workflow's
    existing ``Validate futures candle file integrity`` step exempts the
    same files from the same roster; see :func:`load_exempt_symbols`.

    :param baseline: Previous release's manifest (``{"files": {...}}``).
    :param current: This run's manifest.
    :param exempt_symbols: Market symbols whose files are skipped entirely
        -- the delisted/relisted roster.
    :returns: Regressions in filename order; empty when nothing grew.
    """
    base_files = baseline.get("files", {}) or {}
    exempt = {str(symbol).upper() for symbol in exempt_symbols}
    regressions: list[CadenceRegression] = []

    for name, entry in sorted((current.get("files", {}) or {}).items()):
        before = base_files.get(name)
        if not isinstance(before, dict):
            # New file, or first run after rollout: nothing to compare.
            continue

        symbol = symbol_from_filename(name)
        if symbol is not None and symbol in exempt:
            logger.info("%s: delisted/relisted market, exempt from the cadence gate", name)
            continue

        before_total = before.get("breaks_total")
        if before_total is None:
            # Malformed or legacy baseline entry. Degrade to "nothing to
            # compare" rather than raising: the publish steps carry no
            # ``if: always()``, so a crash here would block the whole
            # release, which is the opposite of this gate's contract.
            logger.warning("%s: baseline entry has no breaks_total; skipping comparison", name)
            continue

        if before.get("truncated") or entry.get("truncated"):
            # Same no-growth rule, weaker evidence: at least one side's
            # break list is capped at MAX_BREAKS_PER_FILE, so the exact
            # gaps cannot be set-differenced. ``breaks_total`` is always
            # exact even when the list is truncated, so compare that. This
            # is intentionally the only place the two branches differ.
            entry_total = entry.get("breaks_total", 0)
            if entry_total > before_total:
                regressions.append(
                    CadenceRegression(
                        file=name,
                        detail=(
                            f"{name}: new cadence break (total {before_total} -> "
                            f"{entry_total}, truncated lists)"
                        ),
                    )
                )
            continue

        known = {_gap_key(gap) for gap in (before.get("breaks") or [])}
        for gap in entry.get("breaks") or []:
            if _gap_key(gap) in known:
                continue
            regressions.append(
                CadenceRegression(
                    file=name,
                    detail=(
                        f"{name}: new cadence break - {gap.get('missing_bars')} bar(s) "
                        f"missing between {gap.get('before')} and {gap.get('after')}"
                    ),
                )
            )

    return regressions


def build_cadence_manifest(futures_dir: Path) -> CadenceScan:
    """Scan every published candle feather and build its manifest entry.

    One unreadable file is skipped with a warning rather than aborting the
    scan, matching the "record, don't reject" stance the export path takes:
    a single corrupt or legacy feather must not cost the other ~700 files
    their cadence record.

    A skipped file is also reported so the caller can *remove* its previous
    manifest entry: the manifest merges rather than replaces, so leaving the
    entry behind would republish yesterday's verdict under today's
    ``generated_at`` and claim a file was checked when it was not.

    :param futures_dir: Directory holding the exported ``*-futures.feather``.
    :returns: A :class:`CadenceScan` of entries plus skipped filenames.
    """
    entries: dict[str, dict] = {}
    skipped: set[str] = set()
    for path in sorted(futures_dir.glob("*-futures.feather")):
        match = _FUTURES_FILE_PATTERN.search(path.name)
        if match is None:
            continue
        timeframe = match.group(1)
        expected_interval = parse_timeframe_interval(timeframe)
        try:
            frame = pl.read_ipc(path, memory_map=False)
        except DATA_DEFECT_ERRORS as exc:
            logger.warning("%s: unreadable, excluded from the cadence manifest: %s", path.name, exc)
            skipped.add(path.name)
            continue
        if frame.is_empty() or "date" not in frame.columns:
            logger.warning("%s: no candle rows, excluded from the cadence manifest", path.name)
            skipped.add(path.name)
            continue

        breaks = find_cadence_breaks(
            frame, timestamp_column="date", expected_interval=expected_interval
        )
        entries[path.name] = FreqtradeExporter._cadence_manifest_entry(
            timeframe,
            expected_interval,
            frame.height,
            frame.get_column("date").min(),
            frame.get_column("date").max(),
            breaks,
        )
    return CadenceScan(entries=entries, skipped=skipped)


def _load_json(path: Path) -> dict | list:
    return json.loads(path.read_text(encoding="utf-8"))


def _command_build(args: argparse.Namespace) -> int:
    scan = build_cadence_manifest(args.futures_dir)
    if not scan.entries:
        print("ERROR: no futures feathers found to build a cadence manifest.", file=sys.stderr)
        return EXIT_ERROR

    exporter = FreqtradeExporter(args.futures_dir, args.futures_dir)
    manifest_path = exporter.write_cadence_manifest(
        args.futures_dir, scan.entries, drop=scan.skipped
    )
    total = sum(entry["breaks_total"] for entry in scan.entries.values())
    gapped = sum(1 for entry in scan.entries.values() if entry["breaks_total"])
    print(
        f"Wrote {manifest_path.name}: {len(scan.entries)} files, "
        f"{gapped} with a break, {total} break(s) total."
    )
    if scan.skipped:
        print(
            f"Dropped {len(scan.skipped)} unreadable file(s) from the manifest "
            f"(absence means not-checked): {', '.join(sorted(scan.skipped))}"
        )
    return EXIT_OK


def _command_check(args: argparse.Namespace) -> int:
    if not args.manifest.exists():
        print("ERROR: export produced no cadence manifest.", file=sys.stderr)
        return EXIT_ERROR

    try:
        baseline = _load_json(args.baseline) if args.baseline.exists() else {"files": {}}
        current = _load_json(args.manifest)
    except (json.JSONDecodeError, OSError) as exc:
        print(f"ERROR: cannot read cadence manifests: {exc}", file=sys.stderr)
        return EXIT_ERROR

    exempt = load_exempt_symbols(args.delisted_roster)
    if exempt:
        print(f"Exempt (delisted/relisted) markets: {', '.join(sorted(exempt))}")
    regressions = find_cadence_regressions(baseline, current, exempt_symbols=exempt)
    args.regressions.write_text(
        json.dumps([item.as_dict() for item in regressions], indent=2), encoding="utf-8"
    )

    if regressions:
        print("ERROR: cadence regressions detected in this release:", file=sys.stderr)
        for item in regressions[:20]:
            print(f"  - {item.detail}", file=sys.stderr)
        if len(regressions) > 20:
            print(f"  ... and {len(regressions) - 20} more", file=sys.stderr)
        return EXIT_REGRESSION

    files = current.get("files", {}) or {}
    total_breaks = sum(entry.get("breaks_total", 0) for entry in files.values())
    print(f"Cadence OK: {len(files)} files checked, {total_breaks} inherited break(s), 0 new.")
    return EXIT_OK


def _command_annotate(args: argparse.Namespace) -> int:
    try:
        regressions = _load_json(args.regressions)
        manifest = _load_json(args.manifest)
    except (json.JSONDecodeError, OSError) as exc:
        print(f"ERROR: cannot annotate cadence manifest: {exc}", file=sys.stderr)
        return EXIT_ERROR

    files = manifest.get("files", {}) or {}
    stamped = 0
    # Stamp the regression onto the entry itself so a consumer reading only
    # the shipped tarball can quarantine this one file rather than
    # distrusting (or crashing on) the whole release.
    for name in sorted({item["file"] for item in regressions}):
        entry = files.get(name)
        if isinstance(entry, dict):
            entry["regressed_from"] = args.previous_tag
            stamped += 1

    args.manifest.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(
        f"Annotated {stamped} manifest entry(ies) with regressed_from={args.previous_tag}.",
        file=sys.stderr,
    )
    return EXIT_OK


def _command_find_issue(args: argparse.Namespace) -> int:
    try:
        issues = _load_json(args.issues)
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning("cannot read open-issue listing, treating as none: %s", exc)
        issues = []

    number = find_issue_number_for_marker(issues, regression_marker(args.file))
    print("" if number is None else number)
    return EXIT_OK


def main(argv: Sequence[str] | None = None) -> int:
    """Entry point for ``python -m gmx_historical_data.cadence_gate``.

    :param argv: Argument vector; defaults to ``sys.argv[1:]``.
    :returns: Process exit code -- see :data:`EXIT_OK`, :data:`EXIT_ERROR`,
        :data:`EXIT_REGRESSION`.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    build = sub.add_parser("build", help="regenerate the cadence manifest from published feathers")
    build.add_argument("--futures-dir", type=Path, default=DEFAULT_FUTURES_DIR)
    build.set_defaults(func=_command_build)

    check = sub.add_parser("check", help="diff this run's manifest against the previous release's")
    check.add_argument("--baseline", type=Path, default=DEFAULT_BASELINE_PATH)
    check.add_argument("--manifest", type=Path, default=DEFAULT_FUTURES_DIR / CADENCE_MANIFEST_NAME)
    check.add_argument("--regressions", type=Path, default=DEFAULT_REGRESSIONS_PATH)
    check.add_argument(
        "--delisted-roster",
        type=Path,
        action="append",
        default=None,
        help=(
            "delisted-market roster to exempt (repeatable; defaults to the "
            "live roster). Pass the previous release's roster too so a "
            "relisting seam is exempt on the run that relists."
        ),
    )
    check.set_defaults(func=_command_check)

    annotate = sub.add_parser("annotate", help="stamp regressed_from onto the affected entries")
    annotate.add_argument(
        "--manifest", type=Path, default=DEFAULT_FUTURES_DIR / CADENCE_MANIFEST_NAME
    )
    annotate.add_argument("--regressions", type=Path, default=DEFAULT_REGRESSIONS_PATH)
    annotate.add_argument("--previous-tag", default="unknown")
    annotate.set_defaults(func=_command_annotate)

    find_issue = sub.add_parser("find-issue", help="exact-marker lookup of an existing issue")
    find_issue.add_argument("--issues", type=Path, required=True)
    find_issue.add_argument("--file", required=True)
    find_issue.set_defaults(func=_command_find_issue)

    args = parser.parse_args(argv)
    if getattr(args, "delisted_roster", None) is None and args.command == "check":
        args.delisted_roster = [DEFAULT_DELISTED_ROSTER]
    return args.func(args)


if __name__ == "__main__":  # pragma: no cover - exercised via the workflow
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    raise SystemExit(main())
