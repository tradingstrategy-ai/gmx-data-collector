"""Format one GitHub issue for a cadence-regression *incident*.

A fleet-wide outage (108 files, 2026-09-09) is one event. Filing one
ticket per file, each embedding the same truncated fleet list, hid the
per-file signal and spent the issue-cap on alphabetical duplicates.

The daily release workflow calls this module after ``cadence_gate check``
writes ``/tmp/gmx-cadence-regressions.json``. Detection and annotation
stay in ``cadence_gate``; this module only builds the ticket.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path

EXIT_OK = 0
EXIT_ERROR = 1

#: Default cap on ``- file: detail`` lines in the issue body. The remainder
#: is signposted as ``and N more files`` so truncation is visible.
DEFAULT_MAX_DETAIL_LINES = 50


@dataclass(frozen=True, slots=True)
class IncidentIssue:
    """One tracking ticket for every file that regressed in a release run.

    :param title: GitHub issue title, including the file count.
    :param body: Markdown body with the marker at the end.
    :param marker: Hidden HTML comment used for exact-string dedupe.
    :param file_count: Distinct files in the regressions list.
    """

    title: str
    body: str
    marker: str
    file_count: int

    def as_dict(self) -> dict[str, object]:
        """:returns: JSON-serialisable form written to ``--out``."""
        return {
            "title": self.title,
            "body": self.body,
            "marker": self.marker,
            "file_count": self.file_count,
        }


def incident_marker(date: str) -> str:
    """Build the hidden marker that identifies this release day's ticket.

    Matching on this exact string is what makes issue dedupe safe.
    GitHub's issue search tokenises, and every candle filename shares
    ``usdc``/``futures``/``feather`` and a timeframe token with every
    other one, so relevance ranking can hand back a different symbol.

    The date scopes the marker to one release run: a retry the same day
    comments on the existing ticket; the next day's incident opens a new
    one.

    :param date: UTC calendar date ``YYYY-MM-DD``.
    :returns: An HTML comment marker, embedded in the issue body.
    """
    return f"<!-- cadence-regression-incident:{date} -->"


def find_issue_number_for_marker(issues: Iterable[dict], marker: str) -> int | None:
    """Find the open issue whose body carries ``marker``.

    :param issues: Issues as returned by ``gh issue list --json number,body``.
    :param marker: Marker from :func:`incident_marker`.
    :returns: The issue number, or ``None`` when no issue carries it.
    """
    for issue in issues:
        if marker in (issue.get("body") or ""):
            return issue.get("number")
    return None


def build_incident_issue(
    regressions: Sequence[dict],
    *,
    previous_tag: str,
    run_url: str,
    date: str,
    max_detail_lines: int = DEFAULT_MAX_DETAIL_LINES,
) -> IncidentIssue:
    """Turn a regressions list into one issue for the whole incident.

    :param regressions: Items with ``file`` and ``detail`` keys, as written
        by ``cadence_gate check``.
    :param previous_tag: Previous release tag (``regressed_from`` value).
    :param run_url: Actions run URL for this release.
    :param date: UTC calendar date of the run.
    :param max_detail_lines: How many ``- detail`` lines to embed before
        signposting the remainder.
    :returns: Title, body, marker, and file count.
    """
    files = sorted({str(item.get("file") or "") for item in regressions if item.get("file")})
    file_count = len(files)
    noun = "file" if file_count == 1 else "files"
    title = f"cadence regression — {date} ({file_count} {noun})"
    marker = incident_marker(date)

    details = [str(item.get("detail") or "") for item in regressions if item.get("detail")]
    shown = details[:max_detail_lines]
    mentioned = {name for name in files if any(name in line for line in shown)}
    omitted_files = max(0, file_count - len(mentioned))

    detail_block = "\n".join(f"- {line}" for line in shown) if shown else "(no regression details)"
    if omitted_files:
        detail_block += f"\n- ... and {omitted_files} more files"

    body = (
        f"A new cadence break appeared in **{file_count} {noun}** that the "
        f"previous release (`{previous_tag}`) recorded as contiguous.\n"
        f"\n"
        f"Run: {run_url}\n"
        f"\n"
        f"{detail_block}\n"
        f"\n"
        f"The release still shipped. Each affected file's "
        f"`_cadence_manifest.json` entry is annotated with "
        f'`"regressed_from": "{previous_tag}"` so a consumer can '
        f"quarantine just those files.\n"
        f"\n"
        f"{marker}"
    )
    return IncidentIssue(title=title, body=body, marker=marker, file_count=file_count)


def _command_format(args: argparse.Namespace) -> int:
    try:
        regressions = json.loads(args.regressions.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        print(f"cannot read regressions: {exc}", file=sys.stderr)
        return EXIT_ERROR
    if not isinstance(regressions, list):
        print("regressions file must be a JSON list", file=sys.stderr)
        return EXIT_ERROR

    issue = build_incident_issue(
        regressions,
        previous_tag=args.previous_tag,
        run_url=args.run_url,
        date=args.date,
        max_detail_lines=args.max_detail_lines,
    )
    args.out.write_text(json.dumps(issue.as_dict(), indent=2), encoding="utf-8")
    print(args.out)
    return EXIT_OK


def _command_find_issue(args: argparse.Namespace) -> int:
    try:
        issues = json.loads(args.issues.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        issues = []
    if not isinstance(issues, list):
        issues = []
    number = find_issue_number_for_marker(issues, incident_marker(args.date))
    print("" if number is None else number)
    return EXIT_OK


def main(argv: Sequence[str] | None = None) -> int:
    """Entry point for ``python -m gmx_historical_data.cadence_issue``.

    :param argv: Argument vector; defaults to ``sys.argv[1:]``.
    :returns: Process exit code.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    fmt = sub.add_parser("format", help="build one incident issue from the regressions list")
    fmt.add_argument("--regressions", type=Path, required=True)
    fmt.add_argument("--previous-tag", default="unknown")
    fmt.add_argument("--run-url", required=True)
    fmt.add_argument("--date", required=True)
    fmt.add_argument("--out", type=Path, required=True)
    fmt.add_argument("--max-detail-lines", type=int, default=DEFAULT_MAX_DETAIL_LINES)
    fmt.set_defaults(func=_command_format)

    find = sub.add_parser("find-issue", help="exact-marker lookup of this day's incident issue")
    find.add_argument("--issues", type=Path, required=True)
    find.add_argument("--date", required=True)
    find.set_defaults(func=_command_find_issue)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":  # pragma: no cover - exercised via the workflow
    raise SystemExit(main())
