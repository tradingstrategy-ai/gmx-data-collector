"""What the daily release is supposed to contain, checked against what it does.

The release publishes one bundle per day and reports "success" based on
whether the steps exited zero. Nothing has ever asserted that the bundle
actually carries every data type it is meant to. Two incidents in one week
came straight out of that gap:

- A module-level ``import hypersync`` made the collector unimportable on the
  runner. The job failed loudly, but only because the entry point crashed --
  had the tick phase merely produced nothing, the release would have shipped a
  volume-less bundle and said "success" (#48).
- Funding exports were retired as a variant and never replaced. The release
  carried the old files forward verbatim for 108 days while reporting nothing
  at all about funding, and the breakage surfaced as a downstream backtest
  failure (#47).

This module makes the expectation explicit: a declarative list of the data
types a bundle must carry, each with the reason it matters and how stale it is
allowed to get. :func:`assess_coverage` grades a bundle against it, so a data
type that quietly stops being produced is a reported fact rather than a
discovery made weeks later by whoever consumes it.

Scope note: this covers the **daily-stamped** types the release itself
produces -- one ``{date}.parquet`` per run. The per-symbol stores under
``futures/`` are graded by the cadence manifest and, for funding, by
``scripts/collect_daily_snapshot._funding_export_summary``, which answer a
different question (is each series continuous?) than this one (did this type
get produced at all?).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from enum import Enum
from pathlib import Path


class CoverageStatus(Enum):
    """How a data type is doing in the bundle under inspection.

    :cvar FRESH: Produced recently enough for its own tolerance.
    :cvar STALE: Present, but its newest file is older than allowed -- the
        type stopped being produced at some point and nothing noticed.
    :cvar MISSING: No directory, or a directory with no dated file in it. An
        empty directory counts as missing: a phase that created its output
        folder and then wrote nothing is precisely the silent failure this
        module exists to catch.
    """

    FRESH = "FRESH"
    STALE = "STALE"
    MISSING = "MISSING"


@dataclass(frozen=True, slots=True)
class DataTypeSpec:
    """One data type the release is responsible for producing.

    :param name: Directory name under ``user_data/data/gmx``.
    :param description: What this type is and what breaks without it. Read by
        whoever is looking at a failing gate, so it has to stand alone.
    :param max_age_days: How many days the newest file may lag "now" before
        the type is graded :attr:`CoverageStatus.STALE`.
    :param required: Whether a release must refuse to ship without this type.
        ``False`` marks a type that is *allowed* to be absent by design -- the
        trade-tick phase deliberately degrades to "no volume today" on a
        HyperSync or RPC failure, and turning that into a failed release would
        undo the fail-soft contract #48 just restored. Those types are still
        always reported; they simply do not block.
    """

    name: str
    description: str
    max_age_days: int
    required: bool = True


#: The daily-stamped types every release bundle must carry.
#:
#: ``max_age_days`` is 0 for the keyless REST phases. The collector stamps its
#: output with ``date_str = now()`` taken at start, and the cron fires at 02:00
#: UTC with a 60-minute timeout, so a scheduled run always writes *today*. A
#: one-day lag there therefore means today's write never happened -- tolerating
#: it would only catch the *second* consecutive missed day.
#:
#: The tick types keep 1: they are keyed by the UTC date each fill happened, and
#: the 02:00 window routinely straddles midnight, so a one-day lag is normal.
DAILY_STAMPED_TYPES: tuple[DataTypeSpec, ...] = (
    DataTypeSpec(
        name="snapshots",
        description=(
            "Point-in-time snapshot of every market: open interest, pool "
            "liquidity, funding and borrowing rates. The only daily record of "
            "those four -- their hourly time-series stores are refreshed by a "
            "separate pipeline that does not run in the release."
        ),
        max_age_days=0,
    ),
    DataTypeSpec(
        name="tickers",
        description="Bid/ask prices per market.",
        max_age_days=0,
    ),
    DataTypeSpec(
        name="apy",
        description="Yield data across all seven periods.",
        max_age_days=0,
    ),
    DataTypeSpec(
        name="volumes",
        description=(
            "24h traded volume per market from Subsquid. Written by a "
            "separate workflow step, so it can go missing while the rest of "
            "the snapshot succeeds."
        ),
        max_age_days=1,
        required=False,
    ),
    DataTypeSpec(
        name="ticks",
        description=(
            "Per-fill on-chain trade tape. The trade-tick phase is designed to "
            "degrade to 'no volume today' on a HyperSync or RPC failure, which "
            "means its absence never fails the run -- so it has to be checked "
            "here or a volume-less bundle ships silently."
        ),
        max_age_days=1,
        required=False,
    ),
    DataTypeSpec(
        name="tick_volume",
        description=(
            "Per-symbol traded volume and USD notional derived from the tape. "
            "This is what fills the candle 'volume' column; without it every "
            "bar reads zero."
        ),
        max_age_days=1,
        required=False,
    ),
)


@dataclass(frozen=True, slots=True)
class CoverageEntry:
    """How one data type actually fared.

    :param name: The data type's directory name.
    :param status: Its grade.
    :param latest: Newest dated file found, or ``None`` when missing.
    :param age_days: Days between ``latest`` and "now", or ``None``.
    :param description: Copied from the spec so a report needs only this.
    :param required: Whether this type blocks a release when not fresh.
    :param future_stamps: Files dated after "now", excluded from ``latest``
        and surfaced so the anomaly is visible rather than silently dropped.
    """

    name: str
    status: CoverageStatus
    latest: date | None
    age_days: int | None
    description: str
    required: bool
    future_stamps: int = 0


def _latest_stamped_date(directory: Path, today: date) -> tuple[date | None, int]:
    """Find the newest non-future ``{YYYY-MM-DD}.parquet`` date in a directory.

    Filenames that are not plain ISO dates are ignored rather than treated as
    errors -- manifests and other companions legitimately share the folder.

    Stamps *after* ``today`` are excluded from the answer and counted instead.
    A file dated in the future is not evidence that a type is current: clock
    skew on a runner or a hand-copied file would otherwise let one bogus stamp
    certify a type as fresh while months of real staleness sat behind it.

    :param directory: Directory to scan.
    :param today: The date to treat as "now".
    :returns: ``(newest non-future date or None, count of future stamps)``.
    """
    if not directory.is_dir():
        return None, 0

    newest: date | None = None
    future = 0
    for path in directory.iterdir():
        if path.name.startswith("._") or path.suffix != ".parquet":
            continue
        try:
            stamped = date.fromisoformat(path.stem)
        except ValueError:
            continue
        if stamped > today:
            future += 1
            continue
        if newest is None or stamped > newest:
            newest = stamped
    return newest, future


def assess_coverage(
    gmx_root: Path,
    now: datetime | None = None,
    specs: tuple[DataTypeSpec, ...] = DAILY_STAMPED_TYPES,
) -> list[CoverageEntry]:
    """Grade a bundle against the data types it is supposed to carry.

    :param gmx_root: The ``user_data/data/gmx`` directory to inspect.
    :param now: Clock to measure staleness against. Defaults to now, UTC.
    :param specs: Data types to require. Defaults to
        :data:`DAILY_STAMPED_TYPES`.
    :returns: One :class:`CoverageEntry` per spec, in spec order.
    """
    # A naive `now` is read as UTC: the release runs on UTC runners and every
    # stamp is a UTC date, so silently applying local time would shift the
    # boundary by a day.
    reference = now or datetime.now(UTC)
    if reference.tzinfo is None:
        reference = reference.replace(tzinfo=UTC)
    today = reference.astimezone(UTC).date()

    entries: list[CoverageEntry] = []
    for spec in specs:
        latest, future = _latest_stamped_date(gmx_root / spec.name, today)
        if latest is None:
            entries.append(
                CoverageEntry(
                    spec.name,
                    CoverageStatus.MISSING,
                    None,
                    None,
                    spec.description,
                    spec.required,
                    future,
                )
            )
            continue

        age = (today - latest).days
        status = CoverageStatus.FRESH if age <= spec.max_age_days else CoverageStatus.STALE
        entries.append(
            CoverageEntry(spec.name, status, latest, age, spec.description, spec.required, future)
        )

    return entries


def format_coverage_report(entries: list[CoverageEntry]) -> str:
    """Render coverage entries as report lines.

    Anything not ``FRESH`` carries its description, so a failing gate explains
    what broke without the reader going to look it up.

    :param entries: Output of :func:`assess_coverage`.
    :returns: Newline-joined report body.
    """
    lines: list[str] = []
    for entry in entries:
        kind = "required" if entry.required else "degradable"
        if entry.latest is None:
            lines.append(f"- {entry.name} ({kind}): {entry.status.value} — no dated file found")
        else:
            lines.append(
                f"- {entry.name} ({kind}): {entry.status.value} — newest "
                f"{entry.latest.isoformat()} ({entry.age_days}d old)"
            )
        if entry.future_stamps:
            lines.append(
                f"    NOTE: {entry.future_stamps} file(s) dated in the future were "
                "ignored when judging freshness"
            )
        if entry.status is not CoverageStatus.FRESH:
            lines.append(f"    {entry.description}")
    return "\n".join(lines)


def failing_entries(entries: list[CoverageEntry]) -> list[CoverageEntry]:
    """Return every entry that is not fresh, blocking or not.

    :param entries: Output of :func:`assess_coverage`.
    :returns: Entries graded :attr:`CoverageStatus.STALE` or
        :attr:`CoverageStatus.MISSING`.
    """
    return [e for e in entries if e.status is not CoverageStatus.FRESH]


def blocking_entries(entries: list[CoverageEntry]) -> list[CoverageEntry]:
    """Return the entries a release must refuse to ship without.

    Excludes types marked ``required=False``, which are allowed to be absent
    by design and are reported rather than enforced -- see
    :class:`DataTypeSpec`.

    :param entries: Output of :func:`assess_coverage`.
    :returns: Non-fresh entries whose absence should fail the release.
    """
    return [e for e in failing_entries(entries) if e.required]
