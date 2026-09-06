# Cadence Validation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make a missing interior candle in an exported `-futures.feather` impossible to
ship silently — detect every cadence break, publish them as a machine-readable manifest
inside `gmx-full.tar.gz`, and fail the release when a run introduces a *new* break in a
span that was previously contiguous.

**Architecture:** Add a pure detection primitive (`CadenceBreak`,
`parse_timeframe_interval`, `find_cadence_breaks`) to `ohlcv_validation.py`, then an
**opt-in, default-no-op** `expected_interval` / `cadence_policy` pair of parameters on
`validate_ohlcv` so none of its seven existing call sites change behaviour.
`FreqtradeExporter` threads `expected_interval` down through `_write` → `_write_both` /
`_prepare_export_frame` (following the route `allow_nonpositive_prices` already takes),
computes breaks on the **merged** frame that actually becomes the shipped bytes, returns
them up to `export_candles`, and merges them into
`user_data/data/gmx/futures/_cadence_manifest.json`. `release-data.yml` gains a
regression gate that diffs the new manifest against the previous release's. `export_funding`
is deliberately untouched.

**Tech Stack:** Python 3.11+, Polars, pandas + pyarrow, Typer CLI, pytest, GitHub Actions.

**Spec:** `docs/superpowers/specs/2026-09-06-cadence-validation-design.md`

## Global Constraints

- **Rebase onto `cbdb8f3` first.** The local checkout sits at `5911ff6` (#27). Every
  signature in this plan assumes the post-#28 shape: `ExportValidationError(location,
  reason, message)`, `DATA_DEFECT_ERRORS`, `is_fatal_environment_error`, `ExportFailure`,
  and **3-tuple** returns from `export_candles` / `export_funding` / `export`. Run
  `git fetch origin master && git checkout master && git merge --ff-only origin/master`
  and confirm `git rev-parse HEAD` is `cbdb8f3…` before Task 1.
- **`expected_interval=None` MUST be a complete no-op.** `validate_ohlcv` has seven call
  sites (`export_candles` ×2, `export_funding`, `_read_existing_export_frame`,
  `_merge_export_frames`, `storage.save_candles`, `scripts/validate_price_continuity.py`)
  and `tests/test_ohlcv_validation.py` has 10 existing tests. None may change behaviour.
- **Never raise on a cadence break in the default export path.** 582 of 706 shipped
  files (82.4%, 8,861 breaks) already carry one, and ~83% of those are outside GMX's
  ~5-week retention window and permanently unrecoverable. A raising default is a total
  release outage. Breaks are `WARNING`-logged and recorded in the manifest.
- **Do not add a fifth timeframe→interval table.** Four already exist
  (`daemon/gap_detector.py:90`, `daemon/gap_detector.py:235`,
  `scripts/validate_price_continuity.py:38`, `cex_gap_fill/detector.py:38`).
  `parse_timeframe_interval` must be written so it can absorb them later, but **do not
  migrate those four call sites in this change** — that is a separate mechanical PR.
- **Timeframe keys at the export call site are filename format** (`1m`, `5m`, `15m`,
  `1h`, `4h`, `1d`), because `storage.list_timeframes()` returns parquet file stems.
  `cex_gap_fill.minutes_for_timeframe` uses **pandas** keys (`1min`, `5min`, `15min`) and
  raises `ValueError` on `"5m"` — it is NOT reusable here. `parse_timeframe_interval`
  must accept both formats.
- **`export_funding` is out of scope.** Do not pass `expected_interval` from it.
  `_transform_funding_rate` ends in `drop_nulls(subset=["open"])`, so holes there are by
  design, and `_FUNDING_TIMEFRAME_PATTERN = re.compile(r"^\d+[mhd]$")` admits `8h`/`12h`.
- **`_read_existing_export_frame` must NOT receive `expected_interval`.** Validating the
  destination's pre-existing gaps would fail before `unsafe_overwrite`'s escape hatch and
  re-wedge the export.
- **Never change `_write_both`'s atomicity/rollback logic** or
  `_assert_history_preserved`. Out of scope per the design doc.
- Manifest writes **merge into** any existing manifest, never replace it wholesale — a
  partial run (`--symbol BTC`) must not erase the other 106 symbols' entries.
- Run tests with `uv run pytest`. There is no `make test` target in this repo.

---

### Task 1: Cadence detection primitive in `ohlcv_validation.py`

**Files:**
- Modify: `src/gmx_historical_data/ohlcv_validation.py`
- Test: `tests/test_ohlcv_validation.py`

**Interfaces:**
- Produces: `CadenceBreak` — `@dataclass(frozen=True, slots=True)` with fields
  `before: datetime`, `after: datetime`, `actual: timedelta`, `missing_bars: int`.
- Produces: `parse_timeframe_interval(timeframe: str) -> timedelta`. Accepts filename
  format (`"1m"`, `"15m"`, `"4h"`, `"1d"`) and pandas format (`"1min"`, `"15min"`), plus
  any `^(\d+)(min|m|h|d)$` token (so funding-style `"8h"` resolves). Raises `ValueError`
  on an unknown token.
- Produces: `find_cadence_breaks(frame: pl.DataFrame, *, timestamp_column: str,
  expected_interval: timedelta) -> list[CadenceBreak]`. Pure — never raises on a finding.
  Returns `[]` for frames with fewer than 2 rows.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_ohlcv_validation.py`. The file already imports `datetime`, `UTC`,
`pl` and `pytest`; add `timedelta` to the datetime import and extend the
`gmx_historical_data.ohlcv_validation` import block:

```python
from datetime import UTC, datetime, timedelta

from gmx_historical_data.ohlcv_validation import (
    CadenceBreak,
    count_open_outside_envelope,
    find_cadence_breaks,
    parse_timeframe_interval,
    validate_ohlcv,
)
```

```python
def _dated_frame(hours: list[int]) -> pl.DataFrame:
    """Build a frame whose `date` column holds the given hour offsets from a fixed base.

    :param hours: Hour offsets from 2025-06-02 00:00 UTC, in order.
    :returns: A minimal valid OHLCV frame at those timestamps.
    """
    base = datetime(2025, 6, 2, tzinfo=UTC)
    dates = [base + timedelta(hours=h) for h in hours]
    n = len(dates)
    return pl.DataFrame(
        {
            "date": pl.Series("date", dates, dtype=pl.Datetime("us", "UTC")),
            "open": pl.Series("open", [1.0] * n, dtype=pl.Float64),
            "high": pl.Series("high", [1.0] * n, dtype=pl.Float64),
            "low": pl.Series("low", [1.0] * n, dtype=pl.Float64),
            "close": pl.Series("close", [1.0] * n, dtype=pl.Float64),
            "volume": pl.Series("volume", [0.0] * n, dtype=pl.Float64),
        }
    )


@pytest.mark.parametrize(
    "token,expected",
    [
        ("1m", timedelta(minutes=1)),
        ("5m", timedelta(minutes=5)),
        ("15m", timedelta(minutes=15)),
        ("1h", timedelta(hours=1)),
        ("4h", timedelta(hours=4)),
        ("1d", timedelta(days=1)),
        ("1min", timedelta(minutes=1)),
        ("5min", timedelta(minutes=5)),
        ("15min", timedelta(minutes=15)),
        ("8h", timedelta(hours=8)),
        ("12h", timedelta(hours=12)),
    ],
)
def test_parse_timeframe_interval_accepts_both_key_formats(token, expected):
    assert parse_timeframe_interval(token) == expected


@pytest.mark.parametrize("token", ["", "h", "1w", "abc", "0h", "1x", "-1h"])
def test_parse_timeframe_interval_rejects_unknown_tokens(token):
    with pytest.raises(ValueError):
        parse_timeframe_interval(token)


def test_find_cadence_breaks_returns_empty_for_contiguous_series():
    frame = _dated_frame([0, 4, 8, 12])
    assert find_cadence_breaks(frame, timestamp_column="date", expected_interval=timedelta(hours=4)) == []


def test_find_cadence_breaks_detects_single_missing_bar():
    # The production BTC 4h defect: 16:00 present, 20:00 missing, 00:00 present.
    frame = _dated_frame([12, 16, 24, 28])
    breaks = find_cadence_breaks(
        frame, timestamp_column="date", expected_interval=timedelta(hours=4)
    )
    assert len(breaks) == 1
    assert breaks[0].before == datetime(2025, 6, 2, 16, tzinfo=UTC)
    assert breaks[0].after == datetime(2025, 6, 3, 0, tzinfo=UTC)
    assert breaks[0].actual == timedelta(hours=8)
    assert breaks[0].missing_bars == 1
    assert isinstance(breaks[0], CadenceBreak)


def test_find_cadence_breaks_reports_multi_bar_hole_and_multiple_breaks():
    frame = _dated_frame([0, 4, 20, 24, 40])
    breaks = find_cadence_breaks(
        frame, timestamp_column="date", expected_interval=timedelta(hours=4)
    )
    assert [b.missing_bars for b in breaks] == [3, 3]


def test_find_cadence_breaks_handles_short_frames():
    assert find_cadence_breaks(
        _dated_frame([0]), timestamp_column="date", expected_interval=timedelta(hours=4)
    ) == []


def test_find_cadence_breaks_is_order_independent():
    """The exporter always sorts before writing, but the primitive must not
    depend on the caller having done so."""
    frame = _dated_frame([24, 12, 16, 28])
    breaks = find_cadence_breaks(
        frame, timestamp_column="date", expected_interval=timedelta(hours=4)
    )
    assert len(breaks) == 1
    assert breaks[0].missing_bars == 1
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_ohlcv_validation.py -k "cadence or timeframe_interval" -v`
Expected: FAIL with `ImportError: cannot import name 'CadenceBreak' from
'gmx_historical_data.ohlcv_validation'`.

- [ ] **Step 3: Implement**

In `src/gmx_historical_data/ohlcv_validation.py`, extend the imports at the top:

```python
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta

import polars as pl
```

Add after the `OPEN_SCALE_RATIO` constant and before `OhlcvValidationResult`:

```python
#: Multiplier from a timeframe token's unit suffix to minutes.  ``m`` and
#: ``min`` are both accepted because this repo carries two key conventions:
#: parquet file stems (and therefore ``storage.list_timeframes()`` and the
#: exporter's ``tf`` variable) use ``1m``/``5m``/``15m``, while the pandas
#: resampling path uses ``1min``/``5min``/``15min``.  Parsing rather than
#: table lookup also resolves funding-style tokens such as ``8h``, which
#: ``_FUNDING_TIMEFRAME_PATTERN`` admits but no fixed six-key table contains.
_TIMEFRAME_UNIT_MINUTES: dict[str, int] = {"min": 1, "m": 1, "h": 60, "d": 1440}

_TIMEFRAME_PATTERN = re.compile(r"^(\d+)(min|m|h|d)$")


@dataclass(frozen=True, slots=True)
class CadenceBreak:
    """One interior discontinuity in an otherwise fixed-interval series.

    A break is a step between two consecutive present bars that is larger
    than the timeframe's expected interval -- i.e. one or more interior bars
    are absent.  The series is still strictly monotonic, which is why
    :func:`validate_ohlcv`'s ``non_monotonic`` guard never sees it.

    :param before: Timestamp of the last bar present before the hole.
    :param after: Timestamp of the next bar present after the hole.
    :param actual: The observed step, ``after - before``.
    :param missing_bars: Number of absent bars, ``actual // expected - 1``.
    """

    before: datetime
    after: datetime
    actual: timedelta
    missing_bars: int


def parse_timeframe_interval(timeframe: str) -> timedelta:
    """Resolve a timeframe token to its fixed bar interval.

    Accepts both key conventions used in this repo -- filename format
    (``'1m'``, ``'15m'``, ``'4h'``, ``'1d'``) and pandas format (``'1min'``,
    ``'15min'``) -- plus any other ``<count><unit>`` token, so funding-style
    ``'8h'`` resolves without a table entry.

    Note that :func:`gmx_historical_data.cex_gap_fill.detector.minutes_for_timeframe`
    accepts *only* the pandas format and raises on ``'5m'``; this function is
    the one to use anywhere the exporter's ``tf`` variable is in hand.

    :param timeframe: Timeframe token, e.g. ``'4h'``.
    :returns: The bar interval as a :class:`~datetime.timedelta`.
    :raises ValueError: If ``timeframe`` is not a recognised token or its
        count is zero.
    """
    match = _TIMEFRAME_PATTERN.match(timeframe)
    if match is None:
        raise ValueError(f"unrecognised timeframe token: {timeframe!r}")
    count = int(match.group(1))
    if count <= 0:
        raise ValueError(f"timeframe count must be positive: {timeframe!r}")
    return timedelta(minutes=count * _TIMEFRAME_UNIT_MINUTES[match.group(2)])


def find_cadence_breaks(
    frame: pl.DataFrame,
    *,
    timestamp_column: str,
    expected_interval: timedelta,
) -> list[CadenceBreak]:
    """Find every interior step that is not exactly ``expected_interval``.

    This is the check :func:`validate_ohlcv` has always lacked: its
    ``non_monotonic`` guard rejects ``diff <= 0`` but accepts any positive
    step, so a series missing an interior bar is monotonic-but-irregular and
    passes silently (issue #29).

    Pure by design -- it reports and never raises, because severity is a
    policy decision for the caller.  82% of currently-shipped files carry at
    least one break, most of them permanently unrecoverable, so the export
    path records rather than rejects.

    Only steps *larger* than expected are reported.  A smaller step means a
    duplicate or off-grid timestamp, which
    :func:`validate_ohlcv`'s ``duplicate_timestamps`` guard already covers.

    :param frame: OHLCV frame; sorted or not.
    :param timestamp_column: Name of the timestamp column.
    :param expected_interval: The timeframe's fixed bar interval, from
        :func:`parse_timeframe_interval`.
    :returns: Breaks in ascending timestamp order; empty if the series is
        contiguous or has fewer than two rows.
    """
    if frame.height < 2:
        return []

    timestamps = frame.get_column(timestamp_column).sort().to_list()
    breaks: list[CadenceBreak] = []
    for before, after in zip(timestamps, timestamps[1:], strict=False):
        actual = after - before
        if actual <= expected_interval:
            continue
        breaks.append(
            CadenceBreak(
                before=before,
                after=after,
                actual=actual,
                missing_bars=int(actual // expected_interval) - 1,
            )
        )
    return breaks
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_ohlcv_validation.py -v`
Expected: all PASS — the 6 new cadence tests plus the 10 pre-existing tests, which must
be untouched.

- [ ] **Step 5: Commit**

```bash
git add src/gmx_historical_data/ohlcv_validation.py tests/test_ohlcv_validation.py
git commit -m "feat(validation): add cadence-break detection primitive" \
  --trailer "Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>" \
  --trailer "Claude-Session: https://claude.ai/code/session_01NZV3XGPzTJvaTLN7aGyjWS"
```

---

### Task 2: Opt-in `expected_interval` / `cadence_policy` on `validate_ohlcv`

**Files:**
- Modify: `src/gmx_historical_data/ohlcv_validation.py`
- Test: `tests/test_ohlcv_validation.py`

**Interfaces:**
- Consumes: `find_cadence_breaks`, `CadenceBreak` from Task 1.
- Produces: `validate_ohlcv(frame, *, timestamp_column, location,
  allow_nonpositive_prices: bool = False, expected_interval: timedelta | None = None,
  cadence_policy: Literal["ignore", "raise"] = "ignore") -> pl.DataFrame`. Return type
  and all existing behaviour unchanged. With `expected_interval=None` the new code is
  never reached.
- Produces: when `expected_interval` is set and `cadence_policy="raise"` and breaks
  exist, raises `ExportValidationError(location, "cadence_break", …)`. Because
  `ExportValidationError` is already a member of `DATA_DEFECT_ERRORS`, the exporter's
  existing per-timeframe guard catches it with **no plumbing change**.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_ohlcv_validation.py` (reuses `_dated_frame` from Task 1):

```python
def test_validate_ohlcv_ignores_cadence_by_default():
    """The default must be a complete no-op -- seven existing call sites and
    10 existing tests depend on it."""
    frame = _dated_frame([12, 16, 24])
    assert validate_ohlcv(frame, timestamp_column="date", location="X/4h") is frame


def test_validate_ohlcv_ignores_cadence_when_policy_is_ignore():
    frame = _dated_frame([12, 16, 24])
    assert (
        validate_ohlcv(
            frame,
            timestamp_column="date",
            location="X/4h",
            expected_interval=timedelta(hours=4),
            cadence_policy="ignore",
        )
        is frame
    )


def test_validate_ohlcv_raises_cadence_break_when_policy_is_raise():
    from gmx_historical_data.ohlcv_validation import ExportValidationError

    frame = _dated_frame([12, 16, 24])
    with pytest.raises(ExportValidationError) as excinfo:
        validate_ohlcv(
            frame,
            timestamp_column="date",
            location="X/4h",
            expected_interval=timedelta(hours=4),
            cadence_policy="raise",
        )
    assert excinfo.value.reason == "cadence_break"
    assert excinfo.value.location == "X/4h"
    assert isinstance(excinfo.value, ValueError)
    assert "2025-06-02 16:00:00+00:00" in str(excinfo.value)


def test_validate_ohlcv_cadence_raise_passes_contiguous_series():
    frame = _dated_frame([0, 4, 8, 12])
    assert (
        validate_ohlcv(
            frame,
            timestamp_column="date",
            location="X/4h",
            expected_interval=timedelta(hours=4),
            cadence_policy="raise",
        )
        is frame
    )


def test_validate_ohlcv_cadence_error_is_a_data_defect_error():
    """The whole point of reusing the #28 taxonomy: the exporter's existing
    guard must already catch this without any new plumbing."""
    from gmx_historical_data.atomic_parquet import DATA_DEFECT_ERRORS
    from gmx_historical_data.ohlcv_validation import ExportValidationError

    frame = _dated_frame([12, 16, 24])
    try:
        validate_ohlcv(
            frame,
            timestamp_column="date",
            location="X/4h",
            expected_interval=timedelta(hours=4),
            cadence_policy="raise",
        )
    except DATA_DEFECT_ERRORS as exc:
        assert isinstance(exc, ExportValidationError)
    else:
        pytest.fail("cadence break was not caught by DATA_DEFECT_ERRORS")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_ohlcv_validation.py -k "cadence_policy or ignores_cadence or cadence_break or cadence_error" -v`
Expected: FAIL with `TypeError: validate_ohlcv() got an unexpected keyword argument
'expected_interval'` (the two `ignores_cadence` tests: the first PASSES already, the
second fails on the kwarg).

- [ ] **Step 3: Implement**

Add `from typing import Literal` to the imports in
`src/gmx_historical_data/ohlcv_validation.py`.

Change `validate_ohlcv`'s signature to:

```python
def validate_ohlcv(
    frame: pl.DataFrame,
    *,
    timestamp_column: str,
    location: str,
    allow_nonpositive_prices: bool = False,
    expected_interval: timedelta | None = None,
    cadence_policy: Literal["ignore", "raise"] = "ignore",
) -> pl.DataFrame:
```

Append to its docstring, after the existing `:param allow_nonpositive_prices:` block:

```
    :param expected_interval: When set, the timeframe's fixed bar interval;
        the frame is additionally scanned for interior cadence breaks (see
        :func:`find_cadence_breaks`).  ``None`` -- the default -- skips the
        scan entirely, so every pre-existing call site is unaffected.
    :param cadence_policy: What to do with breaks found under
        ``expected_interval``.  ``'ignore'`` (default) scans nothing and
        reports nothing -- callers that want the findings call
        :func:`find_cadence_breaks` directly.  ``'raise'`` raises
        :class:`ExportValidationError` with reason ``'cadence_break'``.
        The export path deliberately does **not** use ``'raise'``: 82% of
        already-published files carry an inherited break, most of them
        permanently unrecoverable past GMX's ~5-week retention window, so
        raising would be a total release outage rather than a fix (issue #29).
```

Insert the check as the **last** block before `return frame`, after the volume check —
so cadence is only evaluated on a frame that has already passed every structural guard
(non-null, unique, monotonic), which is what `find_cadence_breaks` assumes:

```python
    if expected_interval is not None and cadence_policy == "raise":
        cadence_breaks = find_cadence_breaks(
            working,
            timestamp_column=timestamp_column,
            expected_interval=expected_interval,
        )
        if cadence_breaks:
            first = cadence_breaks[0]
            missing_total = sum(b.missing_bars for b in cadence_breaks)
            raise ExportValidationError(
                location,
                "cadence_break",
                f"{location}: cadence break count={len(cadence_breaks)} "
                f"missing_bars={missing_total} expected_interval={expected_interval} "
                f"first_gap={first.actual} between {first.before} and {first.after}",
            )

    return frame
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_ohlcv_validation.py tests/test_drive_integrity_regressions.py tests/test_validate_price_continuity.py -v`
Expected: all PASS. The pre-existing tests in all three files must be unchanged — the new
parameters default to a no-op.

- [ ] **Step 5: Commit**

```bash
git add src/gmx_historical_data/ohlcv_validation.py tests/test_ohlcv_validation.py
git commit -m "feat(validation): add opt-in cadence policy to validate_ohlcv" \
  --trailer "Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>" \
  --trailer "Claude-Session: https://claude.ai/code/session_01NZV3XGPzTJvaTLN7aGyjWS"
```

---

### Task 3: Thread `expected_interval` through the exporter write path

**Files:**
- Modify: `src/gmx_historical_data/freqtrade_exporter.py`
- Test: `tests/test_freqtrade_exporter.py`

**Interfaces:**
- Consumes: `CadenceBreak`, `find_cadence_breaks`, `parse_timeframe_interval` from Task 1.
- Produces: `_write(df, path, fmt, overwrite=False, unsafe_overwrite=False,
  allow_nonpositive_prices=False, expected_interval: timedelta | None = None) ->
  list[CadenceBreak]` (was `-> None`; no existing caller uses the return value, so this is
  additive). Returns `[]` when `expected_interval is None`.
- Produces: `_write_both(df, feather_path, parquet_path, unsafe_overwrite,
  allow_nonpositive_prices, expected_interval: timedelta | None = None) ->
  list[CadenceBreak]` (was `-> None`).
- Breaks are computed on the **post-merge** frame — the bytes actually written — not on
  the incoming frame.

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_freqtrade_exporter.py`. The file already imports `pandas as pd`,
`polars as pl`, `pytest`, `Path`, `datetime`, `UTC`, `FreqtradeExporter` and
`ParquetStorage`; add `timedelta` to the datetime import and add:

```python
from gmx_historical_data.ohlcv_validation import CadenceBreak
```

```python
def _gapped_candles(symbol: str, hour_offsets: list[int]) -> pd.DataFrame:
    """Build a candle frame at explicit hour offsets so a hole can be seeded.

    :param symbol: Token symbol.
    :param hour_offsets: Hour offsets from 2024-01-01 00:00 UTC.
    :returns: pandas DataFrame with the columns ``save_candles`` requires.
    """
    base = pd.Timestamp("2024-01-01", tz="UTC")
    timestamps = [base + pd.Timedelta(hours=h) for h in hour_offsets]
    n = len(timestamps)
    return pd.DataFrame(
        {
            "timestamp": timestamps,
            "open": [1.0] * n,
            "high": [1.0] * n,
            "low": [1.0] * n,
            "close": [1.0] * n,
            "symbol": [symbol] * n,
        }
    )


def test_write_returns_no_breaks_without_expected_interval(tmp_path: Path):
    """Default stays a no-op -- every pre-existing _write caller is unaffected."""
    exporter = FreqtradeExporter(tmp_path / "data", tmp_path / "out")
    frame = _freqtrade_frame()
    path = tmp_path / "out" / "X-1h-futures.feather"
    path.parent.mkdir(parents=True, exist_ok=True)
    assert exporter._write(frame, path, "feather") == []


def test_write_reports_cadence_breaks_on_written_frame(tmp_path: Path):
    data_dir = tmp_path / "data"
    storage = ParquetStorage(data_dir)
    # 00:00, 01:00, 03:00 -- the 02:00 bar is absent.
    storage.save_candles(_gapped_candles("BBB", [0, 1, 3]), "1h", "BBB")

    exporter = FreqtradeExporter(data_dir, tmp_path / "out")
    df = pl.from_pandas(storage.read_candles("1h", "BBB"))
    path = tmp_path / "out" / "BBB_USDC_USDC-1h-futures.feather"
    path.parent.mkdir(parents=True, exist_ok=True)

    breaks = exporter._write(
        exporter._transform_dataframe(df),
        path,
        "feather",
        expected_interval=timedelta(hours=1),
    )

    assert len(breaks) == 1
    assert isinstance(breaks[0], CadenceBreak)
    assert breaks[0].missing_bars == 1
    assert path.exists()  # the file is still written -- record, never reject


def test_write_reports_break_created_at_the_merge_seam(tmp_path: Path):
    """The check must run on the merged frame: neither the existing file nor
    the incoming slice has a hole on its own, but the join between them does."""
    exporter = FreqtradeExporter(tmp_path / "data", tmp_path / "out")
    path = tmp_path / "out" / "SEAM_USDC_USDC-1h-futures.feather"
    path.parent.mkdir(parents=True, exist_ok=True)

    base = datetime(2024, 1, 1, tzinfo=UTC)

    def _frame(hours: list[int]) -> pl.DataFrame:
        dates = [base + timedelta(hours=h) for h in hours]
        n = len(dates)
        return pl.DataFrame(
            {
                "date": pl.Series("date", dates, dtype=pl.Datetime("ns", "UTC")),
                "open": pl.Series("open", [1.0] * n, dtype=pl.Float64),
                "high": pl.Series("high", [1.0] * n, dtype=pl.Float64),
                "low": pl.Series("low", [1.0] * n, dtype=pl.Float64),
                "close": pl.Series("close", [1.0] * n, dtype=pl.Float64),
                "volume": pl.Series("volume", [0.0] * n, dtype=pl.Float64),
            }
        )

    assert exporter._write(_frame([0, 1, 2]), path, "feather") == []
    breaks = exporter._write(
        _frame([5, 6, 7]), path, "feather", expected_interval=timedelta(hours=1)
    )

    assert len(breaks) == 1
    assert breaks[0].missing_bars == 2  # 03:00 and 04:00 absent


def test_write_both_reports_cadence_breaks(tmp_path: Path):
    exporter = FreqtradeExporter(tmp_path / "data", tmp_path / "out")
    path = tmp_path / "out" / "BOTH_USDC_USDC-1h-futures.feather"
    path.parent.mkdir(parents=True, exist_ok=True)

    base = datetime(2024, 1, 1, tzinfo=UTC)
    dates = [base + timedelta(hours=h) for h in (0, 1, 3)]
    frame = pl.DataFrame(
        {
            "date": pl.Series("date", dates, dtype=pl.Datetime("ns", "UTC")),
            "open": pl.Series("open", [1.0] * 3, dtype=pl.Float64),
            "high": pl.Series("high", [1.0] * 3, dtype=pl.Float64),
            "low": pl.Series("low", [1.0] * 3, dtype=pl.Float64),
            "close": pl.Series("close", [1.0] * 3, dtype=pl.Float64),
            "volume": pl.Series("volume", [0.0] * 3, dtype=pl.Float64),
        }
    )

    breaks = exporter._write(frame, path, "both", expected_interval=timedelta(hours=1))

    assert len(breaks) == 1
    assert breaks[0].missing_bars == 1
    assert path.exists()
    assert path.with_suffix(".parquet").exists()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_freqtrade_exporter.py -k "cadence or seam or write_returns_no_breaks" -v`
Expected: FAIL — `test_write_returns_no_breaks_without_expected_interval` fails on
`assert None == []`, the rest fail with `TypeError: _write() got an unexpected keyword
argument 'expected_interval'`.

- [ ] **Step 3: Implement**

In `src/gmx_historical_data/freqtrade_exporter.py`, extend the imports:

```python
from datetime import timedelta

from gmx_historical_data.ohlcv_validation import (
    CadenceBreak,
    ExportValidationError,
    assert_export_parity,
    find_cadence_breaks,
    parse_timeframe_interval,
    validate_ohlcv,
)
```

(`parse_timeframe_interval` is unused until Task 4 — add it there instead if your linter
rejects the unused import at this step.)

Change `_write`'s signature and return type:

```python
    def _write(
        self,
        df: pl.DataFrame,
        path: Path,
        fmt: str,
        overwrite: bool = False,
        unsafe_overwrite: bool = False,
        allow_nonpositive_prices: bool = False,
        expected_interval: timedelta | None = None,
    ) -> list[CadenceBreak]:
```

Add to its docstring, after `:param unsafe_overwrite:`:

```
    :param expected_interval: When set, the merged frame -- the bytes this
        call actually publishes -- is scanned for interior cadence breaks.
        Findings are returned and logged, never raised: see the module's
        ``export_candles`` docstring and issue #29.  ``None`` skips the scan.
    :returns: Cadence breaks found in the published frame; empty list when
        ``expected_interval`` is ``None`` or the series is contiguous.
```

Replace `_write`'s body from the `fmt == "both"` branch onwards:

```python
        if fmt == "both":
            feather_path = path if path.suffix == ".feather" else path.with_suffix(".feather")
            parquet_path = feather_path.with_suffix(".parquet")
            return self._write_both(
                df,
                feather_path,
                parquet_path,
                unsafe_overwrite,
                allow_nonpositive_prices,
                expected_interval,
            )

        if fmt not in {"feather", "parquet"}:
            raise ValueError(f"Unsupported export format: {fmt}")

        merged = self._prepare_export_frame(
            df,
            path,
            fmt=fmt,
            unsafe_overwrite=unsafe_overwrite,
            allow_nonpositive_prices=allow_nonpositive_prices,
        )
        breaks = self._scan_cadence(merged, path, expected_interval)
        self._write_single_frame(merged, path, fmt)
        return breaks
```

Change `_write_both`'s signature and return type:

```python
    def _write_both(
        self,
        df: pl.DataFrame,
        feather_path: Path,
        parquet_path: Path,
        unsafe_overwrite: bool,
        allow_nonpositive_prices: bool,
        expected_interval: timedelta | None = None,
    ) -> list[CadenceBreak]:
```

In `_write_both`, insert the scan immediately after the merge block closes and **before**
the `feather_tmp = ...` line — `df` at that point is the final post-merge frame. Do not
touch anything below it:

```python
        breaks = self._scan_cadence(df, feather_path, expected_interval)

        feather_tmp = feather_path.with_name(f".{feather_path.name}.{uuid4().hex}.tmp")
```

and change `_write_both`'s `finally` block to be followed by `return breaks` at the end
of the method:

```python
        finally:
            for backup_path in (feather_backup, parquet_backup):
                if backup_path.exists():
                    backup_path.unlink()

        return breaks
```

Add the shared helper as a new method, placed just above `_write_single_frame`:

```python
    def _scan_cadence(
        self,
        frame: pl.DataFrame,
        path: Path,
        expected_interval: timedelta | None,
    ) -> list[CadenceBreak]:
        """Scan a frame about to be published for interior cadence breaks.

        Records and logs; never raises.  A break means one or more interior
        bars are absent -- a real upstream oracle or collector outage, not a
        transform bug -- and 82% of already-published files carry at least
        one, most permanently unrecoverable past GMX's ~5-week retention
        window.  Rejecting them would wedge the release rather than fix
        anything, so the export path records them into the cadence manifest
        instead (issue #29).

        :param frame: The post-merge frame that is about to be written.
        :param path: Destination path, used for the log line only.
        :param expected_interval: The timeframe's bar interval, or ``None``
            to skip the scan entirely.
        :returns: Breaks found, in ascending timestamp order.
        """
        if expected_interval is None:
            return []
        breaks = find_cadence_breaks(
            frame, timestamp_column="date", expected_interval=expected_interval
        )
        if breaks:
            logger.warning(
                "%s: %d cadence break(s), %d missing bar(s); first gap %s between %s and %s",
                path.name,
                len(breaks),
                sum(b.missing_bars for b in breaks),
                breaks[0].actual,
                breaks[0].before,
                breaks[0].after,
            )
        return breaks
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_freqtrade_exporter.py -v`
Expected: all PASS, including every pre-existing test in the file (notably
`test_export_candles_both_rolls_back_when_second_publish_fails` and
`test_export_candles_both_leaves_existing_files_intact_on_second_write_failure`, which
prove the rollback path in `_write_both` is untouched).

Run: `uv run pytest tests/ -k "export" -v`
Expected: all PASS — no regression in the #27/#28 guard tests.

- [ ] **Step 5: Commit**

```bash
git add src/gmx_historical_data/freqtrade_exporter.py tests/test_freqtrade_exporter.py
git commit -m "feat(export): scan published candle frames for cadence breaks" \
  --trailer "Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>" \
  --trailer "Claude-Session: https://claude.ai/code/session_01NZV3XGPzTJvaTLN7aGyjWS"
```

---

### Task 4: Cadence manifest emitted by `export_candles`

**Files:**
- Modify: `src/gmx_historical_data/freqtrade_exporter.py`
- Test: `tests/test_cadence_manifest.py` (create)

**Interfaces:**
- Consumes: `_write`'s `list[CadenceBreak]` return (Task 3), `parse_timeframe_interval`
  (Task 1).
- Produces: module constants `CADENCE_MANIFEST_NAME = "_cadence_manifest.json"` and
  `MAX_BREAKS_PER_FILE = 200`.
- Produces: `FreqtradeExporter.write_cadence_manifest(gmx_dir: Path, entries: dict[str, dict]) -> Path`
  — merges `entries` into any existing manifest at `gmx_dir / CADENCE_MANIFEST_NAME` and
  writes it atomically (tmp + `os.replace`). Public because the release gate reads and
  the CLI may want to regenerate it.
- Produces: `export_candles` writes the manifest before returning. Its return type is
  **unchanged** (`tuple[dict, list[str], list[ExportFailure]]`) — no CLI arity change.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_cadence_manifest.py`:

```python
"""Tests for the cadence manifest shipped alongside exported candle feathers."""

import json
from pathlib import Path

import pandas as pd

from gmx_historical_data.freqtrade_exporter import (
    CADENCE_MANIFEST_NAME,
    FreqtradeExporter,
)
from gmx_historical_data.storage import ParquetStorage


def _candles(symbol: str, hour_offsets: list[int]) -> pd.DataFrame:
    """Build a candle frame at explicit hour offsets so a hole can be seeded.

    :param symbol: Token symbol.
    :param hour_offsets: Hour offsets from 2024-01-01 00:00 UTC.
    :returns: pandas DataFrame with the columns ``save_candles`` requires.
    """
    base = pd.Timestamp("2024-01-01", tz="UTC")
    timestamps = [base + pd.Timedelta(hours=h) for h in hour_offsets]
    n = len(timestamps)
    return pd.DataFrame(
        {
            "timestamp": timestamps,
            "open": [1.0] * n,
            "high": [1.0] * n,
            "low": [1.0] * n,
            "close": [1.0] * n,
            "symbol": [symbol] * n,
        }
    )


def _manifest(output_dir: Path) -> dict:
    path = output_dir / "gmx" / "futures" / CADENCE_MANIFEST_NAME
    return json.loads(path.read_text(encoding="utf-8"))


def test_manifest_records_a_gap_and_marks_clean_files(tmp_path: Path):
    data_dir = tmp_path / "data"
    storage = ParquetStorage(data_dir)
    storage.save_candles(_candles("AAA", [0, 1, 2, 3]), "1h", "AAA")   # contiguous
    storage.save_candles(_candles("BBB", [0, 1, 3]), "1h", "BBB")      # 02:00 missing

    output_dir = tmp_path / "output"
    FreqtradeExporter(data_dir, output_dir).export_candles(
        symbols=["AAA", "BBB"], timeframes=["1h"]
    )

    manifest = _manifest(output_dir)
    assert "generated_at" in manifest

    clean = manifest["files"]["AAA_USDC_USDC-1h-futures.feather"]
    assert clean["breaks_total"] == 0
    assert clean["missing_bars_total"] == 0
    assert clean["breaks"] == []
    assert clean["timeframe"] == "1h"
    assert clean["expected_interval_seconds"] == 3600
    assert clean["rows"] == 4

    gapped = manifest["files"]["BBB_USDC_USDC-1h-futures.feather"]
    assert gapped["breaks_total"] == 1
    assert gapped["missing_bars_total"] == 1
    assert gapped["breaks"][0]["missing_bars"] == 1
    assert gapped["breaks"][0]["before"] == "2024-01-01T01:00:00+00:00"
    assert gapped["breaks"][0]["after"] == "2024-01-01T03:00:00+00:00"


def test_manifest_records_every_clean_file_so_absence_is_meaningful(tmp_path: Path):
    """A consumer must be able to tell 'checked, contiguous' from 'not checked'.
    Recording only gapped files would make those two indistinguishable."""
    data_dir = tmp_path / "data"
    ParquetStorage(data_dir).save_candles(_candles("AAA", [0, 1, 2]), "1h", "AAA")

    output_dir = tmp_path / "output"
    FreqtradeExporter(data_dir, output_dir).export_candles(symbols=["AAA"], timeframes=["1h"])

    assert "AAA_USDC_USDC-1h-futures.feather" in _manifest(output_dir)["files"]


def test_partial_export_merges_into_existing_manifest(tmp_path: Path):
    """A `--symbol BBB` run must not erase AAA's entry."""
    data_dir = tmp_path / "data"
    storage = ParquetStorage(data_dir)
    storage.save_candles(_candles("AAA", [0, 1, 2]), "1h", "AAA")
    storage.save_candles(_candles("BBB", [0, 1, 3]), "1h", "BBB")

    output_dir = tmp_path / "output"
    exporter = FreqtradeExporter(data_dir, output_dir)
    exporter.export_candles(symbols=["AAA", "BBB"], timeframes=["1h"])
    exporter.export_candles(symbols=["BBB"], timeframes=["1h"])

    files = _manifest(output_dir)["files"]
    assert "AAA_USDC_USDC-1h-futures.feather" in files
    assert "BBB_USDC_USDC-1h-futures.feather" in files


def test_manifest_excludes_funding_and_mark_files(tmp_path: Path):
    """Funding frames are deliberately not cadence-checked (drop_nulls by
    design, free-form timeframes); mark/index files duplicate the candle
    series and would double every entry."""
    data_dir = tmp_path / "data"
    ParquetStorage(data_dir).save_candles(_candles("AAA", [0, 1, 3]), "1h", "AAA")

    output_dir = tmp_path / "output"
    FreqtradeExporter(data_dir, output_dir).export_candles(symbols=["AAA"], timeframes=["1h"])

    names = set(_manifest(output_dir)["files"])
    assert names == {"AAA_USDC_USDC-1h-futures.feather"}


def test_manifest_truncates_pathological_break_lists(tmp_path: Path):
    """1m files carry thousands of breaks; the manifest must stay small."""
    from gmx_historical_data.freqtrade_exporter import MAX_BREAKS_PER_FILE

    # Every other bar present -> one break per present-pair.
    offsets = list(range(0, (MAX_BREAKS_PER_FILE + 20) * 2, 2))
    data_dir = tmp_path / "data"
    ParquetStorage(data_dir).save_candles(_candles("AAA", offsets), "1h", "AAA")

    output_dir = tmp_path / "output"
    FreqtradeExporter(data_dir, output_dir).export_candles(symbols=["AAA"], timeframes=["1h"])

    entry = _manifest(output_dir)["files"]["AAA_USDC_USDC-1h-futures.feather"]
    assert entry["truncated"] is True
    assert len(entry["breaks"]) == MAX_BREAKS_PER_FILE
    assert entry["breaks_total"] > MAX_BREAKS_PER_FILE


def test_manifest_is_not_mistaken_for_a_data_file(tmp_path: Path):
    """The manifest lives in the futures dir; a second export must not try to
    read it as a feather."""
    data_dir = tmp_path / "data"
    ParquetStorage(data_dir).save_candles(_candles("AAA", [0, 1, 2]), "1h", "AAA")

    output_dir = tmp_path / "output"
    exporter = FreqtradeExporter(data_dir, output_dir)
    exporter.export_candles(symbols=["AAA"], timeframes=["1h"])
    results, failed_symbols, failures = exporter.export_candles(
        symbols=["AAA"], timeframes=["1h"]
    )

    assert failed_symbols == []
    assert failures == []
    assert "AAA" in results
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_cadence_manifest.py -v`
Expected: FAIL with `ImportError: cannot import name 'CADENCE_MANIFEST_NAME' from
'gmx_historical_data.freqtrade_exporter'`.

- [ ] **Step 3: Implement**

In `src/gmx_historical_data/freqtrade_exporter.py`, add `import json`, `import os` and
`from datetime import UTC, datetime` to the imports, and add these constants next to
`_FUNDING_TIMEFRAME_PATTERN`:

```python
#: Filename of the cadence manifest published beside the exported feathers.
#: It lands inside ``gmx-full.tar.gz`` because the release workflow tars
#: ``user_data/data/gmx/`` wholesale, so downstream consumers can tell "this
#: file is complete" from "this file has a known hole" by reading one JSON
#: file instead of re-deriving the check per pair -- the duplication that
#: caused the #657 incident.  The leading underscore and ``.json`` suffix
#: keep it clear of every ``*.feather`` / ``*.parquet`` glob in this repo.
CADENCE_MANIFEST_NAME = "_cadence_manifest.json"

#: Cap on ``breaks`` entries recorded per file.  ``breaks_total`` and
#: ``missing_bars_total`` are always exact; the list is truncated and
#: ``truncated`` set to ``True`` beyond this many.  Without it the 1m
#: feathers (8,157 breaks across 111 files) would dominate the manifest.
MAX_BREAKS_PER_FILE = 200
```

Add these two methods to `FreqtradeExporter`, above `_make_gmx_dir`:

```python
    @staticmethod
    def _cadence_manifest_entry(
        timeframe: str,
        expected_interval: timedelta,
        frame_rows: int,
        first: datetime | None,
        last: datetime | None,
        breaks: list[CadenceBreak],
    ) -> dict:
        """Build one manifest entry for a published candle file.

        :param timeframe: Timeframe token as exported, e.g. ``'4h'``.
        :param expected_interval: That timeframe's fixed bar interval.
        :param frame_rows: Row count of the published frame.
        :param first: Earliest timestamp in the published frame.
        :param last: Latest timestamp in the published frame.
        :param breaks: Cadence breaks found in the published frame.
        :returns: JSON-serialisable manifest entry.
        """
        recorded = breaks[:MAX_BREAKS_PER_FILE]
        return {
            "timeframe": timeframe,
            "expected_interval_seconds": int(expected_interval.total_seconds()),
            "rows": frame_rows,
            "first": first.isoformat() if first is not None else None,
            "last": last.isoformat() if last is not None else None,
            "breaks_total": len(breaks),
            "missing_bars_total": sum(b.missing_bars for b in breaks),
            "truncated": len(breaks) > MAX_BREAKS_PER_FILE,
            "breaks": [
                {
                    "before": b.before.isoformat(),
                    "after": b.after.isoformat(),
                    "missing_bars": b.missing_bars,
                }
                for b in recorded
            ],
        }

    def write_cadence_manifest(self, gmx_dir: Path, entries: dict[str, dict]) -> Path:
        """Merge ``entries`` into the cadence manifest and publish it atomically.

        Merging rather than replacing is required: a partial run such as
        ``export-freqtrade --symbol BTC`` touches one file, and must not
        erase the other 106 symbols' recorded state.

        :param gmx_dir: Directory the feathers were written to.
        :param entries: Manifest entries keyed by feather filename.
        :returns: Path to the published manifest.
        """
        path = gmx_dir / CADENCE_MANIFEST_NAME
        files: dict[str, dict] = {}
        if path.exists():
            try:
                files = json.loads(path.read_text(encoding="utf-8")).get("files", {})
            except (json.JSONDecodeError, OSError) as exc:
                logger.warning(
                    "%s: unreadable cadence manifest, rebuilding from this run only: %s",
                    path.name,
                    exc,
                )
        files.update(entries)

        payload = {
            "generated_at": datetime.now(UTC).isoformat(),
            "files": dict(sorted(files.items())),
        }
        tmp = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
        tmp.write_text(json.dumps(payload, indent=2, sort_keys=False), encoding="utf-8")
        os.replace(tmp, path)
        return path
```

In `export_candles`, add a collector before the symbol loop, capture `_write`'s return
for the `-futures` write only, and publish at the end.

Immediately after `failures: list[ExportFailure] = []`:

```python
        cadence_entries: dict[str, dict] = {}
```

Replace the `-futures` `self._write(...)` call — the **first** of the three `_write`
calls in the loop, the one without a `candle_type` argument — with a version that
captures the filename and the returned breaks. Leave the `mark` and `index` writes
exactly as they are; they duplicate the candle series and would double every entry:

```python
                    futures_name = self._get_freqtrade_filename(
                        symbol,
                        tf,
                        "feather" if output_format == "both" else output_format,
                        trading_mode,
                        quote_currency,
                    )
                    expected_interval = parse_timeframe_interval(tf)
                    breaks = self._write(
                        ft_df,
                        gmx_dir / futures_name,
                        output_format,
                        overwrite,
                        unsafe_overwrite,
                        expected_interval=expected_interval,
                    )
                    manifest_key = Path(futures_name).with_suffix(".feather").name
                    cadence_entries[manifest_key] = self._cadence_manifest_entry(
                        tf,
                        expected_interval,
                        ft_df.height,
                        ft_df.get_column("date").min(),
                        ft_df.get_column("date").max(),
                        breaks,
                    )
                    ohlcv_files += 2 if output_format == "both" else 1
                    total_candles += len(ft_df)
```

Note `ft_df`'s min/max are used for `first`/`last` rather than the merged frame's, and
`breaks` come from the merged frame. If a `--symbol`-scoped run must report merged-frame
extents, that is a follow-up; the breaks — the thing the gate keys on — are already
merged-frame accurate.

Finally, immediately before `return results, failed_symbols, failures`:

```python
        if cadence_entries:
            self.write_cadence_manifest(gmx_dir, cadence_entries)
```

Add to `export_candles`'s docstring, after the existing guard paragraph:

```
        Every exported ``-futures`` file's published frame is scanned for
        interior cadence breaks and recorded in ``_cadence_manifest.json``
        beside the feathers (see :data:`CADENCE_MANIFEST_NAME`).  A break is
        logged and recorded, never raised: 82% of already-published files
        carry an inherited one and most are permanently unrecoverable past
        GMX's ~5-week retention window, so rejecting them would wedge the
        release instead of fixing anything.  Preventing a *new* silent gap is
        the release workflow's regression gate, which diffs this manifest
        against the previous release's (issue #29).
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_cadence_manifest.py -v`
Expected: all PASS.

Run: `uv run pytest tests/ -v`
Expected: all PASS — record the exact pass count for the PR body.

- [ ] **Step 5: Commit**

```bash
git add src/gmx_historical_data/freqtrade_exporter.py tests/test_cadence_manifest.py
git commit -m "feat(export): publish a cadence manifest beside exported feathers" \
  --trailer "Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>" \
  --trailer "Claude-Session: https://claude.ai/code/session_01NZV3XGPzTJvaTLN7aGyjWS"
```

---

### Task 5: Release-workflow regression gate

**Files:**
- Modify: `.github/workflows/release-data.yml`
- Test: `tests/test_release_workflow.py`

**Interfaces:**
- Consumes: `_cadence_manifest.json` published by Task 4, and the previous release's
  copy of the same file (already on disk after the workflow's existing "restore previous
  release" step untars `gmx-full.tar.gz` into the working tree).
- Produces: a new `Snapshot restored cadence manifest` step that copies the restored
  manifest to `/tmp/gmx-cadence-baseline.json` **before** collection, and a
  `Validate candle cadence` step **after** export that fails the release on a new break
  in a previously-contiguous span.

> **Decision required before implementing this task** — spec open question 3. This task
> implements plain "fail the release". If the user chooses warn-only, change the final
> `sys.exit(1)` to a `::warning::` annotation and update the test accordingly.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_release_workflow.py`, matching the file's existing
read-the-YAML-and-assert-substrings style:

```python
def test_release_workflow_snapshots_and_validates_cadence() -> None:
    text = WORKFLOW.read_text()

    assert "Snapshot restored cadence manifest" in text
    assert "/tmp/gmx-cadence-baseline.json" in text
    assert "Validate candle cadence" in text
    assert "_cadence_manifest.json" in text
    assert "new cadence break" in text


def test_cadence_gate_runs_after_export() -> None:
    """The gate must read the manifest the export step just wrote, so it has
    to sit after it in the job."""
    text = WORKFLOW.read_text()

    assert text.index("Snapshot restored cadence manifest") < text.index("Validate candle cadence")
    assert text.index("export-freqtrade") < text.index("Validate candle cadence")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_release_workflow.py -v`
Expected: FAIL on `assert "Snapshot restored cadence manifest" in text`.

- [ ] **Step 3: Implement**

In `.github/workflows/release-data.yml`, add this step immediately **after** the existing
`Snapshot restored candle history` step (the one writing
`/tmp/gmx-restore-manifest.json`, around line 181):

```yaml
      - name: Snapshot restored cadence manifest
        run: |
          BASELINE=./user_data/data/gmx/futures/_cadence_manifest.json
          if [ -f "$BASELINE" ]; then
            cp "$BASELINE" /tmp/gmx-cadence-baseline.json
            echo "Captured cadence baseline from the previous release."
          else
            echo '{"files": {}}' > /tmp/gmx-cadence-baseline.json
            echo "No cadence manifest in the previous release; first run after rollout."
          fi
```

Add this step immediately **after** the existing `Validate candle history integrity`
step:

```yaml
      - name: Validate candle cadence
        run: |
          PYTHONPATH=src python - <<'PY'
          import json
          import sys
          from pathlib import Path

          baseline = json.loads(Path("/tmp/gmx-cadence-baseline.json").read_text(encoding="utf-8"))
          current_path = Path("./user_data/data/gmx/futures/_cadence_manifest.json")
          if not current_path.exists():
              print("ERROR: export produced no cadence manifest.", file=sys.stderr)
              sys.exit(1)

          current = json.loads(current_path.read_text(encoding="utf-8"))
          base_files = baseline.get("files", {})
          regressions = []

          for name, entry in sorted(current.get("files", {}).items()):
              before = base_files.get(name)
              if before is None:
                  # New file, or first run after rollout: nothing to compare.
                  continue
              if before.get("truncated") or entry.get("truncated"):
                  # Break lists are incomplete on either side; the totals are
                  # still exact, so fall back to comparing those.
                  if entry["breaks_total"] > before["breaks_total"]:
                      regressions.append(
                          f"{name}: new cadence break (total {before['breaks_total']} -> "
                          f"{entry['breaks_total']}, truncated lists)"
                      )
                  continue

              known = {(b["before"], b["after"]) for b in before["breaks"]}
              baseline_last = before.get("last")
              for gap in entry["breaks"]:
                  if (gap["before"], gap["after"]) in known:
                      continue
                  if baseline_last is not None and gap["before"] >= baseline_last:
                      # The hole is in territory the previous release did not
                      # cover, so it cannot be a regression against it.
                      continue
                  regressions.append(
                      f"{name}: new cadence break — {gap['missing_bars']} bar(s) missing "
                      f"between {gap['before']} and {gap['after']}"
                  )

          if regressions:
              print("ERROR: cadence regressions detected in this release:", file=sys.stderr)
              for line in regressions[:20]:
                  print(f"  - {line}", file=sys.stderr)
              if len(regressions) > 20:
                  print(f"  ... and {len(regressions) - 20} more", file=sys.stderr)
              sys.exit(1)

          total_breaks = sum(e["breaks_total"] for e in current.get("files", {}).values())
          print(
              f"Cadence OK: {len(current.get('files', {}))} files checked, "
              f"{total_breaks} inherited break(s), 0 new."
          )
          PY
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_release_workflow.py -v`
Expected: all PASS.

Validate the YAML parses:

Run: `uv run python -c "import yaml,pathlib; yaml.safe_load(pathlib.Path('.github/workflows/release-data.yml').read_text()); print('yaml ok')"`
Expected: `yaml ok`.

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/release-data.yml tests/test_release_workflow.py
git commit -m "feat(release): fail the release on a new candle cadence break" \
  --trailer "Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>" \
  --trailer "Claude-Session: https://claude.ai/code/session_01NZV3XGPzTJvaTLN7aGyjWS"
```

---

### Task 6: Document the manifest contract for downstream consumers

**Files:**
- Modify: `README.md`
- Test: none (documentation only — verified by review)

**Interfaces:**
- Consumes: `CADENCE_MANIFEST_NAME` and the entry shape from Task 4.

This task exists because the manifest is a **published interface**: the orchestrator repo
is expected to read it instead of re-deriving the cadence check, and an undocumented JSON
file in a tarball will be re-derived instead.

- [ ] **Step 1: Document the manifest in the release-assets section**

`README.md:49` describes the release assets in a table row. Add a subsection after the
`export-freqtrade` usage block around line 342:

```markdown
### Cadence manifest

Every `export-freqtrade` run publishes `_cadence_manifest.json` beside the exported
feathers, at `user_data/data/gmx/futures/_cadence_manifest.json`. It ships inside
`gmx-full.tar.gz`.

GMX's oracle and this collector both have real outage windows, so a `-futures.feather`
can be missing an interior bar. The series is still strictly monotonic, so a consumer
checking only row counts or ordering cannot see the hole. The manifest records every one
of them, per file:

```json
{
  "generated_at": "2026-09-06T12:00:00+00:00",
  "files": {
    "BTC_USDC_USDC-4h-futures.feather": {
      "timeframe": "4h",
      "expected_interval_seconds": 14400,
      "rows": 6862,
      "first": "2023-07-20T08:00:00+00:00",
      "last": "2026-09-06T00:00:00+00:00",
      "breaks_total": 1,
      "missing_bars_total": 1,
      "truncated": false,
      "breaks": [
        {
          "before": "2025-06-02T16:00:00+00:00",
          "after": "2025-06-03T00:00:00+00:00",
          "missing_bars": 1
        }
      ]
    }
  }
}
```

Contract notes:

- **Every exported `-futures` file gets an entry**, including contiguous ones
  (`breaks_total: 0`). Absence from the manifest means "not checked", not "clean".
- `breaks_total` and `missing_bars_total` are always exact. When `truncated` is `true`
  the `breaks` list is capped at 200 entries (1m series can carry thousands).
- Only candle (`-futures`) files are covered. `-funding_rate` files are deliberately not
  cadence-checked: the funding transform drops null rates by design, so holes there are
  expected.
- Timestamps are ISO-8601 UTC. `before` is the last bar present before the hole, `after`
  the next one present.
- A gap listed here is a known upstream outage, not corruption. GMX retains raw data for
  only ~5 weeks, so most listed gaps cannot be backfilled from source.

The release workflow fails if a run introduces a break in a span the previous release
recorded as contiguous, so this file only ever grows by way of a reviewed change.
```

- [ ] **Step 2: Verify the README renders and links resolve**

Run: `uv run python -c "import pathlib; t=pathlib.Path('README.md').read_text(); assert '_cadence_manifest.json' in t and 'Cadence manifest' in t; print('readme ok')"`
Expected: `readme ok`.

- [ ] **Step 3: Run the full suite one final time**

Run: `uv run pytest tests/ -v`
Expected: all PASS. Record the exact count for the PR body.

- [ ] **Step 4: Commit**

```bash
git add README.md
git commit -m "docs: document the cadence manifest contract for downstream consumers" \
  --trailer "Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>" \
  --trailer "Claude-Session: https://claude.ai/code/session_01NZV3XGPzTJvaTLN7aGyjWS"
```

---

## Verification against the production artifact

After Task 4, before opening the PR, confirm the manifest reproduces the known defect on
real data rather than only on fixtures:

```bash
uv run python - <<'PY'
from datetime import timedelta
import polars as pl
from gmx_historical_data.ohlcv_validation import find_cadence_breaks

frame = pl.read_ipc(
    "user_data/data/gmx/futures/BTC_USDC_USDC-4h-futures.feather", memory_map=False
)
for b in find_cadence_breaks(
    frame, timestamp_column="date", expected_interval=timedelta(hours=4)
):
    print(b)
PY
```

Expected output — the exact defect from issue #29:

```
CadenceBreak(before=datetime.datetime(2025, 6, 2, 16, 0, tzinfo=...),
             after=datetime.datetime(2025, 6, 3, 0, 0, tzinfo=...),
             actual=datetime.timedelta(seconds=28800), missing_bars=1)
```

Paste this into the PR body as empirical validation.
