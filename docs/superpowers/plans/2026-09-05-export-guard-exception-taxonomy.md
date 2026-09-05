# Export Guard Exception Taxonomy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `FreqtradeExporter.export_candles()`/`export_funding()`'s per-symbol
guard actually catch the exception type their own validation code raises
(`ValueError`), classify failures into "skip this symbol/timeframe and report"
vs. "abort the whole run" instead of collapsing everything into one bucket,
and stop discarding successfully-exported timeframes when a later timeframe
in the same symbol fails.

**Architecture:** Introduce `ohlcv_validation.ExportValidationError(ValueError)`
carrying a `location` and a greppable `reason` slug, raised by
`validate_ohlcv`, `assert_export_parity`, and `storage._assert_history_preserved`
in place of bare `ValueError`. `atomic_parquet.py` gains `DATA_DEFECT_ERRORS`
(supersedes `CORRUPT_PARQUET_ERRORS`, which stays as a deprecated alias with
its old value, unchanged) and `is_fatal_environment_error()`, which inspects
an `OSError`'s `errno` to distinguish "disk full" from "this file is corrupt".
Both export loops move their per-symbol `try` inside the per-timeframe loop
(fixing the discarded-results bug), catch `DATA_DEFECT_ERRORS`, re-raise
immediately when `is_fatal_environment_error()` says so, and otherwise record
a new `ExportFailure` (symbol, timeframe, reason, message) alongside the
existing `failed_symbols: list[str]`. All three public methods
(`export_candles`, `export_funding`, `export`) grow a third return value,
`list[ExportFailure]`; the three CLI commands and two existing test files are
updated for the new arity, and the CLI additionally shows the reason slugs
in its failure panel and distinguishes a fatal abort from a per-symbol
failure list.

**Tech Stack:** Python 3.11+, Polars, pandas + pyarrow, Typer CLI, pytest.

**Spec:** `docs/superpowers/specs/2026-09-05-export-guard-exception-taxonomy-design.md`

## Global Constraints

- `ExportValidationError` MUST subclass `ValueError` — every existing
  `except ValueError` call site (storage.py's `save_candles`, `scripts/
  validate_price_continuity.py`, daemon config, etc.) must keep working
  unchanged. Never change an existing raised message string; only change
  the exception *class* and add `.location`/`.reason` attributes.
- `CORRUPT_PARQUET_ERRORS` keeps its exact current value
  `(ArrowInvalid, pl.exceptions.ComputeError, OSError)` — it is a deprecated
  alias, not a name to repoint at the new tuple.
- Fatal environment errnos (skip-vs-abort decision is per this exact set,
  do not add or remove without updating the design doc):
  `errno.ENOSPC, errno.EROFS, errno.EDQUOT, errno.EMFILE, errno.ENFILE`.
- The resolved answer to the design's open question is **skip-and-report**:
  a `validate_ohlcv`/`assert_export_parity` failure on one symbol/timeframe
  must never abort other symbols' exports.
- `failed_symbols: list[str]` keeps its exact current meaning and type for
  API back-compat — every task must keep it a flat sorted list of symbol
  names. `failures: list[ExportFailure]` is purely additive (3rd tuple
  element).
- Never change `_write_both`'s atomicity/rollback logic or
  `sweep_orphaned_tmp_files()` — out of scope per the design doc.

---

### Task 1: `ExportValidationError` in `ohlcv_validation.py`

**Files:**
- Modify: `src/gmx_historical_data/ohlcv_validation.py`
- Test: `tests/test_ohlcv_validation.py`

**Interfaces:**
- Produces: `ExportValidationError(ValueError)` with constructor
  `ExportValidationError(location: str, reason: str, message: str)`, exposing
  `.location: str` and `.reason: str` attributes (message is the standard
  `str(exc)` via `ValueError.__init__`).
- Produces: every raise site inside `validate_ohlcv` and `assert_export_parity`
  now raises `ExportValidationError` instead of `ValueError`, with these exact
  reason slugs (same order as the function): `missing_columns`, `empty_frame`,
  `invalid_timestamps`, `duplicate_timestamps`, `non_monotonic`,
  `invalid_price`, `ohlc_ordering`, `open_scale`, `invalid_volume` (all in
  `validate_ohlcv`), and `parity_missing_columns`, `parity_mismatch` (in
  `assert_export_parity`).

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_ohlcv_validation.py` (append at end of file):

```python
def test_validate_ohlcv_raises_export_validation_error_with_reason():
    from gmx_historical_data.ohlcv_validation import ExportValidationError

    frame = pl.DataFrame({"date": [1], "open": [1.0], "high": [1.0], "low": [1.0]})
    with pytest.raises(ExportValidationError) as excinfo:
        validate_ohlcv(frame, timestamp_column="date", location="X/1h")
    assert excinfo.value.reason == "missing_columns"
    assert excinfo.value.location == "X/1h"
    assert isinstance(excinfo.value, ValueError)


def test_validate_ohlcv_empty_frame_reason_slug():
    from gmx_historical_data.ohlcv_validation import ExportValidationError

    frame = pl.DataFrame(
        {"date": [], "open": [], "high": [], "low": [], "close": []},
        schema={"date": pl.Datetime, "open": pl.Float64, "high": pl.Float64,
                "low": pl.Float64, "close": pl.Float64},
    )
    with pytest.raises(ExportValidationError) as excinfo:
        validate_ohlcv(frame, timestamp_column="date", location="X/1h")
    assert excinfo.value.reason == "empty_frame"


def test_assert_export_parity_raises_export_validation_error_with_reason():
    from gmx_historical_data.ohlcv_validation import ExportValidationError

    left = pl.DataFrame(
        {"date": [1], "open": [1.0], "high": [1.0], "low": [1.0], "close": [1.0], "volume": [0.0]}
    )
    right = pl.DataFrame(
        {"date": [1], "open": [2.0], "high": [1.0], "low": [1.0], "close": [1.0], "volume": [0.0]}
    )
    with pytest.raises(ExportValidationError) as excinfo:
        assert_export_parity(left, right, location="X/1h")
    assert excinfo.value.reason == "parity_mismatch"
```

`tests/test_ohlcv_validation.py` already imports `pytest` and `pl` (polars)
at the top, and imports `count_open_outside_envelope, validate_ohlcv` from
`gmx_historical_data.ohlcv_validation` — but does **not** import
`assert_export_parity` (that's only imported in the separate
`tests/test_drive_integrity_regressions.py`). Update the import block at
the top of `tests/test_ohlcv_validation.py`:

```python
from gmx_historical_data.ohlcv_validation import (
    assert_export_parity,
    count_open_outside_envelope,
    validate_ohlcv,
)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `poetry run pytest tests/test_ohlcv_validation.py -k "export_validation_error or reason_slug" -v`
Expected: FAIL with `ImportError: cannot import name 'ExportValidationError'`.

- [ ] **Step 3: Implement `ExportValidationError` and thread reason slugs**

In `src/gmx_historical_data/ohlcv_validation.py`, add near the top (after the
`OPEN_SCALE_RATIO` constant, before `OhlcvValidationResult`):

```python
class ExportValidationError(ValueError):
    """An OHLCV/export invariant failed — classified as a data defect.

    Subclasses ``ValueError`` so every existing ``except ValueError`` call
    site keeps working unchanged; new code (the export guards) can catch
    this type specifically via
    :data:`~gmx_historical_data.atomic_parquet.DATA_DEFECT_ERRORS`.

    :param location: Human-readable location string, matching the
        ``location`` this validator was called with.
    :param reason: Short greppable slug identifying the failure kind (e.g.
        ``"non_monotonic"``, ``"parity_mismatch"``, ``"history_shrink"``).
    :param message: Full human-readable message; becomes ``str(exc)``.
    """

    def __init__(self, location: str, reason: str, message: str) -> None:
        super().__init__(message)
        self.location = location
        self.reason = reason
```

Then replace every `raise ValueError(...)` inside `validate_ohlcv` with
`raise ExportValidationError(location, "<slug>", ...)`, keeping the exact
existing message text. For example:

```python
    if missing:
        raise ExportValidationError(
            location, "missing_columns", f"{location}: missing required OHLCV columns: {missing}"
        )

    if frame.is_empty():
        raise ExportValidationError(location, "empty_frame", f"{location}: invalid OHLCV empty frame")
```

```python
    if not timestamp_nulls.is_empty():
        first_timestamp = _first_timestamp(timestamp_nulls, timestamp_column)
        raise ExportValidationError(
            location,
            "invalid_timestamps",
            f"{location}: invalid OHLCV timestamp values count={timestamp_nulls.height} "
            f"first_timestamp={first_timestamp}",
        )

    duplicated_timestamps = working.filter(pl.col(timestamp_column).is_duplicated())
    if not duplicated_timestamps.is_empty():
        first_timestamp = _first_timestamp(duplicated_timestamps, timestamp_column)
        raise ExportValidationError(
            location,
            "duplicate_timestamps",
            f"{location}: duplicate timestamps count={duplicated_timestamps.height} "
            f"first_timestamp={first_timestamp}",
        )
```

```python
    if not non_monotonic.is_empty():
        first_timestamp = _first_timestamp(non_monotonic, timestamp_column)
        raise ExportValidationError(
            location,
            "non_monotonic",
            f"{location}: non-monotonic timestamps count={non_monotonic.height} "
            f"first_timestamp={first_timestamp}",
        )
```

```python
    for column in PRICE_COLUMNS:
        invalid = working.filter(
            _invalid_price_expr(column, allow_nonpositive_prices=allow_nonpositive_prices)
        )
        if not invalid.is_empty():
            first_timestamp = _first_timestamp(invalid, timestamp_column)
            raise ExportValidationError(
                location,
                "invalid_price",
                f"{location}: invalid OHLCV {column} values count={invalid.height} "
                f"first_timestamp={first_timestamp}",
            )
```

```python
    if not allow_nonpositive_prices:
        ordering_violations = working.filter(_ordering_violation_expr())
        if not ordering_violations.is_empty():
            first_timestamp = _first_timestamp(ordering_violations, timestamp_column)
            raise ExportValidationError(
                location,
                "ohlc_ordering",
                f"{location}: OHLC ordering violation count={ordering_violations.height} "
                f"first_timestamp={first_timestamp}",
            )

        open_scale_violations = working.filter(_open_scale_violation_expr())
        if not open_scale_violations.is_empty():
            first_timestamp = _first_timestamp(open_scale_violations, timestamp_column)
            raise ExportValidationError(
                location,
                "open_scale",
                f"{location}: OHLC open scale violation count={open_scale_violations.height} "
                f"first_timestamp={first_timestamp}",
            )
```

```python
    if "volume" in working.columns:
        invalid_volume = working.filter(_invalid_volume_expr("volume"))
        if not invalid_volume.is_empty():
            first_timestamp = _first_timestamp(invalid_volume, timestamp_column)
            raise ExportValidationError(
                location,
                "invalid_volume",
                f"{location}: invalid OHLCV volume values count={invalid_volume.height} "
                f"first_timestamp={first_timestamp}",
            )
```

And in `assert_export_parity`:

```python
def assert_export_parity(left: pl.DataFrame, right: pl.DataFrame, *, location: str) -> None:
    """Assert that two exported frames are identical after canonical sorting."""

    missing_left = [column for column in EXPORT_COLUMNS if column not in left.columns]
    missing_right = [column for column in EXPORT_COLUMNS if column not in right.columns]
    if missing_left or missing_right:
        raise ExportValidationError(
            location,
            "parity_missing_columns",
            f"{location}: Feather/Parquet export parity mismatch: "
            f"missing_left={missing_left}, missing_right={missing_right}",
        )

    canonical_left = _canonical_export_frame(left)
    canonical_right = _canonical_export_frame(right)
    if canonical_left.equals(canonical_right):
        return

    raise ExportValidationError(
        location, "parity_mismatch", f"{location}: Feather/Parquet export parity mismatch"
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `poetry run pytest tests/test_ohlcv_validation.py tests/test_drive_integrity_regressions.py -v`
Expected: all PASS (the pre-existing `pytest.raises(ValueError, match=...)`
assertions in these files must still pass unchanged, since
`ExportValidationError` is a `ValueError` subclass and messages are
byte-for-byte identical).

- [ ] **Step 5: Commit**

```bash
git add src/gmx_historical_data/ohlcv_validation.py tests/test_ohlcv_validation.py
git commit -m "feat(export-guard): add ExportValidationError with reason slugs to ohlcv_validation" \
  --trailer "Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>" \
  --trailer "Claude-Session: https://claude.ai/code/session_01EmpMDuZLkvswy5JkBZHSEk"
```

---

### Task 2: Route `storage._assert_history_preserved` through `ExportValidationError`

**Files:**
- Modify: `src/gmx_historical_data/storage.py`
- Test: `tests/test_storage.py` (create the two new cases there if a
  matching test file doesn't already assert on `_assert_history_preserved`
  directly — check first with `grep -rn "_assert_history_preserved" tests/`)

**Interfaces:**
- Consumes: `ExportValidationError(location, reason, message)` from Task 1
  (`gmx_historical_data.ohlcv_validation`).
- Produces: `_assert_history_preserved(...)` now raises
  `ExportValidationError(location, "history_shrink", message)` instead of
  bare `ValueError`, for both its raise sites (earliest-shrink and
  latest-shrink). Message text is unchanged.

- [ ] **Step 1: Confirm no existing test asserts the raised type directly**

Run: `grep -rn "_assert_history_preserved" tests/`
This function is private (leading underscore) and is exercised indirectly
through `save_candles()` / the exporter's merge path, not called directly in
tests today (confirmed empty result from an earlier repo-wide grep). If this
grep finds a direct call, read that test before proceeding — it may need a
type update in Step 4 below alongside everything else.

- [ ] **Step 2: Write the failing test**

Add a new test file `tests/test_assert_history_preserved.py`:

```python
"""Unit tests for storage._assert_history_preserved's exception taxonomy."""

from datetime import UTC, datetime

import pytest

from gmx_historical_data.ohlcv_validation import ExportValidationError
from gmx_historical_data.storage import _assert_history_preserved


def test_assert_history_preserved_raises_export_validation_error_on_shrink():
    existing_stats = {
        "rows": 10,
        "earliest": datetime(2024, 1, 1, tzinfo=UTC),
        "latest": datetime(2024, 1, 10, tzinfo=UTC),
    }
    incoming_stats = {
        "rows": 5,
        "earliest": datetime(2024, 1, 5, tzinfo=UTC),
        "latest": datetime(2024, 1, 10, tzinfo=UTC),
    }
    merged_stats = {
        "rows": 5,
        "earliest": datetime(2024, 1, 5, tzinfo=UTC),
        "latest": datetime(2024, 1, 10, tzinfo=UTC),
    }
    with pytest.raises(ExportValidationError) as excinfo:
        _assert_history_preserved(
            existing_stats, incoming_stats, merged_stats, ts_label="date", location="TEST/1h"
        )
    assert excinfo.value.reason == "history_shrink"
    assert excinfo.value.location == "TEST/1h"
    assert isinstance(excinfo.value, ValueError)
```

- [ ] **Step 3: Run test to verify it fails**

Run: `poetry run pytest tests/test_assert_history_preserved.py -v`
Expected: FAIL (`ExportValidationError` raised as plain `ValueError` today —
`pytest.raises(ExportValidationError)` does not match a bare `ValueError`
instance, so this fails with `Failed: DID NOT RAISE` semantics via a type
mismatch... actually pytest.raises requires the *exact* raised type to match
the given type or a subclass of it; since `ValueError` is not a subclass of
`ExportValidationError`, this correctly fails).

- [ ] **Step 4: Implement**

Read `src/gmx_historical_data/storage.py:17-19` imports; change:

```python
from gmx_historical_data.ohlcv_validation import (
    validate_ohlcv,
)
```

to:

```python
from gmx_historical_data.ohlcv_validation import (
    ExportValidationError,
    validate_ohlcv,
)
```

Then in `_assert_history_preserved` (around line 42-72), replace both
`raise ValueError(...)` calls:

```python
    if merged_stats["earliest"] is None or merged_stats["earliest"] > existing_stats["earliest"]:
        raise ExportValidationError(
            location,
            "history_shrink",
            f"{location}: merge would shorten history for {ts_label}: "
            f"existing earliest={existing_stats['earliest']}, merged earliest={merged_stats['earliest']}",
        )

    expected_latest = max(
        ts for ts in (existing_stats["latest"], incoming_stats["latest"]) if ts is not None
    )
    if merged_stats["latest"] is None or merged_stats["latest"] < expected_latest:
        raise ExportValidationError(
            location,
            "history_shrink",
            f"{location}: merge would shorten history for {ts_label}: "
            f"expected latest>={expected_latest}, merged latest={merged_stats['latest']}",
        )
```

Read the exact current second-raise message text in the file before editing
(it was truncated in the earlier `grep -A 30`) and preserve it verbatim,
only swapping the exception class and adding the two new leading arguments.

- [ ] **Step 5: Run tests to verify they pass**

Run: `poetry run pytest tests/test_assert_history_preserved.py -v`
Expected: PASS.

Run: `poetry run pytest tests/ -k "history_preserved or save_candles or merge" -v`
Expected: all PASS (no regressions in `save_candles`'s own history guard or
the exporter's merge path, both of which call this function).

- [ ] **Step 6: Commit**

```bash
git add src/gmx_historical_data/storage.py tests/test_assert_history_preserved.py
git commit -m "feat(export-guard): raise ExportValidationError from _assert_history_preserved" \
  --trailer "Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>" \
  --trailer "Claude-Session: https://claude.ai/code/session_01EmpMDuZLkvswy5JkBZHSEk"
```

---

### Task 3: `DATA_DEFECT_ERRORS` and `is_fatal_environment_error()` in `atomic_parquet.py`

**Files:**
- Modify: `src/gmx_historical_data/atomic_parquet.py`
- Test: `tests/test_atomic_parquet.py` (check existing filename first via
  `find . -name "test_atomic_parquet*"`; if it doesn't exist, create it)

**Interfaces:**
- Consumes: `ExportValidationError` from Task 1
  (`gmx_historical_data.ohlcv_validation`).
- Produces: `DATA_DEFECT_ERRORS: tuple[type[Exception], ...]` =
  `(ArrowInvalid, pl.exceptions.ComputeError, ExportValidationError, OSError)`.
- Produces: `is_fatal_environment_error(exc: BaseException) -> bool`,
  `True` iff `exc` is an `OSError` whose `.errno` is one of
  `{errno.ENOSPC, errno.EROFS, errno.EDQUOT, errno.EMFILE, errno.ENFILE}`.
- Produces: `CORRUPT_PARQUET_ERRORS` UNCHANGED — same tuple value as today,
  `(ArrowInvalid, pl.exceptions.ComputeError, OSError)`, kept only as a
  deprecated alias for any external/back-compat importer.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_atomic_parquet_taxonomy.py`:

```python
"""Tests for atomic_parquet's data-defect vs. fatal-environment taxonomy."""

import errno

import polars as pl
import pytest
from pyarrow.lib import ArrowInvalid

from gmx_historical_data.atomic_parquet import (
    CORRUPT_PARQUET_ERRORS,
    DATA_DEFECT_ERRORS,
    is_fatal_environment_error,
)
from gmx_historical_data.ohlcv_validation import ExportValidationError


def test_corrupt_parquet_errors_alias_still_importable():
    """Back-compat: the deprecated alias keeps its historical value."""
    assert CORRUPT_PARQUET_ERRORS == (ArrowInvalid, pl.exceptions.ComputeError, OSError)


def test_data_defect_errors_includes_export_validation_error():
    assert ExportValidationError in DATA_DEFECT_ERRORS
    assert ArrowInvalid in DATA_DEFECT_ERRORS
    assert pl.exceptions.ComputeError in DATA_DEFECT_ERRORS
    assert OSError in DATA_DEFECT_ERRORS


@pytest.mark.parametrize(
    "errno_value", [errno.ENOSPC, errno.EROFS, errno.EDQUOT, errno.EMFILE, errno.ENFILE]
)
def test_is_fatal_environment_error_true_for_fatal_errnos(errno_value):
    exc = OSError(errno_value, "simulated")
    assert is_fatal_environment_error(exc) is True


def test_is_fatal_environment_error_false_for_other_oserror():
    exc = OSError(errno.ENOENT, "file not found")
    assert is_fatal_environment_error(exc) is False


def test_is_fatal_environment_error_false_for_non_oserror():
    assert is_fatal_environment_error(ValueError("not an OSError")) is False


def test_is_fatal_environment_error_false_for_export_validation_error():
    exc = ExportValidationError("X/1h", "non_monotonic", "X/1h: non-monotonic timestamps")
    assert is_fatal_environment_error(exc) is False
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `poetry run pytest tests/test_atomic_parquet_taxonomy.py -v`
Expected: FAIL with `ImportError: cannot import name 'DATA_DEFECT_ERRORS'`.

- [ ] **Step 3: Implement**

In `src/gmx_historical_data/atomic_parquet.py`, add `import errno` to the
imports at the top (after `import logging`) and add the import of
`ExportValidationError`:

```python
import errno
import logging
import os
from pathlib import Path
from typing import Any

import pandas as pd
import polars as pl
from pyarrow.lib import ArrowInvalid

from gmx_historical_data.ohlcv_validation import ExportValidationError
```

Immediately after the existing `CORRUPT_PARQUET_ERRORS` definition and its
docstring comment (leave that block completely unchanged), append:

```python
#: Fatal filesystem/OS-resource conditions where the correct behaviour is to
#: abort the whole export run rather than treat one symbol as corrupt --
#: continuing would mark every remaining symbol "failed" one at a time for a
#: condition that has nothing to do with any of their data. See
#: :func:`is_fatal_environment_error`.
FATAL_ENVIRONMENT_ERRNOS: frozenset[int] = frozenset(
    {errno.ENOSPC, errno.EROFS, errno.EDQUOT, errno.EMFILE, errno.ENFILE}
)


def is_fatal_environment_error(exc: BaseException) -> bool:
    """Return ``True`` if ``exc`` is an ``OSError`` from a fatal environment condition.

    Distinguishes "the disk is full" (``ENOSPC``), a read-only remount
    (``EROFS``), a quota hit (``EDQUOT``), or exhausted file descriptors
    (``EMFILE``/``ENFILE``) from an ordinary "this file is corrupt"
    ``OSError``. Callers must check this *before* treating an ``OSError`` as
    a per-symbol data defect, and re-raise instead of skipping when it is
    ``True`` -- see ``FreqtradeExporter.export_candles``/``export_funding``.

    :param exc: The caught exception.
    :returns: ``True`` iff ``exc`` is an ``OSError`` whose ``errno`` is in
        :data:`FATAL_ENVIRONMENT_ERRNOS`.
    """
    return isinstance(exc, OSError) and exc.errno in FATAL_ENVIRONMENT_ERRNOS


#: Exception types that mean "this symbol/timeframe's data is defective --
#: skip it and keep going", classified by what the operator should do
#: rather than by which library raised it. Supersedes
#: :data:`CORRUPT_PARQUET_ERRORS` for new call sites: adds
#: :class:`~gmx_historical_data.ohlcv_validation.ExportValidationError` so a
#: ``validate_ohlcv``/``assert_export_parity``/history-guard failure is
#: caught by the same per-symbol guard as a truncated Parquet file, instead
#: of propagating and aborting the whole export. ``OSError`` here still
#: needs an :func:`is_fatal_environment_error` check first -- an ``OSError``
#: matching that check must be re-raised, not treated as a data defect.
DATA_DEFECT_ERRORS: tuple[type[Exception], ...] = (
    ArrowInvalid,
    pl.exceptions.ComputeError,
    ExportValidationError,
    OSError,
)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `poetry run pytest tests/test_atomic_parquet_taxonomy.py -v`
Expected: all PASS.

Run: `poetry run pytest tests/ -k "atomic_parquet" -v`
Expected: all PASS (no regression in existing `test_atomic_parquet_writes.py`).

- [ ] **Step 5: Commit**

```bash
git add src/gmx_historical_data/atomic_parquet.py tests/test_atomic_parquet_taxonomy.py
git commit -m "feat(export-guard): add DATA_DEFECT_ERRORS and is_fatal_environment_error" \
  --trailer "Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>" \
  --trailer "Claude-Session: https://claude.ai/code/session_01EmpMDuZLkvswy5JkBZHSEk"
```

---

### Task 4: `ExportFailure` dataclass and restructured `export_candles()`

**Files:**
- Modify: `src/gmx_historical_data/freqtrade_exporter.py`
- Test: `tests/test_export_survives_corrupt_parquet.py` (update existing
  2-tuple unpacks to 3-tuple; add new taxonomy tests)

**Interfaces:**
- Consumes: `DATA_DEFECT_ERRORS`, `is_fatal_environment_error` from Task 3
  (`gmx_historical_data.atomic_parquet`); `ExportValidationError` from Task 1
  (`gmx_historical_data.ohlcv_validation`).
- Produces: `ExportFailure` frozen dataclass with fields `symbol: str`,
  `timeframe: str`, `reason: str`, `message: str`.
- Produces: `export_candles(...) -> tuple[dict[str, dict], list[str], list[ExportFailure]]`
  (was `tuple[dict[str, dict], list[str]]`) — third element is new, first two
  keep their exact current meaning. A symbol whose *some* timeframes
  succeeded and *some* failed now appears in **both** `results` (for the
  surviving timeframes' counts) and `failed_symbols` (this is new — the
  design's D2 fix — and is intentional, not a bug).
- Produces (module-level, private): `_classify_export_failure(symbol, timeframe, exc) -> ExportFailure`,
  used identically by `export_candles` and `export_funding` (Task 5).

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_export_survives_corrupt_parquet.py`, near the top,
`ExportValidationError` import:

```python
from gmx_historical_data.ohlcv_validation import ExportValidationError
```

Then update the THREE existing 2-tuple unpacks in this file to 3-tuple —
find each and change:

```python
    results, failed_symbols = exporter.export_candles(
```
to
```python
    results, failed_symbols, failures = exporter.export_candles(
```
(applies to `test_export_survives_corrupt_parquet` and
`test_export_survives_corrupt_destination_feather`), and:
```python
    results, failed_symbols = exporter.export(symbols=["AAA", "BBB", "CCC"], timeframes=["1h"])
```
to
```python
    results, failed_symbols, failures = exporter.export(
        symbols=["AAA", "BBB", "CCC"], timeframes=["1h"]
    )
```
(applies to `test_export_wrapper_propagates_failed_symbols`). Do not add
assertions on `failures` in these three existing tests yet — Step 1 here
only fixes their arity so they still run; new taxonomy assertions go in the
new tests below (this keeps the diff to these three tests reviewable as
"fix arity", not "change behavior").

Append these new tests at the end of the file:

```python
def _write_funding_style_duplicate_candles(
    data_dir: Path, symbol: str, timeframe: str = "1h"
) -> None:
    """Write a candle parquet directly with a duplicate timestamp row.

    Bypasses ``ParquetStorage.save_candles()`` (which validates on write) so
    the corruption is only visible when the *exporter* reads and validates
    it -- reproducing "upstream wrote something save_candles would have
    rejected, but it's on disk anyway" rather than a byte-level truncation.

    :param data_dir: Root data directory.
    :param symbol: Token symbol to seed with a duplicate-timestamp candle file.
    :param timeframe: Timeframe filename stem to write (e.g. ``'1h'``,
        ``'4h'``). Both map to themselves under ``TIMEFRAME_TO_FILENAME``, so
        the filename is exactly ``f"{timeframe}.parquet"``.
    """
    candle_dir = data_dir / "candles" / "arbitrum" / symbol
    candle_dir.mkdir(parents=True, exist_ok=True)
    timestamps = pd.date_range("2024-01-01", periods=3, freq=timeframe, tz="UTC")
    df = pd.DataFrame(
        {
            "timestamp": [timestamps[0], timestamps[0], timestamps[1]],
            "open": [1.0, 1.0, 1.0],
            "high": [1.0, 1.0, 1.0],
            "low": [1.0, 1.0, 1.0],
            "close": [1.0, 1.0, 1.0],
        }
    )
    df.to_parquet(candle_dir / f"{timeframe}.parquet", index=False)


def test_export_candles_shares_taxonomy(tmp_path: Path):
    """A validate_ohlcv failure (not a corrupt-bytes failure) is caught by
    the same per-symbol guard as ArrowInvalid -- the exact defect this
    change fixes. Before this change, this ValueError would propagate and
    abort the whole export."""
    data_dir = tmp_path / "data"
    storage = ParquetStorage(data_dir)
    for symbol in ("AAA", "CCC"):
        storage.save_candles(_make_candles(symbol), "1h", symbol)
    _write_funding_style_duplicate_candles(data_dir, "BBB")

    output_dir = tmp_path / "output"
    exporter = FreqtradeExporter(data_dir, output_dir)
    results, failed_symbols, failures = exporter.export_candles(
        symbols=["AAA", "BBB", "CCC"], timeframes=["1h"]
    )

    assert failed_symbols == ["BBB"]
    assert "BBB" not in results
    assert set(results) == {"AAA", "CCC"}
    assert len(failures) == 1
    assert failures[0].symbol == "BBB"
    assert failures[0].timeframe == "1h"
    assert failures[0].reason == "duplicate_timestamps"


def test_export_candles_isolates_timeframes(tmp_path: Path):
    """D2: a symbol failing on one timeframe still gets its other
    timeframe's files counted in results, not discarded."""
    data_dir = tmp_path / "data"
    storage = ParquetStorage(data_dir)
    storage.save_candles(_make_candles("BBB"), "1h", "BBB")  # healthy 1h
    _write_funding_style_duplicate_candles(data_dir, "BBB", timeframe="4h")  # corrupt 4h

    output_dir = tmp_path / "output"
    exporter = FreqtradeExporter(data_dir, output_dir)
    results, failed_symbols, failures = exporter.export_candles(
        symbols=["BBB"], timeframes=["1h", "4h"]
    )

    assert failed_symbols == ["BBB"]
    assert "BBB" in results
    assert results["BBB"]["ohlcv_files"] == 1
    assert len(failures) == 1
    assert failures[0].timeframe == "4h"
    futures_dir = output_dir / "gmx" / "futures"
    assert (futures_dir / "BBB_USDC_USDC-1h-futures.feather").exists()
    assert not (futures_dir / "BBB_USDC_USDC-4h-futures.feather").exists()


def test_export_candles_aborts_on_enospc(tmp_path: Path, monkeypatch):
    """A fatal environment error (disk full) must propagate, not be
    swallowed into failed_symbols -- continuing would mark every remaining
    symbol 'failed' one at a time for a condition that isn't about their data."""
    import errno

    from gmx_historical_data import freqtrade_exporter as fe_module

    data_dir = tmp_path / "data"
    storage = ParquetStorage(data_dir)
    storage.save_candles(_make_candles("AAA"), "1h", "AAA")

    def _raise_enospc(*args, **kwargs):
        raise OSError(errno.ENOSPC, "No space left on device")

    monkeypatch.setattr(fe_module, "atomic_write_ipc", _raise_enospc)

    exporter = FreqtradeExporter(data_dir, tmp_path / "output")
    with pytest.raises(OSError) as excinfo:
        exporter.export_candles(symbols=["AAA"], timeframes=["1h"])
    assert excinfo.value.errno == errno.ENOSPC
```

Add `import pytest` at the top of the test file if not already present
(check first — the file currently has no `pytest.raises` usage, so it is
likely not imported yet).

- [ ] **Step 2: Run tests to verify they fail**

Run: `poetry run pytest tests/test_export_survives_corrupt_parquet.py -v`
Expected: FAIL — the three arity-fixed tests fail with
`ValueError: too many values to unpack` (still 2-tuple today), and the four
new tests fail (two on arity, one on `AttributeError`/behavior since
`export_candles` doesn't classify `ValueError` yet, one because ENOSPC is
currently swallowed by the `except CORRUPT_PARQUET_ERRORS` catching bare
`OSError`).

- [ ] **Step 3: Implement `ExportFailure`, `_classify_export_failure`, and restructure `export_candles`**

In `src/gmx_historical_data/freqtrade_exporter.py`, update imports (top of
file):

```python
import logging
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

import polars as pl

from gmx_historical_data.atomic_parquet import (
    DATA_DEFECT_ERRORS,
    atomic_write_ipc,
    atomic_write_parquet,
    is_fatal_environment_error,
)
from gmx_historical_data.ohlcv_validation import (
    ExportValidationError,
    assert_export_parity,
    validate_ohlcv,
)
from gmx_historical_data.storage import (
    ParquetStorage,
    _assert_history_preserved,
    _coverage_stats,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ExportFailure:
    """One skipped ``(symbol, timeframe)`` during export.

    Carries enough structure for the CLI's failure panel and any downstream
    alerting to show *what kind* of failure occurred, not just that one did
    -- the whole point of this taxonomy (see the 2026-09-05 design doc).

    :param symbol: Token symbol that failed.
    :param timeframe: Timeframe that failed.
    :param reason: Short greppable slug -- an
        :class:`~gmx_historical_data.ohlcv_validation.ExportValidationError`'s
        own ``.reason``, or the caught exception's class name (e.g.
        ``"ArrowInvalid"``, ``"ComputeError"``, ``"OSError"``) when it isn't
        one.
    :param message: Full exception message, for logs/failure panels.
    """

    symbol: str
    timeframe: str
    reason: str
    message: str


def _classify_export_failure(symbol: str, timeframe: str, exc: Exception) -> ExportFailure:
    """Build an :class:`ExportFailure` from a caught data-defect exception.

    :param symbol: The symbol being exported when ``exc`` was raised.
    :param timeframe: The timeframe being exported when ``exc`` was raised.
    :param exc: The caught exception (a member of
        :data:`~gmx_historical_data.atomic_parquet.DATA_DEFECT_ERRORS`).
    :returns: An :class:`ExportFailure` with ``reason`` taken from
        ``exc.reason`` when ``exc`` is an
        :class:`~gmx_historical_data.ohlcv_validation.ExportValidationError`,
        else the exception's class name.
    """
    reason = exc.reason if isinstance(exc, ExportValidationError) else type(exc).__name__
    return ExportFailure(symbol=symbol, timeframe=timeframe, reason=reason, message=str(exc))
```

`CORRUPT_PARQUET_ERRORS` is no longer imported here — this file now only
uses `DATA_DEFECT_ERRORS`.

Replace the entire body of `export_candles` (keep the signature's parameter
list unchanged except the return type annotation) with:

```python
    def export_candles(
        self,
        symbols: list[str] | None = None,
        timeframes: list[str] | None = None,
        output_format: str = "feather",
        trading_mode: str = "futures",
        quote_currency: str = "USDC",
        overwrite: bool = False,
        unsafe_overwrite: bool = False,
        keep_parquet: bool = True,
    ) -> tuple[dict[str, dict], list[str], list[ExportFailure]]:
        """Export OHLCV (candles + mark + index) feathers only.

        Reads only from ``{data_dir}/candles/`` and writes only ``-futures``,
        ``-mark``, and ``-index`` feathers.  Never touches funding files.

        The per-symbol, per-timeframe guard catches
        :data:`~gmx_historical_data.atomic_parquet.DATA_DEFECT_ERRORS` --
        both a corrupt source/destination Parquet or Feather file
        (``ArrowInvalid``, ``pl.exceptions.ComputeError``) and a validation
        failure from this module's own transform
        (:class:`~gmx_historical_data.ohlcv_validation.ExportValidationError`,
        covering ``validate_ohlcv``, ``assert_export_parity``, and the
        history-preservation guard). A fatal environment ``OSError`` (see
        :func:`~gmx_historical_data.atomic_parquet.is_fatal_environment_error`
        -- e.g. disk full) is re-raised immediately rather than treated as a
        per-symbol defect, since it says nothing about any one symbol's data.

        The guard wraps each *timeframe* individually, not the whole symbol:
        a symbol with 3 healthy timeframes and 1 failing one still gets the
        3 healthy timeframes counted in ``results`` and appears in
        ``failed_symbols`` for the one that failed -- both can be true for
        the same symbol at once.

        :param symbols: Specific symbols (default: all candle symbols).
        :param timeframes: Specific timeframes (default: all available).
        :param output_format: ``'feather'`` or ``'parquet'``.
        :param trading_mode: ``'futures'`` or ``'spot'``.
        :param quote_currency: Quote/settlement currency (default ``'USDC'``).
        :param overwrite: Backward-compat alias; merges with history guard.
        :param unsafe_overwrite: Bypass the history guard.  Schema migrations only.
        :param keep_parquet: Default ``True``.  If ``False`` the source candle
            parquet is deleted after a successful feather export.
        :returns: Tuple of (dict mapping symbol to export stats, sorted list
            of symbols with at least one failed timeframe, list of
            :class:`ExportFailure` detailing each failed timeframe).
        :raises OSError: If a fatal environment condition (disk full,
            read-only filesystem, quota, or file-descriptor exhaustion) is
            hit -- see
            :func:`~gmx_historical_data.atomic_parquet.is_fatal_environment_error`.
        """
        gmx_dir = self._make_gmx_dir(trading_mode)

        candle_symbols = set(self.storage.list_symbols())
        export_symbols = (
            sorted(s for s in symbols if s in candle_symbols) if symbols else sorted(candle_symbols)
        )

        results: dict[str, dict] = {}
        failed_symbols: list[str] = []
        failures: list[ExportFailure] = []
        for symbol in export_symbols:
            ohlcv_files = mark_files = index_files = total_candles = 0
            candle_tfs = set(self.storage.list_timeframes(symbol))
            export_tfs = (
                [tf for tf in timeframes if tf in candle_tfs]
                if timeframes
                else sorted(candle_tfs)
            )
            symbol_failed = False

            for tf in export_tfs:
                try:
                    raw = self.storage.read_candles(tf, symbol)
                    if raw.empty:
                        continue
                    df = pl.from_pandas(raw)

                    ft_df = validate_ohlcv(
                        self._transform_dataframe(df),
                        timestamp_column="date",
                        location=f"export_candles({symbol}/{tf})",
                    )
                    self._write(
                        ft_df,
                        gmx_dir
                        / self._get_freqtrade_filename(
                            symbol,
                            tf,
                            "feather" if output_format == "both" else output_format,
                            trading_mode,
                            quote_currency,
                        ),
                        output_format,
                        overwrite,
                        unsafe_overwrite,
                    )
                    ohlcv_files += 2 if output_format == "both" else 1
                    total_candles += len(ft_df)

                    mark_df = validate_ohlcv(
                        self._transform_mark_price(df),
                        timestamp_column="date",
                        location=f"export_candles({symbol}/{tf}) mark",
                    )
                    self._write(
                        mark_df,
                        gmx_dir
                        / self._get_freqtrade_filename(
                            symbol,
                            tf,
                            "feather" if output_format == "both" else output_format,
                            trading_mode,
                            quote_currency,
                            candle_type="mark",
                        ),
                        output_format,
                        overwrite,
                        unsafe_overwrite,
                    )
                    mark_files += 2 if output_format == "both" else 1

                    self._write(
                        mark_df,
                        gmx_dir
                        / self._get_freqtrade_filename(
                            symbol,
                            tf,
                            "feather" if output_format == "both" else output_format,
                            trading_mode,
                            quote_currency,
                            candle_type="index",
                        ),
                        output_format,
                        overwrite,
                        unsafe_overwrite,
                    )
                    index_files += 2 if output_format == "both" else 1

                    if not keep_parquet and output_format in {"feather", "both"}:
                        self._cleanup_candle_source(symbol, tf)
                except DATA_DEFECT_ERRORS as e:
                    if is_fatal_environment_error(e):
                        raise
                    logger.error(
                        "export_candles(%s/%s): skipping timeframe after read/write failure: %s",
                        symbol,
                        tf,
                        e,
                    )
                    failures.append(_classify_export_failure(symbol, tf, e))
                    symbol_failed = True
                    continue

            if ohlcv_files or mark_files or index_files:
                results[symbol] = {
                    "files": ohlcv_files + mark_files + index_files,
                    "candles": total_candles,
                    "ohlcv_files": ohlcv_files,
                    "funding_files": 0,
                    "mark_files": mark_files,
                    "index_files": index_files,
                }
            if symbol_failed:
                failed_symbols.append(symbol)

        return results, failed_symbols, failures
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `poetry run pytest tests/test_export_survives_corrupt_parquet.py -v`
Expected: all PASS. Note `test_export_candles_shares_taxonomy` currently
imports `pd` and `ParquetStorage`, both already imported at the top of this
test file (confirmed earlier) — no new imports needed there beyond `pytest`
and `ExportValidationError` (unused directly in the new tests above except
via `isinstance` — if `ExportValidationError` ends up unused after writing
the tests, remove that import to keep lint clean; check with
`ruff check tests/test_export_survives_corrupt_parquet.py` if configured,
otherwise just confirm no unused-import warning from your editor/linter).

- [ ] **Step 5: Commit**

```bash
git add src/gmx_historical_data/freqtrade_exporter.py tests/test_export_survives_corrupt_parquet.py
git commit -m "feat(export-guard): restructure export_candles with per-timeframe taxonomy guard" \
  --trailer "Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>" \
  --trailer "Claude-Session: https://claude.ai/code/session_01EmpMDuZLkvswy5JkBZHSEk"
```

---

### Task 5: Restructured `export_funding()`

**Files:**
- Modify: `src/gmx_historical_data/freqtrade_exporter.py`
- Test: `tests/test_export_funding_survives_corrupt_parquet.py`

**Interfaces:**
- Consumes: `ExportFailure`, `_classify_export_failure` from Task 4 (same
  module); `DATA_DEFECT_ERRORS`, `is_fatal_environment_error` (Task 3);
  `ExportValidationError` (Task 1); `assert_export_parity` (Task 1, imported
  already in this module).
- Produces: `export_funding(...) -> tuple[dict[str, dict], list[str], list[ExportFailure]]`,
  same per-timeframe-guard semantics as `export_candles` in Task 4.

- [ ] **Step 1: Write the failing tests**

In `tests/test_export_funding_survives_corrupt_parquet.py`, add imports at
the top:

```python
import errno

import pytest

from gmx_historical_data import freqtrade_exporter as fe_module
from gmx_historical_data.ohlcv_validation import ExportValidationError
```

Fix the THREE existing 2-tuple unpacks to 3-tuple (same mechanical change as
Task 4 Step 1 — locate each `results, failed_symbols = exporter.export...`
line in this file and rename to `results, failed_symbols, failures =`).

Append these new tests at the end of the file:

```python
def _seed_funding_symbol(data_dir: Path, symbol: str, df: pl.DataFrame) -> None:
    """Write a single funding-rate symbol's parquet directly (no truncation).

    :param data_dir: Root data directory.
    :param symbol: Token symbol.
    :param df: The funding-rate DataFrame to write as-is.
    """
    symbol_dir = data_dir / "funding" / "arbitrum" / "rates" / symbol
    symbol_dir.mkdir(parents=True, exist_ok=True)
    df.write_parquet(symbol_dir / "1h.parquet")


def test_export_funding_survives_validation_error(tmp_path: Path):
    """A validate_ohlcv failure (duplicate timestamps, not corrupt bytes) is
    caught by the same guard as ArrowInvalid/ComputeError -- the exact
    defect this change fixes."""
    data_dir = tmp_path / "data"
    for symbol in ("AAA", "CCC"):
        _seed_funding_symbol(data_dir, symbol, _make_funding_df())

    bad = _make_funding_df(hours=2)
    bad = pl.concat([bad, bad.head(1)])  # duplicate the first timestamp
    _seed_funding_symbol(data_dir, "BBB", bad)

    output_dir = tmp_path / "output"
    exporter = FreqtradeExporter(data_dir, output_dir)
    results, failed_symbols, failures = exporter.export_funding(
        symbols=["AAA", "BBB", "CCC"], timeframes=["1h"]
    )

    assert failed_symbols == ["BBB"]
    assert "BBB" not in results
    assert set(results) == {"AAA", "CCC"}
    assert len(failures) == 1
    assert failures[0].symbol == "BBB"
    assert failures[0].reason == "duplicate_timestamps"


def test_export_funding_survives_parity_mismatch(tmp_path: Path, monkeypatch):
    """A forced assert_export_parity failure on one symbol (output_format
    'both') is caught by the guard; other symbols still export."""
    data_dir = tmp_path / "data"
    for symbol in ("AAA", "BBB", "CCC"):
        _seed_funding_symbol(data_dir, symbol, _make_funding_df())

    from gmx_historical_data import ohlcv_validation

    real_assert_export_parity = ohlcv_validation.assert_export_parity

    def fake_assert_export_parity(left, right, *, location):
        if "BBB" in location:
            raise ExportValidationError(
                location, "parity_mismatch", f"{location}: forced parity mismatch for test"
            )
        return real_assert_export_parity(left, right, location=location)

    monkeypatch.setattr(fe_module, "assert_export_parity", fake_assert_export_parity)

    output_dir = tmp_path / "output"
    exporter = FreqtradeExporter(data_dir, output_dir)
    results, failed_symbols, failures = exporter.export_funding(
        symbols=["AAA", "BBB", "CCC"], timeframes=["1h"], output_format="both"
    )

    assert failed_symbols == ["BBB"]
    assert set(results) == {"AAA", "CCC"}
    assert len(failures) == 1
    assert failures[0].reason == "parity_mismatch"


def test_export_funding_aborts_on_enospc(tmp_path: Path, monkeypatch):
    """A fatal environment error must propagate, not be recorded as a
    per-symbol failure."""
    data_dir = tmp_path / "data"
    _seed_funding_symbol(data_dir, "AAA", _make_funding_df())

    def _raise_enospc(*args, **kwargs):
        raise OSError(errno.ENOSPC, "No space left on device")

    monkeypatch.setattr(fe_module, "atomic_write_ipc", _raise_enospc)

    exporter = FreqtradeExporter(data_dir, tmp_path / "output")
    with pytest.raises(OSError) as excinfo:
        exporter.export_funding(symbols=["AAA"], timeframes=["1h"])
    assert excinfo.value.errno == errno.ENOSPC


def test_export_funding_isolates_timeframes(tmp_path: Path):
    """D2: BBB fails on 8h only -- its 1h file still exists and is counted."""
    data_dir = tmp_path / "data"
    _seed_funding_symbol(data_dir, "BBB", _make_funding_df())  # healthy 1h

    bad = _make_funding_df(hours=2)
    bad = pl.concat([bad, bad.head(1)])  # duplicate timestamp -> validate_ohlcv raises
    symbol_dir = data_dir / "funding" / "arbitrum" / "rates" / "BBB"
    bad.write_parquet(symbol_dir / "8h.parquet")

    output_dir = tmp_path / "output"
    exporter = FreqtradeExporter(data_dir, output_dir)
    results, failed_symbols, failures = exporter.export_funding(
        symbols=["BBB"], timeframes=["1h", "8h"]
    )

    assert failed_symbols == ["BBB"]
    assert "BBB" in results
    assert results["BBB"]["funding_files"] == 1
    assert len(failures) == 1
    assert failures[0].timeframe == "8h"
    futures_dir = output_dir / "gmx" / "futures"
    assert (futures_dir / "BBB_USDC_USDC-1h-funding_rate.feather").exists()
    assert not (futures_dir / "BBB_USDC_USDC-8h-funding_rate.feather").exists()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `poetry run pytest tests/test_export_funding_survives_corrupt_parquet.py -v`
Expected: FAIL (arity mismatches on the fixed tests; behavior mismatches on
the new ones, same reasons as Task 4 Step 2).

- [ ] **Step 3: Implement — restructure `export_funding`**

Replace the entire body of `export_funding` (keep the signature's parameter
list unchanged except the return type annotation) with:

```python
    def export_funding(
        self,
        symbols: list[str] | None = None,
        timeframes: list[str] | None = None,
        output_format: str = "feather",
        trading_mode: str = "futures",
        quote_currency: str = "USDC",
        overwrite: bool = False,
        unsafe_overwrite: bool = False,
    ) -> tuple[dict[str, dict], list[str], list[ExportFailure]]:
        """Export funding_rate feathers only.

        Reads only from ``{data_dir}/funding/`` and writes only
        ``-funding_rate`` feathers.  Never touches OHLCV files.  The funding
        parquet source is owned by the unified-funding pipeline — this
        method never deletes it.

        Shares :meth:`export_candles`'s per-symbol, per-timeframe guard over
        :data:`~gmx_historical_data.atomic_parquet.DATA_DEFECT_ERRORS` --
        see that method's docstring for the full contract, including the
        fatal-environment-error re-raise and the "a symbol can be in both
        ``results`` and ``failed_symbols``" semantics.

        :param symbols: Specific symbols (default: all funding symbols).
        :param timeframes: Specific timeframes (default: all available).
        :param output_format: ``'feather'`` or ``'parquet'``.
        :param trading_mode: ``'futures'`` or ``'spot'``.
        :param quote_currency: Quote/settlement currency (default ``'USDC'``).
        :param overwrite: Backward-compat alias; merges with history guard.
        :param unsafe_overwrite: Bypass the history guard.  Schema migrations only.
        :returns: Tuple of (dict mapping symbol to export stats, sorted list
            of symbols with at least one failed timeframe, list of
            :class:`ExportFailure` detailing each failed timeframe).
        :raises OSError: On a fatal environment condition -- see
            :meth:`export_candles`.
        """
        gmx_dir = self._make_gmx_dir(trading_mode)

        funding_symbols = set(self.list_funding_symbols())
        export_symbols = (
            sorted(s for s in symbols if s in funding_symbols)
            if symbols
            else sorted(funding_symbols)
        )

        results: dict[str, dict] = {}
        failed_symbols: list[str] = []
        failures: list[ExportFailure] = []
        for symbol in export_symbols:
            funding_files = 0
            funding_tfs = set(self.list_funding_timeframes(symbol))
            export_tfs = (
                [tf for tf in timeframes if tf in funding_tfs]
                if timeframes
                else sorted(funding_tfs)
            )
            symbol_failed = False

            for tf in export_tfs:
                try:
                    funding_df = self._read_funding_rate(symbol, tf)
                    if funding_df is None or funding_df.is_empty():
                        continue
                    ft_funding = validate_ohlcv(
                        self._transform_funding_rate(funding_df),
                        timestamp_column="date",
                        location=f"export_funding({symbol}/{tf})",
                        allow_nonpositive_prices=True,
                    )
                    self._write(
                        ft_funding,
                        gmx_dir
                        / self._get_freqtrade_filename(
                            symbol,
                            tf,
                            "feather" if output_format == "both" else output_format,
                            trading_mode,
                            quote_currency,
                            candle_type="funding_rate",
                        ),
                        output_format,
                        overwrite,
                        unsafe_overwrite,
                        allow_nonpositive_prices=True,
                    )
                    funding_files += 2 if output_format == "both" else 1
                except DATA_DEFECT_ERRORS as e:
                    if is_fatal_environment_error(e):
                        raise
                    logger.error(
                        "export_funding(%s/%s): skipping timeframe after read/write failure: %s",
                        symbol,
                        tf,
                        e,
                    )
                    failures.append(_classify_export_failure(symbol, tf, e))
                    symbol_failed = True
                    continue

            if funding_files:
                results[symbol] = {
                    "files": funding_files,
                    "candles": 0,
                    "ohlcv_files": 0,
                    "funding_files": funding_files,
                    "mark_files": 0,
                    "index_files": 0,
                }
            if symbol_failed:
                failed_symbols.append(symbol)

        return results, failed_symbols, failures
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `poetry run pytest tests/test_export_funding_survives_corrupt_parquet.py -v`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add src/gmx_historical_data/freqtrade_exporter.py tests/test_export_funding_survives_corrupt_parquet.py
git commit -m "feat(export-guard): restructure export_funding with per-timeframe taxonomy guard" \
  --trailer "Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>" \
  --trailer "Claude-Session: https://claude.ai/code/session_01EmpMDuZLkvswy5JkBZHSEk"
```

---

### Task 6: Update `export()` wrapper for the 3-tuple

**Files:**
- Modify: `src/gmx_historical_data/freqtrade_exporter.py`
- Test: `tests/test_export_survives_corrupt_parquet.py`,
  `tests/test_export_funding_survives_corrupt_parquet.py`

**Interfaces:**
- Consumes: `export_candles`, `export_funding` from Tasks 4-5 (both now
  return 3-tuples).
- Produces: `export(...) -> tuple[dict[str, dict], list[str], list[ExportFailure]]`,
  where `failures` is the sorted concatenation of both sub-calls' failures
  (sorted by `(symbol, timeframe)`).

- [ ] **Step 1: Write the failing test**

Append to `tests/test_export_survives_corrupt_parquet.py`:

```python
def test_export_wrapper_merges_failures_from_both_pipelines(tmp_path: Path):
    """export()'s failures list is the union of candle and funding
    failures, not just one pipeline's."""
    data_dir = tmp_path / "data"
    _seed_data_dir(data_dir)  # BBB corrupt candles

    output_dir = tmp_path / "output"
    exporter = FreqtradeExporter(data_dir, output_dir)
    results, failed_symbols, failures = exporter.export(
        symbols=["AAA", "BBB", "CCC"], timeframes=["1h"]
    )

    assert failed_symbols == ["BBB"]
    assert any(f.symbol == "BBB" for f in failures)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `poetry run pytest tests/test_export_survives_corrupt_parquet.py::test_export_wrapper_merges_failures_from_both_pipelines -v`
Expected: FAIL with `ValueError: too many values to unpack` (export() is
still 2-tuple at this point).

- [ ] **Step 3: Implement**

Replace the body of `export()` from `candle_results, candle_failed_symbols = self.export_candles(**candle_kwargs)` through the `return merged, failed_symbols` line with:

```python
        candle_results, candle_failed_symbols, candle_failures = self.export_candles(**candle_kwargs)
        funding_results, funding_failed_symbols, funding_failures = self.export_funding(
            symbols=symbols,
            timeframes=timeframes,
            output_format=output_format,
            trading_mode=trading_mode,
            quote_currency=quote_currency,
            overwrite=overwrite,
            unsafe_overwrite=unsafe_overwrite,
        )
        failed_symbols = sorted(set(candle_failed_symbols) | set(funding_failed_symbols))
        failures = sorted(
            [*candle_failures, *funding_failures], key=lambda f: (f.symbol, f.timeframe)
        )

        merged: dict[str, dict] = {}
        for symbol in sorted(set(candle_results) | set(funding_results)):
            c = candle_results.get(symbol, {})
            f = funding_results.get(symbol, {})
            merged[symbol] = {
                "files": c.get("files", 0) + f.get("files", 0),
                "candles": c.get("candles", 0),
                "ohlcv_files": c.get("ohlcv_files", 0),
                "funding_files": f.get("funding_files", 0),
                "mark_files": c.get("mark_files", 0),
                "index_files": c.get("index_files", 0),
            }
        return merged, failed_symbols, failures
```

Also update the `export()` method's signature return type annotation and
its docstring's `:returns:` line to match the 3-tuple, mirroring Task 4's
docstring style:

```python
    def export(
        self,
        symbols: list[str] | None = None,
        timeframes: list[str] | None = None,
        output_format: str = "feather",
        trading_mode: str = "futures",
        quote_currency: str = "USDC",
        overwrite: bool = False,
        unsafe_overwrite: bool = False,
        keep_parquet: bool = True,
    ) -> tuple[dict[str, dict], list[str], list[ExportFailure]]:
```

and change the `:returns:` docstring line to:

```
        :returns: Tuple of (merged per-symbol stats, sorted union of symbols
            that failed candle export and/or funding export and were
            skipped, sorted union of both pipelines' :class:`ExportFailure`
            lists -- see :meth:`export_candles` and :meth:`export_funding`).
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `poetry run pytest tests/test_export_survives_corrupt_parquet.py tests/test_export_funding_survives_corrupt_parquet.py -v`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add src/gmx_historical_data/freqtrade_exporter.py tests/test_export_survives_corrupt_parquet.py
git commit -m "feat(export-guard): merge candle+funding failures in export() wrapper" \
  --trailer "Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>" \
  --trailer "Claude-Session: https://claude.ai/code/session_01EmpMDuZLkvswy5JkBZHSEk"
```

---

### Task 7: Update the three CLI commands for the 3-tuple, add fatal-abort panel

**Files:**
- Modify: `src/gmx_historical_data/cli.py`
- Test: `tests/test_export_survives_corrupt_parquet.py`,
  `tests/test_export_funding_survives_corrupt_parquet.py`

**Interfaces:**
- Consumes: `export`, `export_candles`, `export_funding` (3-tuple returns
  from Tasks 4-6).
- Produces: `_format_export_failures(failures: list) -> str` module-level
  helper in `cli.py`, used by all three commands' failure panels.
- Produces: each of `export_freqtrade_command`, `export_candles_command`,
  `export_funding_command` now (a) unpacks 3-tuple, (b) catches `OSError`
  separately from the existing generic `except Exception` to print a
  distinct "Export Aborted" panel and exit 1, (c) includes each failure's
  `symbol/timeframe: reason` in the "Export Failures" panel.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_export_survives_corrupt_parquet.py`:

```python
def test_export_freqtrade_command_shows_failure_reason(tmp_path: Path):
    """The CLI's failure panel names the reason slug, not just the symbol --
    this is the whole point of the taxonomy: an operator glancing at the
    output can tell 'one bad symbol' from 'systematic bug' by reading the
    reason, not just a bare symbol list."""
    data_dir = tmp_path / "data"
    _seed_data_dir(data_dir)  # BBB corrupt (ArrowInvalid on truncated bytes)

    output_dir = tmp_path / "output"
    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "export-freqtrade",
            "--data-dir",
            str(data_dir),
            "--output-dir",
            str(output_dir),
            "--timeframe",
            "1h",
        ],
    )

    assert result.exit_code != 0
    assert "BBB" in result.output
    assert "ArrowInvalid" in result.output
```

Append to `tests/test_export_funding_survives_corrupt_parquet.py`:

```python
def test_export_funding_command_aborts_distinctly_on_enospc(tmp_path: Path, monkeypatch):
    """A fatal environment error surfaces as a distinct 'Export Aborted'
    panel, not the generic per-symbol 'Export Failures' panel -- an
    operator must be able to tell 'the disk is full' from 'one symbol's
    data is bad' at a glance."""
    data_dir = tmp_path / "data"
    _seed_funding_symbol(data_dir, "AAA", _make_funding_df())

    def _raise_enospc(*args, **kwargs):
        raise OSError(errno.ENOSPC, "No space left on device")

    monkeypatch.setattr(fe_module, "atomic_write_ipc", _raise_enospc)

    output_dir = tmp_path / "output"
    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "export-funding",
            "--data-dir",
            str(data_dir),
            "--output-dir",
            str(output_dir),
            "--timeframe",
            "1h",
        ],
    )

    assert result.exit_code != 0
    assert "Export Aborted" in result.output
    assert "Export Failures" not in result.output
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `poetry run pytest tests/test_export_survives_corrupt_parquet.py::test_export_freqtrade_command_shows_failure_reason tests/test_export_funding_survives_corrupt_parquet.py::test_export_funding_command_aborts_distinctly_on_enospc -v`
Expected: FAIL — first test fails because the reason slug isn't in the panel
text yet (only the symbol list is); second fails with
`ValueError: too many values to unpack` inside the CLI command (it still
does `results, failed_symbols = exporter.export_funding(...)`), which the
`except Exception` handler in the command currently swallows into a generic
"Export failed" message rather than "Export Aborted".

- [ ] **Step 3: Implement**

Add this helper function in `src/gmx_historical_data/cli.py`, placed just
above `export_freqtrade_command` (search for `def export_freqtrade_command`
to find the insertion point):

```python
def _format_export_failures(failures: list) -> str:
    """Render an ``ExportFailure`` list as one line per failure for a CLI panel.

    :param failures: List of ``ExportFailure`` (symbol, timeframe, reason,
        message) from an exporter call.
    :returns: Newline-joined ``"{symbol}/{timeframe}: {reason}"`` lines,
        empty string if ``failures`` is empty.
    """
    return "\n".join(f"  {f.symbol}/{f.timeframe}: {f.reason}" for f in failures)
```

In `export_freqtrade_command`, replace:

```python
    try:
        results, failed_symbols = exporter.export(
            symbols=symbols_to_export,
            timeframes=timeframes_to_export,
            output_format=output_format,
            overwrite=overwrite,
            unsafe_overwrite=unsafe_overwrite,
            keep_parquet=not delete_source,
        )
    except Exception as e:
        console.print(f"[red]Export failed: {e}[/red]")
        console.print("[red]Traceback:[/red]")
        console.print(traceback.format_exc())
        raise typer.Exit(1)
```

with:

```python
    try:
        results, failed_symbols, failures = exporter.export(
            symbols=symbols_to_export,
            timeframes=timeframes_to_export,
            output_format=output_format,
            overwrite=overwrite,
            unsafe_overwrite=unsafe_overwrite,
            keep_parquet=not delete_source,
        )
    except OSError as e:
        console.print()
        console.print(
            Panel(
                f"[red bold]Fatal environment error -- the export was stopped rather than "
                f"skipping the affected symbol.[/red bold]\n{e}",
                title="Export Aborted",
                box=box.ROUNDED,
                border_style="red",
            )
        )
        raise typer.Exit(1) from e
    except Exception as e:
        console.print(f"[red]Export failed: {e}[/red]")
        console.print("[red]Traceback:[/red]")
        console.print(traceback.format_exc())
        raise typer.Exit(1)
```

and its failure panel:

```python
    if failed_symbols:
        console.print()
        console.print(
            Panel(
                f"[red bold]{len(failed_symbols)} symbol(s) failed export and were "
                "skipped -- every other symbol still exported.[/red bold]\n"
                f"Failed: {', '.join(sorted(failed_symbols))}\n\n"
                f"{_format_export_failures(failures)}",
                title="Export Failures",
                box=box.ROUNDED,
                border_style="red",
            )
        )
        # Non-zero exit is required: the downstream cron alert keys off it.
        raise typer.Exit(1)
```

Apply the identical three edits (try/except OSError, 3-tuple unpack, panel
body) to `export_candles_command` (calling `exporter.export_candles(...)`)
and `export_funding_command` (calling `exporter.export_funding(...)`) —
same OSError panel text, same `_format_export_failures(failures)` addition
to their existing failure panels. Each of those two commands currently has
`except Exception as e: ...; raise typer.Exit(1) from e` (note the `from e`
these two already have, unlike `export_freqtrade_command`) — keep each
command's existing `from e` style when adding the new `except OSError`
clause above it.

- [ ] **Step 4: Run tests to verify they pass**

Run: `poetry run pytest tests/test_export_survives_corrupt_parquet.py tests/test_export_funding_survives_corrupt_parquet.py -v`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add src/gmx_historical_data/cli.py tests/test_export_survives_corrupt_parquet.py tests/test_export_funding_survives_corrupt_parquet.py
git commit -m "feat(export-guard): surface failure reasons and distinguish fatal aborts in CLI" \
  --trailer "Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>" \
  --trailer "Claude-Session: https://claude.ai/code/session_01EmpMDuZLkvswy5JkBZHSEk"
```

---

### Task 8: Full test suite + real-collector verification

**Files:** none modified — verification only.

**Interfaces:** none — this task consumes the finished implementation from
Tasks 1-7 and verifies it end-to-end, including against real production
data.

- [ ] **Step 1: Run the full test suite**

Run: `poetry run pytest tests/ -v 2>&1 | tail -80`
Expected: PASS count strictly greater than the baseline recorded in PR #27
(`389 passed, 6 skipped`) — record the exact new pass count for the PR
description. Investigate and fix any failure before proceeding; do not
skip or `xfail` a test to get to green.

- [ ] **Step 2: Run the export-focused subset with verbose output for the PR record**

Run: `poetry run pytest tests/ -k "export" -v`
Expected: every test listed in the design doc's testing table passes:
`test_export_funding_survives_validation_error`,
`test_export_funding_survives_parity_mismatch`,
`test_export_funding_aborts_on_enospc`,
`test_export_funding_isolates_timeframes`,
`test_export_candles_shares_taxonomy`,
`test_corrupt_parquet_errors_alias_still_importable`, plus every
pre-existing export test.

- [ ] **Step 3: Run a lint/type check if the project has one configured**

Run: `ls pyproject.toml && grep -n "ruff\|mypy" pyproject.toml | head -20`
If `ruff` is configured, run: `poetry run ruff check src/gmx_historical_data/ tests/`
If `mypy` is configured, run: `poetry run mypy src/gmx_historical_data/freqtrade_exporter.py src/gmx_historical_data/atomic_parquet.py src/gmx_historical_data/ohlcv_validation.py src/gmx_historical_data/storage.py`
Expected: no new errors introduced by this change (pre-existing unrelated
errors, if any, are out of scope — do not fix them here).

- [ ] **Step 4: Verify against the real collector — dry run on a copy first**

The design doc's own regression test replayed the actual incident file;
do the equivalent live check without touching production data. Find the
real data directory (referenced in issue #26 as
`./user_data/data/gmx` invoked from `gmx-strategies`):

```bash
find ~ -maxdepth 4 -iname "gmx" -type d 2>/dev/null
```

Copy the real data directory to a scratch location (never write to the
production dir from this verification step):

```bash
mkdir -p /tmp/export-guard-verify
cp -r <real_data_dir> /tmp/export-guard-verify/data
```

Run the real CLI command against the copy:

```bash
poetry run python -m gmx_historical_data.cli export-freqtrade \
  --data-dir /tmp/export-guard-verify/data \
  --output-dir /tmp/export-guard-verify/output
```

Expected: the command completes (exit 0 if all symbols are currently
healthy, or exit 1 with an "Export Failures" panel naming specific
symbols/timeframes/reasons if any are — either outcome is a pass for this
verification step, since the point is confirming the guard classifies and
reports correctly rather than crashing uncaught). Read the console output
in full; if it prints a raw Python traceback instead of a Rich panel, that
is a regression — stop and fix before proceeding to Task 9.

- [ ] **Step 5: Clean up the scratch verification directory**

```bash
rm -rf /tmp/export-guard-verify
```

---

### Task 9: Close issue #26 and open the PR

**Files:** none modified — process/communication only.

**Interfaces:** none.

- [ ] **Step 1: Push the branch and open a PR**

```bash
git push -u origin <branch-name>
gh pr create --title "fix(export): catch the exception type the validation guard actually raises" --body "$(cat <<'EOF'
## Why

PR #27 fixed export_candles()/export_funding()'s write-atomicity and added
a per-symbol guard catching CORRUPT_PARQUET_ERRORS. That guard never fires
for the most likely cause of a real failure: every validate_ohlcv() /
assert_export_parity() / history-preservation-guard failure raises a bare
ValueError, which is not in that tuple. A single bad symbol's ValueError
still aborts the whole export -- the exact failure mode #27 was meant to
close, just one exception type over.

Closes #26.

## Summary

[fill in from the commits made across Tasks 1-7]

## Validation

[paste the Task 8 Step 1/2 pytest output]

Replayed export-freqtrade against a copy of the real production data
directory (Task 8 Step 4) -- completes with a Rich panel (success or a
named per-symbol failure list), never a raw traceback.
EOF
)"
```

- [ ] **Step 2: Comment on and close issue #26**

Only after the PR above is merged (do not close the issue while the fix is
still unmerged):

```bash
gh issue comment 26 --body "$(cat <<'EOF'
Fixed in #<PR number>. PR #27 closed the write-atomicity half of this bug;
the remaining half was that export_candles()/export_funding()'s per-symbol
guard caught CORRUPT_PARQUET_ERRORS but every validation failure inside
that same try block raises a bare ValueError, which isn't in that tuple --
so a single bad symbol's validation failure still aborted the whole export
and skipped every alphabetically-later symbol, exactly like the original
report.

This PR adds ExportValidationError(ValueError) with a reason slug, has the
guard catch it alongside the corrupt-file exceptions (skip that
symbol/timeframe, record it, keep going), and separately classifies a
fatal OSError (disk full, read-only fs, quota, fd exhaustion) to abort the
whole run instead of misreporting it as 107 individually-corrupt symbols.
Verified against a copy of the real production data directory.
EOF
)"
gh issue close 26
```

- [ ] **Step 3: Final confirmation to the user**

Report back: PR URL, final pytest pass count, and confirmation issue #26 is
closed.

---

## Self-Review Notes (for whoever executes this plan)

- **Spec coverage:** Every component in the design's "Components" section
  (1-4) has a task: `ExportValidationError` → Task 1; `DATA_DEFECT_ERRORS`/
  `is_fatal_environment_error` → Task 3; `FreqtradeExporter` restructuring
  → Tasks 4-6; `ExportFailure` → Task 4. The design's full test table is
  covered across Tasks 4, 5, and 8. The "Error handling contract" section's
  two requirements (no partial file on skip; distinguishable panels) are
  covered by Task 7 (panels) and are otherwise already guaranteed by
  `_write_both`/`_write_single_frame`'s existing atomicity, untouched by
  this plan per the Global Constraints.
- **Explicitly out of scope, confirmed not touched by any task:** disk
  retention/87% host, otel exporter queue backpressure, `_write_both`'s
  atomicity/rollback logic.
- **Open question:** resolved as skip-and-report per the user's explicit
  confirmation before this plan was written (see Global Constraints).
