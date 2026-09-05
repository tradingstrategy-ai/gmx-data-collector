# Export guard exception taxonomy — design

- **Date:** 2026-09-05
- **Repo:** `gmx-data-collector` (standalone checkout at `~/dev/gmx-data-collector`; the
  copy under `gmx-strategies/gmx-data-collector` is the same commit as a submodule —
  work happens here, not there)
- **Baseline commit:** `5911ff6` (`Merge pull request #27 from
  tradingstrategy-ai/fix/atomic-writes-and-export-resilience`)
- **Status:** DESIGN — not approved, not implemented
- **Scope:** `FreqtradeExporter.export_funding` and `FreqtradeExporter.export_candles`
  (they share the guard), plus `atomic_parquet.CORRUPT_PARQUET_ERRORS` and
  `ohlcv_validation`.

---

## Correction to the premise

This work was carried forward as *"`export_funding()` still has unguarded feather
paths."* Reading the code at `5911ff6`, that is **no longer true** and should not be
used as the problem statement:

- `_write_single_frame` routes both formats through `atomic_write_ipc` /
  `atomic_write_parquet` (tmp → fsync → `os.replace`).
- `_write_both` does a tmp+backup+parity dance with rollback in `except` and temp
  cleanup in `finally`.
- `export_funding` already has a per-symbol `try` that records `failed_symbols`.
- The CLI already exits non-zero on `failed_symbols` so the cron alert keys off it.

PR #27 closed the write-atomicity hole. The defect below sits immediately next to it
and was not addressed: **the guard's exception tuple does not cover the exception type
this loop actually raises.**

---

## Problem

`export_funding` guards each symbol with:

```python
except CORRUPT_PARQUET_ERRORS as e:      # (ArrowInvalid, pl.exceptions.ComputeError, OSError)
    failed_symbols.append(symbol)
    continue
```

Every *validation* failure reachable inside that `try` raises **`ValueError`**, which
is not in the tuple:

| Raise site | Count | Trigger |
|---|---|---|
| `ohlcv_validation.validate_ohlcv` | 10 | missing columns, empty frame, non-monotonic/duplicate timestamps, bad OHLC relations |
| `ohlcv_validation.assert_export_parity` | 2 | Feather/Parquet frames disagree |
| `freqtrade_exporter._merge_export_frames` | 1 | history-preservation guard (the 2026-05-11 incident guard) |
| `_write_single_frame` / `_write` | 2 | unsupported format |

`export_funding` calls `validate_ohlcv(...)` directly at the top of each timeframe
iteration, and `_write` → `_write_both` → `assert_export_parity` on the way out.

### Caller-observable impact

1. `export_funding()` **raises** instead of returning `(results, failed_symbols)`.
2. The CLI's blanket `except Exception` catches it, prints `Export failed: <msg>`, and
   exits 1.
3. Symbols sorted **after** the failing one never export. They keep the previous run's
   feather and there is no record naming them.
4. The operator sees one exception line. There is no way to tell "one bad symbol,
   107 fine" from "the whole export died" — and the stale symbols are invisible.

Point 4 is the failure signature of the month-long silent candle-export incident: a
real, partial data failure that renders as a single transient-looking error. The
per-symbol guard was written precisely to prevent that, and does not fire for the most
likely cause.

### Two adjacent defects found while reading

**D2 — timeframe results discarded.** The `try` wraps the whole `for tf in export_tfs`
loop, and `results[symbol] = {...}` is the last statement inside it. A failure on the
4th timeframe throws away the recorded count for the 3 that already wrote successfully.
The files are on disk; the totals under-report them.

**D3 — `OSError` is miscategorised.** `OSError` in `CORRUPT_PARQUET_ERRORS` means
`ENOSPC` (disk full) is treated as "this symbol's parquet is corrupt, skip it". With the
host currently at 87% disk, a full disk would mark **every** symbol failed one at a
time, complete the run, and report a data-corruption story for what is an environment
failure. Worth fixing in the same change because it is the same taxonomy bug pointing
the other way: too *wide* here, too *narrow* for `ValueError`.

---

## Approaches considered

**Option 1 — add `ValueError` to `CORRUPT_PARQUET_ERRORS`.** One line. Rejected: it
collapses "this symbol's data is defective" and "our transform has a bug" and "the disk
is full" into one bucket that always means *skip and continue*. A genuine logic
regression in `_transform_funding_rate` would then silently skip all 107 symbols and
exit 1 with a tidy failure list — wrong data, confident report. That is the exact class
of failure this repo has already been burned by.

**Option 2 — catch `Exception` per symbol.** Rejected outright; hides everything.

**Option 3 (recommended) — a three-way taxonomy.** Classify by *what the operator
should do*, not by which library raised:

| Class | Contents | Behaviour |
|---|---|---|
| `DATA_DEFECT_ERRORS` | `ArrowInvalid`, `pl.exceptions.ComputeError`, `ExportValidationError`, and `OSError` *not* matching the fatal errnos below | skip this symbol/timeframe, record it, continue |
| `FATAL_ENVIRONMENT_ERRORS` | `OSError` whose `errno` ∈ {`ENOSPC`, `EROFS`, `EDQUOT`, `EMFILE`, `ENFILE`} | abort the whole run immediately, exit non-zero |
| *(anything else)* | — | propagate; unknown means do not guess |

`ExportValidationError(ValueError)` is a new exception in `ohlcv_validation`, raised by
`validate_ohlcv`, `assert_export_parity`, and the history-preservation guard. It
subclasses `ValueError`, so existing callers and tests that catch `ValueError` keep
working — no breaking change. Non-`errno`-matching `OSError` (e.g. a genuinely
unreadable file) stays a data defect.

---

## Design

### Components

1. **`ohlcv_validation.ExportValidationError(ValueError)`** — new. Carries `location`
   and a short `reason` slug (`missing_columns`, `non_monotonic`, `parity_mismatch`,
   `history_shrink`, …) so the failure list is greppable rather than prose.
2. **`atomic_parquet`** — keep `CORRUPT_PARQUET_ERRORS` as a deprecated alias for
   compatibility; add `DATA_DEFECT_ERRORS` and `is_fatal_environment_error(exc)`.
   The errno check is a function, not a tuple, because `OSError` classification depends
   on the instance.
3. **`FreqtradeExporter`** — restructure both export loops:
   - Move the `try` **inside** the timeframe loop (fixes D2).
   - Accumulate `funding_files` across surviving timeframes; always assign
     `results[symbol]` for any symbol that produced ≥1 file.
   - Re-raise on `is_fatal_environment_error` before the data-defect handler.
4. **Failure record shape** — `failed_symbols: list[str]` is kept for API
   compatibility; add a parallel `failures: list[ExportFailure]`
   (`dataclass(slots=True)` of `symbol`, `timeframe`, `reason`, `message`) returned as
   a third tuple element. Callers that unpack two values must be updated in the same
   change: `export`, `export_funding_command`, `export_candles_command`,
   `export_freqtrade_command`.

### Data flow (unchanged except at the guard)

```
funding parquet ──_read_funding_rate──► polars frame
                        │ ComputeError → DATA_DEFECT → skip (symbol, tf)
   ──_transform_funding_rate──► ──validate_ohlcv──►
                        │ ExportValidationError → DATA_DEFECT → skip (symbol, tf)
   ──_write──► _write_both ──assert_export_parity──► atomic replace
                        │ ExportValidationError → DATA_DEFECT → skip (symbol, tf)
                        │ OSError(ENOSPC)        → FATAL     → abort run
```

### Error handling contract

- A skipped `(symbol, timeframe)` never leaves a partial file — guaranteed already by
  `_write_both`'s rollback and `_write_single_frame`'s atomic replace. Unchanged.
- The run exits non-zero if `failures` is non-empty **or** it aborted fatally, but the
  two are distinguishable in output: `Export Failures` panel vs `Export Aborted` panel.
  The cron alert keys off the exit code as it does today; the Discord/SigNoz body can
  then carry the reason slug.

### Testing

Mirror the existing pattern in `tests/test_export_funding_survives_corrupt_parquet.py`
(which already builds a 3-symbol AAA/BBB/CCC fixture and asserts
`failed_symbols == ["BBB"]`). New tests, all in that style:

| Test | Asserts |
|---|---|
| `test_export_funding_survives_validation_error` | corrupt BBB's frame so `validate_ohlcv` raises → `failures` names BBB, AAA+CCC still export |
| `test_export_funding_survives_parity_mismatch` | monkeypatch `assert_export_parity` to raise for BBB only → same |
| `test_export_funding_aborts_on_enospc` | monkeypatch write to raise `OSError(errno.ENOSPC)` → raises, does **not** return a failure list |
| `test_export_funding_isolates_timeframes` | BBB fails on `8h` only → BBB's `1h` file exists and is counted (D2) |
| `test_export_candles_shares_taxonomy` | same guard behaviour on the candle path |
| `test_corrupt_parquet_errors_alias_still_importable` | back-compat |

Run: `uv run pytest tests/ -k "export"` — record the exact pass count in the PR.

---

## Explicitly out of scope

- Disk retention / the 87% host (item B) — separate work; only the `ENOSPC`
  classification touches it here.
- otel exporter queue backpressure (item C).
- Any change to `_write_both`'s atomicity, which is correct as merged in #27.

---

## Open question for review

**When `validate_ohlcv` fails, is that a data defect or a bug in our transform?**

This design answers "data defect — skip and report", which keeps the other 106 symbols
exporting. The opposite answer is defensible: our own transform produced the frame, so
a validation failure means *our code* is wrong and the run should stop loudly rather
than emit 106 good files and a warning.

Recommendation: skip-and-report, because the funding parquet source is externally
produced (650 of 1,949 raw-store files were found defective in the production dry run
noted in the `export_funding` docstring), so bad input is the likelier cause and
halting the whole export punishes 106 healthy symbols for one bad upstream file. The
reason slug makes a systematic transform bug obvious anyway — it shows up as *all*
symbols failing with the same slug, which the current design surfaces clearly.

Confirm or overrule before implementation.
