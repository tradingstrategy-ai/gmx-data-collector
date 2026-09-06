# Cadence validation for exported candle series — design

- **Date:** 2026-09-06
- **Repo:** `gmx-data-collector` (standalone checkout at `~/dev/gmx-data-collector`; the
  copy under `gmx-strategies/gmx-data-collector` is a pinned submodule mirror —
  work happens here, not there)
- **Baseline commit:** `cbdb8f3` (`fix(export): catch the exception type the validation
  guard actually raises (#28)`). **The local checkout is at `5911ff6` (#27) and is one
  commit stale** — `git pull` before implementing. Everything below assumes the
  post-#28 shape: `ExportValidationError`, `DATA_DEFECT_ERRORS`,
  `is_fatal_environment_error`, `ExportFailure`, and 3-tuple returns from
  `export_candles`/`export_funding`/`export`.
- **Status:** DESIGN — not approved, not implemented
- **Scope:** `ohlcv_validation` (new detection primitive), `FreqtradeExporter`'s
  candle write path, a new release-artifact cadence manifest, and the
  `release-data.yml` regression gate. Explicitly **not** `export_funding`'s data
  contract, and **not** any change to what candle data the collector fetches.
- **Issue:** [#29](https://github.com/tradingstrategy-ai/gmx-data-collector/issues/29)
  (P1, bug). Downstream incident:
  `tradingstrategy-ai/freqtrade-multistrategy-orchestrator#657`.

---

## Problem

`validate_ohlcv()` checks that timestamps are non-null, unique, and **strictly
increasing** (`ohlcv_validation.py`, the `non_monotonic` guard). It never checks that
the delta between consecutive candles equals the timeframe's fixed interval. A missing
interior bar yields a series that is monotonic but irregular — one step is 2x/3x the
expected delta — and passes every existing guard silently.

`assert_export_parity()` only compares two frames to each other, so re-exporting the
same hole agrees with itself forever. `_assert_history_preserved()` only checks
`earliest`/`latest`/row-count monotonicity, so a hole in the middle is invisible to it
too. The result ships in `gmx-full.tar.gz` with no warning and no way for a consumer to
distinguish "this file is complete" from "this file has a silent hole".

### Verified root cause

Read `ohlcv_validation.py` in full at `cbdb8f3`. Every raise site is one of:
`missing_columns`, `empty_frame`, `invalid_timestamps`, `duplicate_timestamps`,
`non_monotonic`, `invalid_price`, `ohlc_ordering`, `open_scale`, `invalid_volume`,
`parity_missing_columns`, `parity_mismatch`, `schema_regression`, `history_shrink`.
**None of them is a cadence/continuity check.** The issue's diagnosis is correct.

Reproduced against the shipped artifact
(`user_data/data/gmx/futures/BTC_USDC_USDC-4h-futures.feather`):

```
rows 6862  range 2023-07-20 08:00 -> 2026-09-06 00:00 UTC
cadence breaks: 1
  gap 0 days 08:00:00 between 2025-06-02 16:00:00+00:00 and 2025-06-03 00:00:00+00:00
```

### Blast radius — the fact that drives the whole design

A full sweep of the 706 shipped `*-futures.feather` files on a production host:

| tf | files | files with ≥1 break | % | total breaks |
|---|---|---|---|---|
| 1m | 117 | 111 | 94.9% | 8,157 |
| 5m | 117 | 109 | 93.2% | 315 |
| 15m | 118 | 108 | 91.5% | 131 |
| 1h | 118 | 107 | 90.7% | 109 |
| 4h | 118 | 40 | 33.9% | 40 |
| 1d | 118 | 107 | 90.7% | 109 |
| **total** | **706** | **582** | **82.4%** | **8,861** |

**82% of already-shipped files carry at least one cadence break.** Only 1,508 of the
8,861 breaks fall inside GMX's ~5-week retention window; the rest are permanently
unrecoverable — there is no re-fetch path.

Break sizes (1h/4h/1d, 258 breaks) separate into three distinct populations:

| missing bars | count | what it is |
|---|---|---|
| 1 | 114 | isolated oracle outage (the BTC 4h case) |
| 2 | 32 | short oracle outage |
| 25–72 | 109 | **fleet-wide collector outage** — 2026-07-14 alone accounts for 535 breaks across ~107 symbols at the same timestamp |
| >72 | 1 | `MEGA` 1h, 213 bars — new-listing / relisting artifact (see PR #23, which deliberately retains delisted market history) |

This is decisive: **a hard `raise` on any cadence break — the issue's first proposed
fix — would fail 82% of the export on its first run and cannot be deployed.** It would
also mis-classify legitimate relisting holes as corruption. The fix must make gaps
*visible and non-regressing*, not *fatal*.

---

## What already exists (do not rebuild)

Four separate timeframe→interval mappings already live in this repo, plus two gap
detectors:

| Location | Shape | Key format |
|---|---|---|
| `daemon/gap_detector.py:90` (`GapDetector.TIMEFRAME_DELTAS`) | `dict[str, timedelta]` | pandas — `1min`, `5min`, `15min` |
| `daemon/gap_detector.py:235` (`AdaptiveGapDetector.TIMEFRAME_DELTAS`) | identical duplicate | pandas |
| `scripts/validate_price_continuity.py:38` (`TIMEFRAME_DELTAS`) | `dict[str, pd.Timedelta]` | filename — `1m`, `5m`, `15m` |
| `cex_gap_fill/detector.py:38` (`_TF_TO_MINUTES`) + `minutes_for_timeframe()` | `dict[str, int]` | pandas — `1min`, `5min`, `15min` |

- **`scripts/validate_price_continuity.py` already computes grid gaps** (`missing_bars`,
  via span/delta arithmetic) and already has `_timeframe_from_name(path)`. It has a
  passing test, `tests/test_validate_price_continuity.py::test_grid_gap_detected`. It is
  a **standalone script that nothing in the export path calls.**
- **`cex_gap_fill/`** (PR #20) already implements `reindex_and_mark_missing()`,
  `detect_gaps()`, and a wired `fill-gaps-cex` CLI command that fills gaps from a CEX
  into the **source parquet**, before export. The issue's second proposal — "gap-fill
  from a secondary venue" — is therefore *already built*; it is simply not run as part
  of the release pipeline.
- **`.github/workflows/release-data.yml:160-215`** already restores the previous
  release's tarball, writes `/tmp/gmx-restore-manifest.json` (per-file `rows`,
  `min_date`, `max_date`), and fails the release on a regression against it. This is an
  exact precedent for the regression gate proposed below.

### Key-format trap

`storage.list_timeframes()` returns **parquet file stems**, and `storage.save_candles`
writes with `TIMEFRAME_TO_FILENAME` — so the `tf` variable inside both export loops is
always **filename format** (`1m`, `5m`, `15m`, `1h`, `4h`, `1d`). The CLI passes
`--timeframe` through unfiltered and intersects it with `list_timeframes()`, so it
cannot introduce a pandas-format key either.

`cex_gap_fill.minutes_for_timeframe("5m")` therefore raises `ValueError: unknown
timeframe: '5m'` — it is **not** reusable at the export call site as-is. Any new helper
must accept the filename format, and must not become a fifth divergent mapping.

---

## Why funding frames are excluded

`export_funding()` calls `validate_ohlcv(..., allow_nonpositive_prices=True)`. Three
independent reasons a cadence check must not apply there:

1. **`_transform_funding_rate()` ends with `df.drop_nulls(subset=["open"])`.** A null
   funding rate is dropped by design, so holes in a funding series are a deliberate
   product of the transform, not a defect.
2. **Funding timeframes are free-form.** `_FUNDING_TIMEFRAME_PATTERN = re.compile(r"^\d+[mhd]$")`
   admits `8h`, `2h`, `12h` — anything matching the pattern. A fixed six-key mapping
   would `KeyError`, and the repo's own tests already exercise an `8h` funding frame
   (`tests/test_export_funding_survives_corrupt_parquet.py`).
3. **Downstream consumes funding as a step function**, via as-of join on the last known
   rate, not on a fixed grid — a missing funding row does not produce the class of
   failure #657 describes.

The design therefore threads cadence checking as an **opt-in** parameter that only the
candle path passes. `export_funding` is untouched.

---

## Approaches considered

**Option 1 — hard `raise` on any cadence break inside `validate_ohlcv`.**
This is the issue's first proposal, read literally. **Rejected on measurement:** 582 of
706 files break immediately, the underlying data is unrecoverable past GMX's 5-week
retention, and legitimate relisting holes (PR #23) would be permanently indistinguishable
from corruption. It converts a silent-data bug into a total release outage.

**Option 2 — gap-fill from a CEX before packaging, then hard-fail on what's left.**
The machinery exists (`fill-gaps-cex`). **Rejected as the primary fix, kept as a
follow-up:** it mutates published price history for 582 files in one shot, it cannot
help the ~83% of breaks already outside the retention window where no CEX bar maps
cleanly onto a stale GMX oracle print, and it needs its own data-quality review. It is
remediation, not detection, and shipping it as the fix for #29 would conflate the two.

**Option 3 — manifest only, no gate.** Emit a "known gaps" file beside the release and
stop. Closes the *visibility* half of the issue. **Rejected as insufficient:** nothing
then prevents the *next* silent gap, which is what actually broke the vault sleeve.

**Option 4 (recommended) — detect always, publish a manifest, gate on regressions.**
Compute cadence breaks on every exported candle frame; publish them as a
machine-readable manifest inside `gmx-full.tar.gz`; fail the release only when a run
introduces a break in a region that was previously contiguous. Pre-existing breaks are
recorded and pass. This makes every gap visible to downstream consumers immediately,
makes a *new* silent gap impossible, and does not wedge the fleet.

---

## Recommended design

Three layers, each independently testable.

### Layer 1 — detection primitive (pure, `ohlcv_validation.py`)

```python
@dataclass(frozen=True, slots=True)
class CadenceBreak:
    before: datetime      # last good bar
    after: datetime       # next present bar
    actual: timedelta     # after - before
    missing_bars: int     # actual // expected - 1

def parse_timeframe_interval(timeframe: str) -> timedelta
def find_cadence_breaks(frame, *, timestamp_column, expected_interval) -> list[CadenceBreak]
```

`parse_timeframe_interval` accepts **both** key formats (`1m` and `1min`, `4h`, `1d`)
by normalising through `config.FILENAME_TO_TIMEFRAME` and falling back to a regex parse
of `^(\d+)(min|m|h|d)$`, so an arbitrary funding-style `8h` resolves too and no fifth
hardcoded table is added. It raises `ValueError` on an unknown token.

`find_cadence_breaks` is a pure function returning a list — it never raises on a
finding. Everything above it decides severity. It assumes the frame is already sorted
and duplicate-free, which `validate_ohlcv`'s existing guards have established by the
time it runs.

### Layer 2 — opt-in hook in `validate_ohlcv`

```python
def validate_ohlcv(
    frame, *, timestamp_column, location,
    allow_nonpositive_prices: bool = False,
    expected_interval: timedelta | None = None,      # NEW
    cadence_policy: Literal["ignore", "raise"] = "ignore",   # NEW
) -> pl.DataFrame
```

`expected_interval=None` (the default) is a complete no-op. This matters: `validate_ohlcv`
has **five** call sites (`export_candles` ×2 for candles and mark, `export_funding`,
`_read_existing_export_frame`, `_merge_export_frames`) plus callers in `storage.py` and
`scripts/validate_price_continuity.py`, and `tests/test_ohlcv_validation.py` has 10
existing tests. Defaulting to off means none of them change behaviour.

When `cadence_policy="raise"` and breaks are found, it raises
`ExportValidationError(location, "cadence_break", …)`. This slots into #28's existing
taxonomy with **zero new plumbing**: `ExportValidationError` is already in
`DATA_DEFECT_ERRORS`, so the export guard already catches it, already skips that
`(symbol, timeframe)`, already records an `ExportFailure` with `reason="cadence_break"`,
already keeps the other 106 symbols exporting, and already exits non-zero. No new
exception class, no new tuple, no CLI arity change.

`"raise"` is *not* used by the default export path (see Layer 3). It exists so the
regression gate and `validate_price_continuity.py` can opt into strictness, and so the
behaviour is available without a second code path.

### Layer 3 — export-time recording + manifest + regression gate

**Where the check runs.** On the **merged** frame, not the incoming one. `_write` →
`_prepare_export_frame` → `_merge_export_frames` produces the frame that actually
becomes the shipped bytes; a check on `ft_df` alone would miss a hole at the seam
between existing history and the incoming slice. `_merge_export_frames` knows only
`path`, not `tf`, so `expected_interval` is threaded down through `_write` alongside
`allow_nonpositive_prices` — the same parameter already travels that exact route, so
this follows an established signature precedent rather than inventing one.

`_read_existing_export_frame` is deliberately **not** given the interval: validating the
destination's pre-existing gaps would fail before the escape hatch and re-wedge the
export.

**Severity: record, don't raise.** `export_candles` passes `expected_interval` with
`cadence_policy="ignore"` and collects the returned breaks into a per-file record. A
break is a logged `WARNING` and a manifest entry, not an `ExportFailure`. The 82%
measurement is the whole justification.

**The manifest.** `export_candles` writes
`user_data/data/gmx/futures/_cadence_manifest.json`, which lands inside
`gmx-full.tar.gz` (the workflow tars `user_data/data/gmx/` wholesale, so no packaging
change is needed):

```json
{
  "generated_at": "2026-09-06T12:00:00+00:00",
  "files": {
    "BTC_USDC_USDC-4h-futures.feather": {
      "timeframe": "4h",
      "expected_interval_seconds": 14400,
      "rows": 6862,
      "first": "2023-07-20T08:00:00+00:00",
      "last":  "2026-09-06T00:00:00+00:00",
      "breaks_total": 1,
      "missing_bars_total": 1,
      "breaks": [
        {"before": "2025-06-02T16:00:00+00:00",
         "after":  "2025-06-03T00:00:00+00:00",
         "missing_bars": 1}
      ]
    }
  }
}
```

This is the issue's "machine-readable known-gaps manifest", and it is the piece that
directly answers *"there is no way for a downstream consumer to distinguish 'this file
is complete' from 'this file has a silent hole'"*. A consumer reads one file instead of
re-deriving the check per pair — exactly the duplication that caused #657.

Files whose breaks exceed a cap (`_MAX_BREAKS_PER_FILE = 200`) record
`breaks_total`/`missing_bars_total` with a truncated `breaks` list and
`"truncated": true`, so the 1m files (8,157 breaks across 111 files) cannot bloat the
manifest.

**The regression gate** (`release-data.yml`). Mirroring the existing restore-manifest
step at lines 160–215: after export, compare the new manifest against the previous
release's. Fail the release when a file gains a break whose `before` timestamp falls
inside a span that was contiguous in the previous manifest. A break at a timestamp
already listed, or in a region the previous release did not cover, passes. This is what
makes a *new* silent gap impossible while leaving the 8,861 inherited ones alone.

---

## Data flow

```
candle parquet ──_transform_dataframe──► ──validate_ohlcv(expected_interval=None)──►
   ──_write(expected_interval=Δ)──► _prepare_export_frame ──► _merge_export_frames
                                        │
                                        ├─ validate_ohlcv(expected_interval=Δ,
                                        │                 cadence_policy="ignore")
                                        │        └─► list[CadenceBreak] ──► manifest entry + WARNING log
                                        └─ atomic replace ──► *-futures.feather
                                                                    │
                            _cadence_manifest.json ◄────────────────┘
                                        │
                    release-data.yml regression gate: new break in a
                    previously-contiguous span ⇒ fail the release
```

---

## Explicitly out of scope

- **Changing what the collector fetches.** The 2026-07-14 fleet-wide outage and the
  2025-06-02 oracle outage are real upstream events; this work makes them visible, it
  does not prevent them.
- **`export_funding`** — see "Why funding frames are excluded".
- **Wiring `fill-gaps-cex` into the release pipeline** — remediation, tracked as open
  question 1 below.
- **Consolidating the four existing `TIMEFRAME_DELTAS` tables.** `parse_timeframe_interval`
  is written so it *can* absorb them later; migrating four call sites is a separate,
  purely-mechanical PR and mixing it in would make this one unreviewable.
- Any change to `_write_both`'s atomicity or `_assert_history_preserved`.

---

## Open questions for review

**1. Manifest-only, or also wire `fill-gaps-cex` into the nightly release?**
Recommendation: **manifest-only for this change.** Gap-filling mutates published price
history across 582 files, only ~17% of breaks are still inside the retention window
where a fill is even meaningful, and it deserves its own before/after review. File it as
a follow-up issue once the manifest tells us precisely which gaps are fillable.
*Needs a decision — it changes the scope of this PR.*

**2. Should funding frames get a cadence check at all?**
Recommendation: **no**, for the three reasons above. If the answer is "record-only for
visibility", it is a small additive change (pass `expected_interval` in `export_funding`
too, manifest-only, never raise) — but it will report large volumes of legitimate
`drop_nulls` holes and I expect it to be noise. *Confirm or overrule.*

**3. Regression gate severity: fail the release, or warn and alert?**
Recommendation: **fail**, matching the existing candle-history-integrity step it sits
beside. The counter-argument is that a fleet-wide upstream outage (2026-07-14 style)
would then block the nightly release for a cause the collector cannot fix. A middle
option is to fail only when *fewer than N symbols* are affected — an isolated break is a
data defect, a correlated fleet-wide break is an outage to be recorded and alerted on.
*Needs a decision; the plan implements plain "fail" unless overruled.*

**4. Should the manifest classify break kinds?**
The data supports three distinguishable classes (isolated oracle outage, fleet-wide
correlated outage, listing/relisting hole). Classification needs a cross-symbol view,
which `export_candles` has but the per-file check does not. Recommendation: **ship the
raw manifest first**, add classification once downstream tells us which distinction they
actually need. *Low stakes — deferring is safe.*
