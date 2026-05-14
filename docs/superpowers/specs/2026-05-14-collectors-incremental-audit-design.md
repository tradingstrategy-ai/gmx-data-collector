# Collectors Incremental Audit — Design

**Status**: Draft (brainstorming output, awaiting plan)
**Date**: 2026-05-14
**Author**: Saikat K (with Claude Sonnet 4.6 / Opus 4.7)
**Related**: [docs/incremental-collection.md](../../incremental-collection.md) (existing CLI-side incremental docs), PR #15 (data_report.txt API-vs-Combined coverage), PR #16 (funding schema fix).

## Problem

Several GMX data collectors do not consult on-disk state before fetching. When the daily snapshot is re-run for the same date — or after a partial failure earlier in the day — markets, tickers, APY, volumes, and a portion of the OHLCV fetches all re-issue API calls whose results are already on disk. This wastes network time, GMX API quota, and CI minutes, and risks rate limits during back-to-back runs.

This spec defines a **coverage gate**: a small shared helper that each collector consults before fetching. If the on-disk data is already current, the collector skips the fetch entirely.

The CLI-side OHLCV collector (`gmx_historical_data.cli collect --update`) already has this via `FetchBoundaryCalculator` + `AdaptiveGapDetector`. Funding / Open Interest / Pool Liquidity have block-checkpoint `--resume` support. Both are out of scope and left untouched.

## Current state (inventoried 2026-05-14)

| Collector | Incremental today? | Mechanism |
|---|---|---|
| OHLCV (`cli collect`) | ✓ Best-in-class | `FetchBoundaryCalculator` |
| Funding (unified) | ✓ Block checkpoint | `--resume` |
| Open Interest | ✓ Block checkpoint | `--resume` |
| Pool Liquidity | ✓ Block checkpoint | `--resume` |
| Daily snapshot OHLCV | ◐ Partial | `if feather.exists()` → smaller `limit`, still fetches |
| Daily snapshot markets | ✗ None | Overwrites `snapshots/{date}.parquet` every run |
| Daily snapshot tickers | ✗ None | Overwrites `tickers/{date}.parquet` |
| Daily snapshot APY | ✗ None | Overwrites `apy/{date}.parquet` |
| Daily snapshot volumes | ✗ None | Overwrites `volumes/{date}.parquet` (when enabled) |
| Chainlink RPC fallback | ✗ None | Always fresh (used only as fallback) |

## Scope

In scope:
- `scripts/collect_daily_snapshot.py` — all five phases (markets, OHLCV, tickers, APY, volumes when re-enabled).
- New module `src/gmx_historical_data/coverage_gate.py`.
- Report (`data_report.txt`) integration so skips are visible.
- CLI flag `--force-refresh` to bypass the gate.

Out of scope:
- Funding / OI / Pool Liquidity (`--resume` already handles this).
- Workflow file changes (`release-data.yml`).
- Chainlink RPC fallback (only invoked when HyperSync fails — a fallback path; not the bottleneck).
- Consolidating duplicate quickstart code paths.

## Design decisions

| Question | Decision |
|---|---|
| Skip rule | Skip when data fully present and current (existence + min row count + OHLCV freshness check). |
| Granularity | Per `(data type, key)` tuple. Per `(symbol, tf)` for OHLCV, per date for daily-stamped. |
| Currency check | `file exists AND non-empty AND rows ≥ expected_min_rows` (daily-stamped). `max(date) ≥ expected_last_bar` (OHLCV). |
| Override | `--force-refresh` CLI flag, no env var. |
| Pattern | Shared module `coverage_gate.py`, called inline at each fetch site. No config file, no decorator. |
| OHLCV freshness | Upgrade: check `max(date) ≥ _expected_last_bar(tf, today)` before any API call. |
| Reporting | Add `## Skipped (already current)` section to `data_report.txt`. |
| Block-side collectors | Untouched — their `--resume` mechanism is correct for blockchain pagination. |

## Architecture

### `src/gmx_historical_data/coverage_gate.py`

```python
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import pandas as pd


@dataclass(frozen=True)
class SkipDecision:
    """Outcome of a coverage check at one collector site.

    :ivar skip: ``True`` when the collector should bypass its fetch.
    :ivar reason: One of ``"missing"``, ``"too_small"``, ``"stale"``,
        ``"forced"``, ``"current"``.
    :ivar existing_rows: Row count of the on-disk file (``0`` when missing).
    :ivar expected_min_rows: The threshold ``is_current`` was asked to enforce.
    """

    skip: bool
    reason: str
    existing_rows: int
    expected_min_rows: int


def is_current(
    path: Path,
    expected_min_rows: int,
    *,
    force: bool = False,
    fmt: Literal["parquet", "feather"] = "parquet",
) -> SkipDecision:
    """Decide whether ``path`` already covers the work we are about to do.

    For daily-stamped files (markets, tickers, APY, volumes).

    :param path: Target output file.
    :param expected_min_rows: Floor — files with fewer rows are treated as
        incomplete and re-fetched.
    :param force: When ``True``, always returns ``skip=False`` with
        ``reason='forced'``.
    :param fmt: ``'parquet'`` (default) or ``'feather'``.
    """


def has_ohlcv_through(
    feather_path: Path,
    expected_max_date: pd.Timestamp,
    *,
    force: bool = False,
) -> SkipDecision:
    """Decide whether an OHLCV feather already extends through the latest
    expected bar.

    :param feather_path: Per-symbol feather (e.g. ``BTC_USDC_USDC-1h-futures.feather``).
    :param expected_max_date: Latest fully-closed bar for the timeframe.
    :param force: When ``True``, always returns ``skip=False`` with
        ``reason='forced'``.
    """
```

Two functions because the rules differ. `is_current` reads parquet metadata only (cheap row count). `has_ohlcv_through` reads only the `date` column.

Both return `SkipDecision` (not a bare bool) so callers can record *why* a fetch ran — and the report can show it.

### Decision matrix

| Scenario | `is_current` | `has_ohlcv_through` |
|---|---|---|
| File missing | `skip=False, reason="missing", existing_rows=0` | `skip=False, reason="missing", existing_rows=0` |
| File corrupt (`Exception` on read) | `skip=False, reason="missing", existing_rows=0`, warning logged | `skip=False, reason="missing", existing_rows=0` |
| Rows < `expected_min_rows` | `skip=False, reason="too_small"` | n/a |
| `max(date) < expected_max_date` | n/a | `skip=False, reason="stale"` |
| Pass all checks | `skip=True, reason="current"` | `skip=True, reason="current"` |
| `force=True` | `skip=False, reason="forced"` | `skip=False, reason="forced"` |

`existing_rows` is always `0` for both the genuinely-missing and the corrupt-file cases — a corrupt file on disk may still have non-zero bytes, but the row count we could not determine is reported as `0`. Callers should not interpret `existing_rows=0` as proof the file is absent.

**Row-count implementation**: `is_current` reads parquet metadata with `pyarrow.parquet.read_metadata(path).num_rows` — this opens only the footer (~few KB), never the row groups. For feathers, `has_ohlcv_through` reads just the `date` column via `pl.read_ipc(path, columns=["date"])`. Both calls are wrapped in `try/except Exception`; failure routes to the `"missing"` branch.

## Per-collector integration

All changes in `scripts/collect_daily_snapshot.py`. New CLI flag at the bottom of `main()`'s argparse:

```python
parser.add_argument(
    "--force-refresh",
    action="store_true",
    help="Ignore the coverage gate and re-fetch all data types.",
)
```

A `skipped: dict[str, SkipDecision]` accumulates results.

### Phase 1 — Markets snapshot

Important caveat: `_fetch_all_markets(api)` is called unconditionally because its result (`all_markets`, the raw API dict list) feeds both the markets snapshot AND `_extract_symbols(all_markets)` used by Phase 2 OHLCV. Gating the markets API call would require either reconstructing API-shape dicts from the parquet (key renaming camelCase ↔ snake_case) or refactoring `_extract_symbols` to accept either shape. Neither is worth the complexity for one cheap API call.

What the gate actually skips for markets: the `collect_markets_snapshot` flatten + `markets_df.to_parquet` write. Negligible time, but kept for consistency with the other phases and for the `## Skipped` report line.

```python
markets_path = snapshots_dir / f"{date_str}.parquet"
markets_decision = is_current(markets_path, expected_min_rows=100, force=args.force_refresh)
if markets_decision.skip:
    skipped["markets"] = markets_decision
    # Read existing parquet for downstream report use; column shape already
    # matches what generate_report expects (is_swap_only, open_interest_long,
    # open_interest_short, market_token, name).
    markets_df = pd.read_parquet(markets_path)
else:
    markets_df = collect_markets_snapshot(all_markets, date_str)
    markets_df.to_parquet(markets_path, index=False)
```

`expected_min_rows=100`: floor allows for shrinkage from today's 135.

### Phase 2 — OHLCV per `(symbol, tf)`

`collect_and_save_ohlcv` gains a `force_refresh` param. Inside the loop:

```python
expected_last = _expected_last_bar(tf, target_date=date_str)
decision = has_ohlcv_through(filepath, expected_last, force=force_refresh)
if decision.skip:
    # Single feather read shared between pre_merge and post_merge — no
    # write happened, so they are identical by construction.
    stats = _feather_date_stats(filepath)
    coverage[(symbol, tf)] = {
        "pre_merge": stats,
        "api_slice": None,
        "post_merge": stats,
        "status": "SKIPPED",
    }
    continue
```

New helper `_expected_last_bar(tf: str, target_date: str, *, now: pd.Timestamp | None = None) -> pd.Timestamp`:

```python
def _expected_last_bar(
    tf: str,
    target_date: str,
    *,
    now: pd.Timestamp | None = None,
) -> pd.Timestamp:
    """Latest fully-closed bar we expect on disk for ``target_date``.

    For *today's* date, returns the most recent closed bar of the timeframe
    (e.g. for ``1h`` at 17:42 UTC → ``today 17:00``).  For *past* dates
    (backfill via ``--date``), returns the last bar of that calendar day
    (e.g. for ``1h`` and ``--date 2026-03-10`` → ``2026-03-10 23:00``).

    :param tf: Timeframe (``1m``, ``5m``, ``15m``, ``1h``, ``4h``, ``1d``).
    :param target_date: ISO date string (``YYYY-MM-DD``).
    :param now: Override for the current UTC wall-clock; defaults to
        ``pd.Timestamp.utcnow()``.
    """
    target = pd.Timestamp(target_date, tz="UTC").normalize()
    now = now if now is not None else pd.Timestamp.now(tz="UTC")
    if target.date() < now.date():
        # Past-date backfill: clamp to end-of-day for that date.
        anchor = target + pd.Timedelta(hours=23, minutes=59)
    else:
        anchor = now
    return anchor.floor({"1m": "1min", "5m": "5min", "15m": "15min",
                         "1h": "1h", "4h": "4h", "1d": "1D"}[tf])
```

A `SKIPPED` entry's `pre_merge == post_merge` and the existing release-workflow integrity check (`release-data.yml:170-181`) sees identical `before`/`after` stats — no false regression.

### Phase 4 — Tickers

```python
ticker_path = tickers_dir / f"{date_str}.parquet"
decision = is_current(ticker_path, expected_min_rows=100, force=args.force_refresh)
if decision.skip:
    skipped["tickers"] = decision
    ticker_count = _row_count(ticker_path)
else:
    ticker_count = collect_and_save_tickers(api, date_str, tickers_dir)
```

### Phase 5 — APY

```python
apy_path = apy_dir / f"{date_str}.parquet"
decision = is_current(apy_path, expected_min_rows=7 * 100, force=args.force_refresh)
if decision.skip:
    skipped["apy"] = decision
    apy_count = _row_count(apy_path)
else:
    apy_count = collect_and_save_apy(api, date_str, apy_dir)
```

`7 * 100`: 7 APY periods × ≥100 markets. APY also emits GLV rows (`_flatten_apy` populates both `markets` and `glvs` keys), so the actual row count is `7 * (markets + glvs) ≈ 7 * 135 ≈ 945`. The `700` floor is intentionally conservative — it allows for shrinkage and accepts a tiny risk of skipping a partially-collected file where only 1 of 7 periods succeeded.

### Volumes (when re-enabled)

Same pattern as tickers.

### New coverage status

The OHLCV coverage map already supports `OK / NEW / FLAT / REGRESSION / FAILED`. Add **`SKIPPED`** for entries where the gate fired. The report's existing `## OHLCV Coverage — {tf}` table renders `SKIPPED` with API columns shown as `n/a` and Combined columns showing the unchanged pre-merge values.

## Report integration

In `generate_report`, two additions:

```python
def generate_report(
    ...,
    ohlcv_coverage: dict[tuple[str, str], dict] | None = None,
    skipped: dict[str, SkipDecision] | None = None,
) -> None: ...
```

New section, emitted only when any skips occurred (either daily-stamped or OHLCV):

```
## Skipped (already current)
- Markets snapshots: existing 135 rows ≥ 100 required (reason: current)
- Tickers: existing 126 rows ≥ 100 required (reason: current)
- APY: existing 945 rows ≥ 700 required (reason: current)
- OHLCV 1h: 87/114 symbols skipped (existing max ≥ expected last bar)
- OHLCV 1d: 114/114 symbols skipped
```

Daily-stamped lines come from `skipped`. OHLCV lines aggregate `ohlcv_coverage` entries with `status == "SKIPPED"`.

## Edge cases

1. **Same-day rerun**: today's files already complete → all skip. Main intended win.
2. **Mid-day rerun, OHLCV partially current**: feather max date at 14:00, current hour 17:00 → `expected_last_bar("1h")` = 16:00 → gate says stale → fetch.
3. **Schema drift on existing file**: not the gate's concern. Handled by `_write`'s schema-tolerant merge (PR #16).
4. **Empty file with header only**: `existing_rows=0 < expected_min_rows` → fetch.
5. **Symbol exists in feather but not in today's markets**: loop iterates today's markets, so dropped symbols don't get gate-checked. Untouched.
6. **New listing today**: no feather → `missing` → full fetch (`limit=10000`).
7. **`--force-refresh`**: every gate returns `skip=False, reason="forced"`. Report omits `## Skipped`.
8. **`--date <past>` backfill**: gate evaluates that date's file; `_expected_last_bar` clamps to that date's end-of-day. Works.
9. **Corrupt parquet**: caught by `try/except` around the metadata read; treated as missing; warning logged; fetch path overwrites.
10. **Completely empty `--output-dir`** (first run): every gate returns `reason="missing"` → fetch everything. Report omits `## Skipped` entirely (the section only emits when at least one skip occurred).
11. **`--quickstart` then collect**: quickstart copies the previous release tarball into `--output-dir` first. The gate then sees the seeded files and — correctly — skips fetches for any data type whose date matches today's. If you want to force a fresh fetch after a quickstart seed, pass `--force-refresh` explicitly. The two flags compose without surprises.
12. **Markets API call is not gated**: `_fetch_all_markets(api)` runs unconditionally because `_extract_symbols(all_markets)` needs the raw API shape for Phase 2 OHLCV. The Phase 1 gate only skips the flatten + parquet write. Documented under Phase 1 above.

## Performance impact

| Phase | Today | After change (cold) | After change (skip-day) |
|---|---|---|---|
| Markets | 1 API call | 1 + ~1ms metadata read | 0 calls |
| OHLCV | 684 fetches | 684 + 684 × ~2ms feather metadata | 0 |
| Tickers | 1 | 1 + ~1ms | 0 |
| APY | 7 | 7 + ~1ms | 0 |

Same-day rerun: ~3 seconds (gate reads only) vs ~3 minutes today. Cold-path overhead: ~1–2 seconds total of metadata reads. Acceptable trade.

## Testing

Unit tests in `tests/test_coverage_gate.py`:

| Test | Setup | Expect |
|---|---|---|
| `test_missing_file` | path doesn't exist | `skip=False, reason="missing"` |
| `test_corrupt_file` | invalid bytes at path | `skip=False, reason="missing"`, warning logged |
| `test_too_small` | 5 rows, min=100 | `skip=False, reason="too_small"` |
| `test_exactly_min` | 100 rows, min=100 | `skip=True` |
| `test_current` | 135 rows, min=100 | `skip=True, reason="current"` |
| `test_forced` | 135 rows, min=100, force=True | `skip=False, reason="forced"` |
| `test_ohlcv_stale` | feather max=12:00, expected=16:00 | `skip=False, reason="stale"` |
| `test_ohlcv_current` | feather max=16:00, expected=16:00 | `skip=True` |
| `test_ohlcv_ahead` | feather max=18:00, expected=16:00 | `skip=True` |

Integration test in `tests/test_daily_snapshot_gate.py`:

- Seed `/tmp/test_data/data/gmx/snapshots/2026-05-14.parquet` with 135 rows.
- Seed `/tmp/test_data/data/gmx/tickers/2026-05-14.parquet` and `apy/2026-05-14.parquet` similarly so all daily-stamped phases skip.
- Run `main()` with `--date 2026-05-14` against a mocked GMX API where `get_tickers()` and `get_apy()` raise (proving they aren't called).
- Assert: `markets_path` mtime unchanged after the run; report contains the `## Skipped` section listing all three.
- Note: `get_markets_info()` is **not** mocked-to-raise because the API call runs unconditionally (see Phase 1 caveat). The assertion targets the parquet write, not the API call.

## Backwards compatibility

- `generate_report` gains an optional `skipped=None` param appended **after** `ohlcv_coverage` so positional callers keep working. Existing call sites (notebooks, tests) keep working.
- `SkipDecision` lives only in `coverage_gate.py`; imports are scoped.
- No on-disk format changes — schemas of `snapshots/{date}.parquet` etc. are untouched.
- `--force-refresh` flag defaults to off → existing CI cron `release-data.yml` behavior unchanged.
- `## Failed OHLCV Fetches` heading preserved (CI grep dependency at `release-data.yml:184`).
- `_abort_on_failed_ohlcv_fetches` runs after `generate_report` and only inspects `failed_symbols`. A `SKIPPED` entry never makes it into `failed_symbols`, so this path is unchanged.

## Files touched

| File | Change |
|---|---|
| `src/gmx_historical_data/coverage_gate.py` | New module |
| `scripts/collect_daily_snapshot.py` | Gate calls in 4 phases, new CLI flag, `coverage` map gets `SKIPPED` status, `generate_report` skipped section |
| `tests/test_coverage_gate.py` | New unit tests |
| `tests/test_daily_snapshot_gate.py` | New integration test |

No workflow files, no `freqtrade_exporter.py`, no `release-data.yml` changes.

## Open questions

None as of approval — all eight clarifying questions resolved during brainstorming.

## Risks / reversibility

- Pure-additive logic at each call site. Easy revert via `git revert`.
- Gate is read-only of existing files. Worst-case failure mode is the gate incorrectly skipping when it shouldn't — operator can recover with `--force-refresh`.
- No data format change, no schema migration.
- Tests cover all 9 edge cases above.
