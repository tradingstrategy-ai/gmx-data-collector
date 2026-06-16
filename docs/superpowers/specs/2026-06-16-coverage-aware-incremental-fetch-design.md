# Coverage-aware incremental fetch + `--force` rewrite

**Date:** 2026-06-16
**Status:** Approved (design)
**Author:** pair (user + Claude)

## Problem

`collect --update` (incremental) is slow because the two **historical backfill**
paths re-walk each price feed from genesis on every run, then rely on
merge-deduplication to discard rounds already on disk. The result is correct but
wastes minutes-to-hours per symbol re-downloading data we already own.

Confirmed root causes in code:

- `gap_analyzer.py:~105` — the Chainlink backfill-range helper returns
  `(None, our_earliest_unix - 1)`. A `None` start means "from genesis".
- `fetch_boundary_calculator.py:231,248` — incremental branches (`NORMAL_GAP`,
  `DATA_LOSS_GAP`) hardcode `chainlink_start_timestamp=None`.
- `fetch_boundary_calculator.py:260-264` — `NO_EXISTING_DATA` / `API_UNAVAILABLE`
  falls back to `_calculate_full_boundaries`, which also sets
  `chainlink_start_timestamp=None` (genesis). A symbol that *does* have stored
  candles can still land here and trigger a full re-walk.
- `cli.py:710` — oracle/HyperSync path: `start = start_block or GMX_V2_GENESIS_BLOCK`,
  i.e. genesis when no explicit start is supplied. Same waste for non-Chainlink symbols.

The GMX-API candle fetch is already incremental (`gap_result.fetch_start =
our_latest + interval`) and is **not** part of the problem.

## Goals

1. **Check existing coverage before fetching.** Never re-download a range we
   already store.
2. **Append by default.** Default writes merge into existing files
   (`save_candles(overwrite=False)`, already the storage default) with the
   existing `_assert_history_preserved` guard intact.
3. **`--force` = rewrite.** A single explicit flag re-fetches full ranges
   (genesis) **and** overwrites files (`overwrite=True`), bypassing coverage
   checks and the per-symbol checkpoint skip.
4. Apply the check-before-fetch principle to **all three** fetch paths:
   GMX-API (verify only — already correct), Chainlink RPC, oracle/HyperSync.

## Non-goals

- No change to the merge/dedup algorithm or on-disk schema.
- No change to GMX-API incremental logic beyond verification.
- No new storage component — reuse `AdaptiveGapDetector` / `gap_analyzer`
  existing-data inspection.

## Design

### Principle

```
inspect stored coverage  →  fetch only the missing slice  →  append (merge)
--force                  →  skip inspection, full range   →  overwrite
```

### Component changes

1. **`gap_analyzer.py` — bounded backfill range**
   - The backfill-range helper must return a **bounded start**, never `None`, on
     the incremental path:
     - If stored data already reaches the feed floor (feed genesis or a
       configured earliest), return a "skip backfill" signal (e.g. `needed=False`).
     - If a genuine hole exists older than `our_earliest`, return
       `(hole_start, our_earliest - 1)` where `hole_start` is the feed floor or
       the known boundary — bounded, not genesis-by-default.
   - Fix the misclassification so a symbol with existing stored candles is not
     routed to `NO_EXISTING_DATA`.

2. **`fetch_boundary_calculator.py` — use bounded values**
   - `NORMAL_GAP` and `DATA_LOSS_GAP`: set `chainlink_start_timestamp` and
     `oracle_start_block` from the bounded helper output instead of `None`.
   - `NO_GAP`: unchanged (already skips).
   - `NO_EXISTING_DATA`: only fall back to full when there really is no stored
     data; otherwise treat as incremental.
   - GMX-API branch: unchanged (verified incremental).

3. **`cli.py` — thread `force` through the collect path**
   - `force=False` (default): coverage-aware ranges; `save_candles(overwrite=False)`.
   - `force=True`: full ranges (genesis); `save_candles(overwrite=True)`; also
     bypass the per-symbol checkpoint skip (its current meaning) so `--force`
     means a clean redo end-to-end.
   - Oracle path: pass a resume `start_block` derived from coverage when not
     forced; keep `GMX_V2_GENESIS_BLOCK` only under `--force`.

4. **`storage.py` — pass-through**
   - Already supports `overwrite`. Thread the flag from `cli.py`; no logic change.

### Data flow

| Invocation | Coverage check | Fetch range | Write mode |
|---|---|---|---|
| `collect --update` (default) | yes | gap only | merge / append |
| `collect --update --force` | no | genesis → now | overwrite |
| `collect --full` | n/a | genesis → now | merge (overwrite only if `--force`) |

### Error handling / safety

- Default (append) path keeps `_assert_history_preserved` as the backstop
  against accidental history loss.
- `--force` + `overwrite=True` intentionally bypasses that guard — gated behind
  the explicit flag only, never the default.
- If coverage inspection errors or returns empty → fall back to full fetch
  (safe, matches today's behavior).

## Testing (TDD — written before implementation)

- **`gap_analyzer`**
  - stored data already covers the historical range → backfill `needed=False`.
  - partial hole older than `our_earliest` → bounded `(hole_start, our_earliest-1)`.
  - incremental never returns a `None` start.
- **`fetch_boundary_calculator`**
  - incremental yields tight chainlink/oracle ranges from coverage.
  - a symbol with existing candles is not classified `NO_EXISTING_DATA`.
- **`cli` / integration**
  - default append: file row-count grows only by the gap; no genesis re-walk.
  - `--force`: file is overwritten (rewrite path exercised).
  - a symbol already current → **0 rounds fetched** (regression guard for the
    original bug).
- **oracle path**
  - `start_block` resumes from coverage by default; genesis only under `--force`.

## Affected files

- `src/gmx_historical_data/gap_analyzer.py`
- `src/gmx_historical_data/fetch_boundary_calculator.py`
- `src/gmx_historical_data/cli.py`
- `src/gmx_historical_data/storage.py` (flag pass-through)
- `tests/` — new/updated unit + integration tests per above
