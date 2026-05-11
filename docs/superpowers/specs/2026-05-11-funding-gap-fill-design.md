# Funding Rate Hourly Gap-Fill — Design Spec

**Date:** 2026-05-11
**Status:** Draft — pending review
**Related plan:** `docs/superpowers/plans/2026-05-11-funding-gap-fill.md`
**Predecessor:** `docs/superpowers/specs/2026-05-11-funding-correctness-polars-design.md`

## 1. Background

`scripts/extract_funding_factor.py:aggregate_hourly_rates` emits **one row per (symbol, market, hour) that contains at least one on-chain `Funding` event**. For low-volume markets, that leaves silent hours with no row. Audit of all 129 markets on 2026-05-11 found 88 markets with post-Oct 2025 internal gaps >48 h (worst: `AI16Z` at 1731 h, `MKR` at 1408 h). For majors (BTC, ETH, SOL, XRP, DOGE, LINK), post-Oct coverage is contiguous.

The funding factor is a **contract state variable**, not a per-event quantity. The on-chain rate at any second is the most recent `fundingFactorPerSecond` reported by an event, in effect until the next event. So a silent hour does not mean "rate unknown" — it means **"rate unchanged since the last event."** Downstream consumers (FreqTrade backtesting, notebooks, strategies) currently see the rate as missing rather than carried-forward, which forces every consumer to re-derive the same logic.

## 2. Goals

- After the unified merge step, every hour in `[first_event_hour, last_event_hour]` for each (symbol, market) has exactly one row in `1h.parquet`.
- Silent-hour rows inherit `funding_rate` and derived columns from the most recent prior event row.
- Filled rows are flagged via a new boolean column `is_gap_filled` (`true` = filled, `false` = had at least one event in the hour) so accuracy-critical consumers can exclude them.
- `update_count` for filled rows is `0`.
- The raw event parquet (`raw/funding/{SYMBOL}/...`) and the factor parquet (`rates/{SYM}/1h_factor.parquet`) are **unchanged** — fill happens only on the merged output.

## 3. Non-goals

- **No forward-fill past the last on-chain event.** Dead markets stop at their last observed hour; we do not zombie-fill to "now."
- **No backward-fill.** Hours before the first event in a market remain absent (we have no prior state to carry).
- **No interpolation.** Forward-fill only — that is what the on-chain semantics dictate.
- No change to `1h_factor.parquet` (event-only, faithful to chain).
- No change to `1h_datastore.parquet` (already hourly, synthesised from RPC reads).

## 4. Design

### 4.1 New helper: `forward_fill_hourly_grid`

Lives in `scripts/extract_unified_funding.py` (alongside `apply_direction_to_rates`). Pure function, single (symbol, market) input.

```python
def forward_fill_hourly_grid(rates: pl.DataFrame) -> pl.DataFrame:
    """Expand event-based hourly rates onto a continuous grid.

    Builds an hourly grid spanning ``[min(timestamp), max(timestamp)]`` and
    forward-fills rate columns from the most recent prior event. Adds
    ``is_gap_filled`` boolean and zeroes ``update_count`` for filled rows.
    """
```

**Operations (Polars only):**

1. If empty input → return unchanged (preserve schema downstream).
2. Compute hourly grid: `pl.datetime_range(min, max, interval="1h", time_zone="UTC")`.
3. Left-join input on `timestamp` → unmatched grid hours have NULL in event columns.
4. Capture `is_gap_filled = update_count.is_null()` BEFORE filling.
5. Forward-fill numeric rate columns: `funding_rate`, `funding_rate_min`, `funding_rate_max`, `funding_rate_hourly`, `funding_rate_annualized`.
6. Forward-fill identifier columns: `symbol`, `market`.
7. Fill `update_count` nulls with `0` (cast back to `UInt32`).
8. Sort by `timestamp`.

The grid construction handles single-market cases naturally. The factor parquet is already partitioned by symbol (the storage layout writes one parquet per symbol under `rates/{SYM}/1h_factor.parquet`), so each call to `forward_fill_hourly_grid` processes one symbol's data.

### 4.2 Integration point in `merge_symbol`

```python
factor_path = rates_dir / symbol / "1h_factor.parquet"
if factor_path.exists():
    hs_df = pl.read_parquet(factor_path)
    hs_df = forward_fill_hourly_grid(hs_df)           # ← NEW
    dir_df = pl.read_parquet(direction_path) if direction_path.exists() else None
    hs_df = apply_direction_to_rates(hs_df, dir_df)
    hs_df = hs_df.with_columns(pl.lit("hypersync").alias("source"))
    frames.append(hs_df)
```

Order matters: fill *before* direction merge so direction can also forward-fill across the now-dense grid (handled by the existing forward_fill in `apply_direction_to_rates`).

### 4.3 Interaction with `apply_direction_to_rates`

The existing direction forward-fill (added in the previous chunk) already uses `forward_fill().over(group_keys)`. When the rates grid is now dense, the direction forward-fill naturally extends across filled rows — no change required.

If the rates frame has an `is_gap_filled` column when it reaches `apply_direction_to_rates`, the helper should preserve it. Current implementation does (it only adds `longs_pay_shorts`, `funding_fee_long`, `funding_fee_short`).

### 4.4 Interaction with the datastore concat

The merge step concatenates `1h_datastore.parquet` (pre-V2.2, already hourly + signed) with the gap-filled factor frame (V2.2+). The concat uses `how="diagonal_relaxed"`, so missing `is_gap_filled` on the datastore side becomes NULL. We fill those NULLs with `false` after concat (DataStore rows are not gap-filled — each is a real per-hour DataStore read).

### 4.5 Schema change

`1h.parquet` gains one column:

| Column | Type | Meaning |
|---|---|---|
| `is_gap_filled` | `Boolean` | `true` = no on-chain event in this hour, value forward-filled. `false` = ≥1 event observed. |

Existing consumers that don't reference `is_gap_filled` are unaffected (Polars/pandas readers tolerate extra columns).

The FreqTrade feather export currently builds a 6-column OHLCV frame (`date`, `open`, `high`, `low`, `close`, `volume`); it doesn't propagate `is_gap_filled`. Acceptable — FreqTrade consumers want a continuous grid regardless.

## 5. Risks & migration

- **Existing `1h.parquet` files were produced without fill.** New runs of `make funding-unified` or `funding-unified-resume` overwrite them. No migration script needed; data regenerates from `1h_factor.parquet` + `1h_datastore.parquet` on next merge.
- **File size grows for sparse markets.** Worst case: `AI16Z` jumps from 1102 rows to ~5800 rows (≈ 5.3× larger). Aggregate across 129 markets: ~9 % parquet size increase. Acceptable.
- **No retroactive impact on factor parquet** — `1h_factor.parquet` stays event-only. A future consumer that wants pure event data can still read it directly.

## 6. Testing strategy

- Unit test: synthetic 3-event hourly frame with two gaps → grid contains all hours, gaps forward-filled, `is_gap_filled` matches expectation.
- Unit test: empty frame → empty frame returned.
- Unit test: single-row frame → returned unchanged (grid is one hour).
- Unit test: direction forward-fill still works on gap-filled rates.
- Integration test: re-run merge on a real symbol (BTC, with 138 h V2.2 gap) → confirm row count rises, `funding_rate` continuous, the gap is filled with the rate from the last event before the gap.

## 7. Out of scope (future)

- Extend-to-now policy for active markets (currently the grid ends at last on-chain event).
- Configurable fill horizon via CLI (`--fill-horizon=now|last_event|none`).
- Backfill from `extract_funding_datastore.py` for the V2.2 rollout gap (would require archive RPC).
- A dedicated `1h_continuous.parquet` separate from `1h.parquet` if event-only consumers complain about the schema growth.
