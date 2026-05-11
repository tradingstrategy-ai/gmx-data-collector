# Funding Rate Correctness & Polars Cleanup — Design Spec

**Date:** 2026-05-11
**Status:** Draft — pending review
**Related plan:** `docs/superpowers/plans/2026-05-11-funding-correctness-polars.md`

## 1. Background

A code audit of the GMX funding rate collection pipeline surfaced four classes of issues:

1. **Correctness:** `longs_pay_shorts` is hardcoded `True` in the factor extractor (`scripts/extract_funding_factor.py:582`). The merge step in `scripts/extract_unified_funding.py:478-480` tries to correct this by joining direction data from `extract_funding_fee_per_size.py`, but `fill_null(True)` silently assumes longs pay when no fee-per-size events exist in an hour.
2. **Aggregation accuracy:** `aggregate_hourly_rates` in `scripts/extract_funding_factor.py:704` uses unweighted `mean()` over events within an hour. GMX emits `Funding` events on every position update, so events cluster around active trading. A simple mean overweights clusters; a time-weighted average (TWAP) — weighting each rate by the seconds it remained in effect — is faithful to actual accrued funding.
3. **Polars cleanup:** Two scripts still use pandas in the feather export hot path:
   - `scripts/extract_unified_funding.py:585-672` (`export_feather`): `pd.read_feather/read_parquet` + `pyarrow.feather.write_feather`.
   - `scripts/extract_funding_fee_per_size.py:885-899`: `.to_pandas()` round-trip before `pyarrow.feather.write_feather`.
4. **Code hygiene:** Dead `HAS_FEATHER` / `HAS_POLARS` try/except import shims in `extract_unified_funding.py:60-72`, `extract_funding_factor.py:105-110`, `extract_funding_fee_per_size.py:118-129`. Polars and pyarrow are hard requirements (declared in `pyproject.toml`); the fallbacks are unreachable. Some scripts also import inside `main()` (e.g. `extract_unified_funding.py:891-894`).

## 2. Goals

- **Correctness:** Eliminate silent assumptions about funding direction. Either propagate `null` or carry the last known direction forward, but never silently default to `longs_pay_shorts=True`.
- **Aggregation:** Switch hourly aggregation to time-weighted (TWAP). Document the exact formula in code.
- **Polars-only feather export:** Remove the last pandas paths in feather writers.
- **Cleanup:** Delete dead optional-dependency shims; hoist function-local imports.

## 3. Non-goals

- No changes to `extract_funding_factor.py`'s ABI decoding logic.
- No changes to file naming conventions (`1h.parquet`, `1h_factor.parquet`, `*-1h-funding_rate.feather`).
- No retroactive recompute of historical hours — only new collections will use TWAP. Re-collecting the full history is optional and can be triggered separately by the user.
- No changes to the merge logic between HyperSync and DataStore sources.

## 4. Design

### 4.1 `longs_pay_shorts` correctness

**Factor extractor (`extract_funding_factor.py`):**
- Stop writing the `longs_pay_shorts` column from per-event records (`FundingFactorRecord`). The factor event genuinely doesn't encode direction.
- Stop computing `funding_fee_long` / `funding_fee_short` in the factor output (they're meaningless without direction). The signed columns belong in the unified merge stage where direction is joined in.
- Keep `funding_rate_min/max/hourly/annualized` (unsigned magnitudes) — these are well-defined without direction.

**Unified merge (`extract_unified_funding.py`):**
- Replace `fill_null(True)` (line 479) with `forward_fill().over("symbol")` to carry the last known direction. Newer history retains the most recent observed direction until the next direction event flips it.
- For the leading null gap (hours before any direction event), explicitly mark the column as `null` and skip the signed-fee derivation for those rows (leave `funding_fee_long/short` as null). Downstream consumers can decide how to handle.

### 4.2 TWAP aggregation

**Current (factor.py:697-711, unweighted mean):**
```python
df.with_columns(pl.col("timestamp").dt.truncate("1h").alias("hour"))
  .group_by(["symbol","market","hour"])
  .agg(pl.col("funding_rate_per_second").mean().alias("funding_rate"))
```

**New (TWAP):** For each (symbol, market, hour) bucket, the rate at any second within the hour is the most recent `funding_rate_per_second` reported by an event up to that second. TWAP = (sum over events of `rate_i * dt_i`) / `seconds_in_hour`, where `dt_i` is the seconds the event's rate was in effect within the hour.

Implementation in Polars:

```python
df = df.sort(["symbol", "market", "timestamp"]).with_columns(
    pl.col("timestamp").dt.truncate("1h").alias("hour"),
)
# Compute seconds-in-effect per event, capped at the hour boundary.
df = df.with_columns(
    (
        pl.col("timestamp").dt.epoch("s").shift(-1).over(["symbol", "market"])
        .clip_max((pl.col("hour") + pl.duration(hours=1)).dt.epoch("s"))
        - pl.col("timestamp").dt.epoch("s")
    ).alias("dt_seconds"),
)
# For the last event before the hour boundary, dt = hour_end - timestamp.
df = df.with_columns(pl.col("dt_seconds").fill_null(
    (pl.col("hour") + pl.duration(hours=1)).dt.epoch("s") - pl.col("timestamp").dt.epoch("s")
))
# TWAP = sum(rate * dt) / 3600.
hourly = df.group_by(["symbol", "market", "hour"]).agg(
    (pl.col("funding_rate_per_second") * pl.col("dt_seconds")).sum().alias("weighted_sum"),
    pl.col("dt_seconds").sum().alias("total_seconds"),
    pl.col("funding_rate_per_second").min().alias("funding_rate_min"),
    pl.col("funding_rate_per_second").max().alias("funding_rate_max"),
    pl.len().alias("update_count"),
)
hourly = hourly.with_columns(
    (pl.col("weighted_sum") / pl.col("total_seconds")).alias("funding_rate"),
).drop(["weighted_sum", "total_seconds"])
```

**Edge cases:**
- Single event in the hour: TWAP = that rate (degenerates correctly).
- First event ever in a market: the rate before the event is unknown. We start the TWAP window at the event's timestamp (not the hour boundary). Hours preceding the first event get no row.
- Last event in the data: the rate is assumed to remain in effect until the hour boundary (we don't extrapolate beyond).

### 4.3 Polars-only export paths

**`extract_unified_funding.py:export_feather` (lines 585-672):** Rewrite using Polars exclusively:
- `pl.read_parquet(path)` for input.
- Polars expressions for column construction (`pl.col("timestamp").cast(pl.Datetime("ns", "UTC"))`).
- `df.write_ipc(filepath)` for output. Polars `write_ipc` writes Arrow IPC v2 = feather v2.

**`extract_funding_fee_per_size.py:875-899`:** Same pattern — drop `.to_pandas()` + `pq_feather.write_feather`, use `df.write_ipc(...)` directly.

### 4.4 Cleanup

- Delete `HAS_POLARS` / `HAS_FEATHER` try/except blocks. Plain `import polars as pl` and `import pyarrow` at module top. If polars is missing, `ImportError` is the right behaviour (matches every other script).
- Hoist function-local imports in `extract_unified_funding.py:main()` to module top.

## 5. Risks & migration

- **TWAP changes data semantics.** Existing `1h.parquet` and `1h_factor.parquet` files were computed with unweighted mean. New collections will produce different values for the same hour. Acceptable per user direction. The user can choose to re-collect via `make funding-unified-nn` if they want consistent history.
- **`longs_pay_shorts=null` for leading gap.** Downstream consumers (FreqTrade, notebooks) that expect a boolean will see null. Auditing every consumer is out of scope; we'll document the behaviour in the column docstring and check the notebooks in `notebooks/` for any breakage.

## 6. Testing strategy

- Unit tests for TWAP aggregation: synthetic events with known dt → expected TWAP.
- Unit test for the factor extractor: confirm `longs_pay_shorts` column is no longer written.
- Unit test for the merge: confirm forward-fill behaviour with a market that has a direction event mid-history.
- Round-trip test for Polars-only `export_feather`: write a feather, read it back with pandas (FreqTrade compatibility), confirm columns match.
- Smoke test: re-run `make funding-unified` on a single market (`MARKET=ETH/USD`) and verify parquet date range matches the pre-change run.

## 7. Out of scope (future)

- Adding a `funding_rate_signed` column with the longs-pay sign baked in, as a convenience for downstream consumers.
- Replacing the cron-based daily collection with event-driven incremental updates.
- Migrating `extract_funding_factor.py` raw event decode to Polars (currently uses dataclass per record; benefits would be small).
