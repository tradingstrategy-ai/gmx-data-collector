# Polars Migration Design

## Goal

> This spec describes the **target state** after migration. All code examples show what the implementation will look like, not what currently exists.

Replace pandas with Polars for all CPU-bound data processing in both the collection pipeline and the freqtrade export pipeline. Polars is already a declared dependency (`pyproject.toml`). No new dependencies required.

## Architecture

All heavy aggregation, sorting, deduplication, and file I/O migrates to Polars. Public return types at pipeline boundaries remain `pd.DataFrame` for the aggregators (so `cli.py` call sites outside `_merge_and_save_candles` need no changes). `freqtrade_exporter.py` and `storage.py` migrate fully end-to-end.

## Tech Stack

- **Polars** `^1.38.1` (already declared) — lazy/eager DataFrame engine backed by Rust `polars-core`
- **PyArrow** — kept for schema definitions; Polars reads/writes Arrow natively
- **pandas** — kept at call-site boundaries where return type is `pd.DataFrame`

---

## Files Changed

| File | Change |
|------|--------|
| `src/gmx_historical_data/oracle_event_aggregator.py` | Polars internals; return `pd.DataFrame` via `.to_pandas()` |
| `src/gmx_historical_data/event_aggregator.py` | Same pattern |
| `src/gmx_historical_data/freqtrade_exporter.py` | Full Polars migration (read → transform → write) |
| `src/gmx_historical_data/storage.py` | Polars native `write_parquet` + zstd-3 |
| `src/gmx_historical_data/cli.py` | `_merge_and_save_candles`: Polars concat/unique/sort |

**No changes to:** `cli.py` orchestration, HyperSync collectors, `block_timestamp_cache.py`, checkpoint logic, daemon config.

---

## Component Designs

### 1. `oracle_event_aggregator.py`

**Timeframe mapping** — pandas aliases differ from Polars:

```python
_PANDAS_TO_POLARS_TIMEFRAME = {
    "1min": "1m",
    "5min": "5m",
    "15min": "15m",
    "1h": "1h",
    "4h": "4h",
    "1D": "1d",
}
```

**`build_oracle_price_dataframe`** — replaces list comprehension + pandas DataFrame construction:

```python
import polars as pl

def build_oracle_price_dataframe(events, token_decimals=18):
    if not events:
        return pd.DataFrame(columns=["timestamp", "price"])
    divisor = get_price_divisor(token_decimals)
    df = pl.DataFrame({
        "timestamp": [pd.Timestamp(e.block_timestamp, unit="s", tz="UTC") for e in events],
        "price": [(e.min_price + e.max_price) / 2 / divisor for e in events],
        "original_order": list(range(len(events))),
    })
    df = df.sort(["timestamp", "original_order"])
    return df.to_pandas()
```

**`resample_oracle_price_dataframe`** — replaces `resample().agg()`:

```python
def resample_oracle_price_dataframe(price_df, timeframe, symbol):
    if price_df.empty:
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "symbol"])
    polars_tf = _PANDAS_TO_POLARS_TIMEFRAME[timeframe]
    df = pl.from_pandas(price_df)
    ohlcv = df.sort("timestamp").group_by_dynamic(
        "timestamp", every=polars_tf
    ).agg([
        pl.first("price").alias("open"),
        pl.max("price").alias("high"),
        pl.min("price").alias("low"),
        pl.last("price").alias("close"),
    ])
    ohlcv = ohlcv.with_columns(pl.lit(symbol).alias("symbol"))
    ohlcv = ohlcv.drop_nulls(subset=["open", "high", "low", "close"])
    return ohlcv.to_pandas()
```

**`aggregate_oracle_events_to_ohlcv`** — kept unchanged (still used in `_collect_symbol_via_oracle_fallback`). Internally delegates to the two functions above.

### 2. `event_aggregator.py`

Same timeframe mapping and same `group_by_dynamic` pattern applied to `aggregate_events_to_ohlcv`. The execution price vs oracle mid-price selection logic stays in Python before the DataFrame is built.

```python
def aggregate_events_to_ohlcv(events, timeframe, symbol, use_execution_price=False):
    if not events:
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "symbol"])
    polars_tf = _PANDAS_TO_POLARS_TIMEFRAME[timeframe]
    prices = [
        e.execution_price if use_execution_price else (e.index_token_price_min + e.index_token_price_max) / 2
        for e in events
    ]
    df = pl.DataFrame({
        "timestamp": [pd.Timestamp(e.block_timestamp, unit="s", tz="UTC") for e in events],
        "price": prices,
        "original_order": list(range(len(events))),
    })
    df = df.sort(["timestamp", "original_order"])
    ohlcv = df.group_by_dynamic("timestamp", every=polars_tf).agg([
        pl.first("price").alias("open"),
        pl.max("price").alias("high"),
        pl.min("price").alias("low"),
        pl.last("price").alias("close"),
    ])
    ohlcv = ohlcv.with_columns(pl.lit(symbol).alias("symbol"))
    ohlcv = ohlcv.drop_nulls(subset=["open", "high", "low", "close"])
    return ohlcv.to_pandas()
```

### 3. `freqtrade_exporter.py`

Full end-to-end Polars. No pandas imports remain.

**Read:**
```python
df = pl.read_parquet(path)
```

**Transform OHLCV** — no deduplication (data is already clean from `_merge_and_save_candles`):
```python
df = df.rename({"timestamp": "date"})
df = df.with_columns([
    pl.col("date").dt.convert_time_zone("UTC").dt.cast_time_unit("ns"),
    pl.lit(0).cast(pl.Float64).alias("volume"),
])
df = df.select(["date", "open", "high", "low", "close", "volume"])
df = df.sort("date")
```

**Transform funding rate** — dedup with `keep="first"` to match current `drop_duplicates()` default:
```python
# Map funding_rate_hourly → open; zero out high/low/close/volume
col = "funding_rate_hourly" if "funding_rate_hourly" in df.columns else "funding_rate"
df = df.rename({"timestamp": "date", col: "open"})
df = df.with_columns([
    pl.lit(0).cast(pl.Float64).alias("high"),
    pl.lit(0).cast(pl.Float64).alias("low"),
    pl.lit(0).cast(pl.Float64).alias("close"),
    pl.lit(0).cast(pl.Float64).alias("volume"),
    pl.col("date").dt.convert_time_zone("UTC").dt.cast_time_unit("ns"),
])
df = df.sort("date").unique(subset=["date"], keep="first")
```

**Transform mark price** — same dedup pattern as funding rate:
```python
# Mark price reuses OHLCV transform then deduplicates
df = df.sort("date").unique(subset=["date"], keep="first")
```

**Write feather (Arrow IPC = feather v2):**
```python
df.write_ipc(output_path)
```

### 4. `storage.py`

Replace `pq.write_table(..., compression_level=22)` with Polars native write at `compression_level=3`:

```python
# Before
pq.write_table(arrow_table, path, compression="zstd", compression_level=22)

# After
pl.from_arrow(arrow_table).write_parquet(str(path), compression="zstd", compression_level=3)
```

Schema enforcement currently done via PyArrow schema at write time. With Polars, enforce schema by casting columns explicitly before write — same guarantees, no PyArrow dependency at the write call.

### 5. `cli.py` — `_merge_and_save_candles`

Replace pandas concat/dedup/sort:

```python
# Before
combined = pd.concat(dfs, ignore_index=True)
combined = combined.drop_duplicates(subset=["timestamp"], keep="last")
combined = combined.sort_values("timestamp").reset_index(drop=True)
storage.save_candles(combined, timeframe, symbol)

# After
pl_frames = [pl.from_pandas(df) for df in dfs if df is not None and not df.empty]
combined = pl.concat(pl_frames)
combined = combined.unique(subset=["timestamp"], keep="last")
combined = combined.sort("timestamp")
storage.save_candles(combined.to_pandas(), timeframe, symbol)
```

---

## Testing

- All existing tests continue to pass (public return types unchanged for aggregators)
- Each migrated function gets a fixture-based test asserting numerical equivalence between old pandas output and new Polars output (float tolerance `1e-9`)
- `freqtrade_exporter.py` tests updated to use Polars-native assertions
- One `@pytest.mark.slow` benchmark test: pandas vs Polars on 50k synthetic oracle events across 6 timeframes — quantifies speedup in CI output

## Expected Speedup

| Operation | pandas | Polars | Gain |
|-----------|--------|--------|------|
| `group_by_dynamic` (resample) | baseline | 5–10x | CPU-bound aggregation |
| `sort` | baseline | 3–5x | Parallel radix sort |
| `unique` (dedup) | baseline | 3–8x | Hash-based, parallel |
| `write_parquet` zstd-3 vs zstd-22 | baseline | 5–15x on write | Compression level |
| `write_ipc` (feather) | baseline | 2–3x | Arrow native |

End-to-end collection run: estimated 20–35% reduction in wall time (aggregation is not the dominant bottleneck — HyperSync I/O is). Freqtrade export: estimated 3–8x faster.
