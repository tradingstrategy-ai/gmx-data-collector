# Funding Rate Correctness & Polars Cleanup — Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:executing-plans (or superpowers:subagent-driven-development if subagents available) to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix two correctness bugs in the funding rate pipeline (hardcoded `longs_pay_shorts=True`), switch hourly aggregation to time-weighted (TWAP), and finish the Polars migration in the feather export hot path.

**Architecture:** Four independent chunks, each in its own commit. Chunk 1 is pure cleanup (deletable shims, import hoisting). Chunk 2 changes parquet column schema (removes meaningless signed-fee columns from factor output). Chunk 3 changes only file I/O (no data semantics). Chunk 4 changes hourly bucket values (TWAP vs mean) — acceptable per user.

**Tech Stack:** Polars `^1.x` (existing), pyarrow (kept for schema/file format), pytest, ruff.

**Spec:** `docs/superpowers/specs/2026-05-11-funding-correctness-polars-design.md`

**Prep changes (uncommitted, in this branch):** Makefile `DATA_DIR`/`FEATHER_DIR` defaults updated to external drive, `user_data/data/{gmx,binance,bybit}` symlinks removed, `OVERWRITE` flag added to `export-freqtrade`. Bundle with chunks below.

---

## Chunk 1: Code hygiene

### Task 1: Delete dead `HAS_*` shims and hoist imports

**Files:**
- Modify: `scripts/extract_unified_funding.py:60-72` (delete try/except, also lines 602, 891-894 main-local imports)
- Modify: `scripts/extract_funding_factor.py:105-110` (delete try/except, also line 680/796/842 `HAS_POLARS` guards)
- Modify: `scripts/extract_funding_fee_per_size.py:118-129` (delete try/except, also 698/829/875 guards)

**Context:** `pyproject.toml` requires `polars`, `pyarrow`, `pandas`. The `try/except ImportError` blocks are dead. Each guard like `if not HAS_POLARS: sys.exit(1)` becomes unreachable once the import is unconditional.

- [ ] **Step 1.1: Run baseline tests** to capture current pass count
  ```bash
  export JSON_RPC_ARBITRUM=$ARBITRUM_CHAIN_JSON_RPC
  poetry run python -m pytest tests/ -q --ignore=tests/test_hybrid_collection.py --ignore=tests/test_gmx_event_collector.py --ignore=tests/test_integration_event_based.py --ignore=tests/test_integration_gmx_first.py --ignore=tests/test_live_funding.py 2>&1 | tail -5
  ```

- [ ] **Step 1.2: Clean `extract_unified_funding.py`**
  - Replace lines 60-72 (try/except for polars + pandas+pyarrow.feather):
    ```python
    import polars as pl
    import pyarrow.feather as pq_feather
    ```
  - Note: `import pandas as pd` is only used in `export_feather` — Chunk 3 will remove it. For Chunk 1, keep `import pandas as pd` at module top (not in try/except).
  - Delete the `if not HAS_FEATHER: ...` guard at line 602.
  - Find function-local imports inside `main()` (around line 891) and hoist them to the top of the module.

- [ ] **Step 1.3: Clean `extract_funding_factor.py`**
  - Replace lines 105-110 with unconditional `import polars as pl`.
  - Delete `if not HAS_POLARS: ...` guards at 680, 796, 842.

- [ ] **Step 1.4: Clean `extract_funding_fee_per_size.py`**
  - Replace lines 118-129 with unconditional imports.
  - Delete `if not HAS_POLARS: ...` and `if not HAS_FEATHER: ...` guards.

- [ ] **Step 1.5: Smoke-test each script**
  ```bash
  poetry run python scripts/extract_unified_funding.py --help
  poetry run python scripts/extract_funding_factor.py --help
  poetry run python scripts/extract_funding_fee_per_size.py --help
  ```
  Each must print `--help` text without error.

- [ ] **Step 1.6: Run unit tests** — must match Step 1.1 pass count.

- [ ] **Step 1.7: Lint** — `poetry run ruff check scripts/extract_unified_funding.py scripts/extract_funding_factor.py scripts/extract_funding_fee_per_size.py`

- [ ] **Step 1.8: Show diff to user for review.**

---

## Chunk 2: `longs_pay_shorts` correctness fix

### Task 2a: Factor extractor — stop emitting hardcoded direction

**Files:**
- Modify: `scripts/extract_funding_factor.py` — `FundingFactorRecord` dataclass, `aggregate_hourly_rates`, the per-record write step
- Modify: `tests/test_funding_factor.py` if it exists, else create

**Context:** factor.py:582 sets `longs_pay = True` because the event doesn't encode direction. This value flows into the per-record parquet (`raw/`) and the hourly aggregate (`1h_factor.parquet` after rename). The merge step replaces it correctly but the intermediate file has misleading values.

- [ ] **Step 2a.1: Read current `FundingFactorRecord` schema** (factor.py around line 176) to confirm fields.

- [ ] **Step 2a.2: Write failing test** — `tests/test_funding_factor_aggregate.py`:
  ```python
  """Test that aggregate_hourly_rates does not emit longs_pay_shorts."""
  import polars as pl
  from datetime import datetime, timezone
  from scripts.extract_funding_factor import aggregate_hourly_rates

  def test_aggregate_omits_longs_pay_shorts():
      ts = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
      df = pl.DataFrame({
          "block_number": [1, 2],
          "block_timestamp": [int(ts.timestamp()), int(ts.timestamp()) + 30],
          "timestamp": [ts, ts],
          "symbol": ["ETH", "ETH"],
          "market": ["0xm", "0xm"],
          "funding_rate_per_second": [1e-9, 2e-9],
      })
      hourly = aggregate_hourly_rates(df)
      assert "longs_pay_shorts" not in hourly.columns
      assert "funding_fee_long" not in hourly.columns
      assert "funding_fee_short" not in hourly.columns
      assert "funding_rate" in hourly.columns
      assert "funding_rate_min" in hourly.columns
      assert "funding_rate_max" in hourly.columns
  ```

- [ ] **Step 2a.3: Run test to confirm failure.**

- [ ] **Step 2a.4: Modify `FundingFactorRecord`** — remove `longs_pay_shorts` field (and the corresponding column in any to-parquet path).

- [ ] **Step 2a.5: Modify `aggregate_hourly_rates`** (factor.py:669-754) — remove the `longs_pay_shorts`, `funding_fee_long`, `funding_fee_short` columns from the agg/with_columns steps. Keep min/max/update_count.

- [ ] **Step 2a.6: Modify the per-record builder** at factor.py:582-610 — delete the `longs_pay = True` line and don't pass it to `FundingFactorRecord`.

- [ ] **Step 2a.7: Run the new test** — must pass.

- [ ] **Step 2a.8: Run full test suite** — track regressions.

### Task 2b: Unified merge — forward-fill direction

**Files:**
- Modify: `scripts/extract_unified_funding.py:478-492` (`merge_factor_with_direction` block)

- [ ] **Step 2b.1: Write failing test** — `tests/test_unified_merge_direction.py`:
  ```python
  """Test direction forward-fill in unified merge."""
  import polars as pl
  from datetime import datetime, timezone, timedelta
  from scripts.extract_unified_funding import merge_factor_with_direction  # or wherever

  def test_forward_fill_direction():
      base = datetime(2026, 1, 1, tzinfo=timezone.utc)
      hs_df = pl.DataFrame({
          "timestamp": [base + timedelta(hours=i) for i in range(5)],
          "symbol": ["ETH"] * 5,
          "market": ["0xm"] * 5,
          "funding_rate": [1e-9] * 5,
      })
      # Direction observed only at hour 2 (longs pay)
      dir_df = pl.DataFrame({
          "timestamp": [base + timedelta(hours=2)],
          "direction_longs_pay": [True],
      })
      merged = merge_factor_with_direction(hs_df, dir_df)
      # Hour 0-1: no prior direction → null (or expected leading-null behaviour)
      # Hour 2-4: True (forward-filled)
      assert merged.filter(pl.col("timestamp") == base)["longs_pay_shorts"].item() is None
      assert merged.filter(pl.col("timestamp") == base + timedelta(hours=2))["longs_pay_shorts"].item() is True
      assert merged.filter(pl.col("timestamp") == base + timedelta(hours=4))["longs_pay_shorts"].item() is True
  ```
  (May need to extract the merge logic into a named function for testability.)

- [ ] **Step 2b.2: Run test to confirm failure.**

- [ ] **Step 2b.3: Modify the merge step.** Replace `fill_null(True)` with forward-fill over `["symbol", "market"]`:
  ```python
  hs_df = hs_df.sort(["symbol", "market", "timestamp"]).with_columns(
      pl.col("direction_longs_pay").forward_fill().over(["symbol", "market"]).alias("longs_pay_shorts")
  ).drop("direction_longs_pay")
  ```
  Then guard the signed-fee derivation: only compute `funding_fee_long/short` when `longs_pay_shorts` is non-null; leave nulls for leading gap rows.

- [ ] **Step 2b.4: Run the new test.**

- [ ] **Step 2b.5: Run full test suite.**

- [ ] **Step 2b.6: Show diff and an example output for review.**

---

## Chunk 3: Polars-only feather export

### Task 3: Remove pandas from `export_feather` and `extract_funding_fee_per_size.py` writer

**Files:**
- Modify: `scripts/extract_unified_funding.py:585-672` (`export_feather`)
- Modify: `scripts/extract_funding_fee_per_size.py:875-899` (the per-symbol feather write block)

**Context:** Both paths read parquet, transform, and write feather. Polars `read_parquet`/`write_ipc` cover the entire pipeline without pandas round-trip.

- [ ] **Step 3.1: Write round-trip test** — `tests/test_funding_feather_export.py`:
  ```python
  """Polars-only feather export must produce a feather file readable by pandas."""
  import pandas as pd
  import polars as pl
  import tempfile
  from pathlib import Path
  from datetime import datetime, timezone
  from scripts.extract_unified_funding import export_feather  # or its refactored equivalent

  def test_export_feather_pandas_compatible(tmp_path):
      # Build a synthetic 1h.parquet
      rates = tmp_path / "rates" / "ETH"
      rates.mkdir(parents=True)
      ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
      df = pl.DataFrame({
          "timestamp": [ts],
          "funding_rate_hourly": [1.5e-6],
          "symbol": ["ETH"],
      })
      df.write_parquet(rates / "1h.parquet")

      feather_root = tmp_path / "out"
      export_feather(network_dir=tmp_path, feather_dir=feather_root)
      out = feather_root / "data" / "gmx" / "futures" / "ETH_USDC_USDC-1h-funding_rate.feather"
      assert out.exists()
      back = pd.read_feather(out)
      assert list(back.columns) == ["date", "open", "high", "low", "close", "volume"]
      assert back["open"].iloc[0] == 1.5e-6
  ```

- [ ] **Step 3.2: Run test (should fail — `export_feather` signature may differ or it uses pandas).**

- [ ] **Step 3.3: Rewrite `export_feather`** — pure Polars:
  ```python
  def export_feather(network_dir: Path, feather_dir: Path,
                     market_filter: str | None = None,
                     quote_currency: str = "USDC") -> None:
      rates_dir = network_dir / "rates"
      if not rates_dir.exists():
          return
      gmx_dir = feather_dir / "data" / "gmx" / "futures"
      gmx_dir.mkdir(parents=True, exist_ok=True)

      for sym_dir in sorted(rates_dir.iterdir()):
          if not sym_dir.is_dir():
              continue
          symbol = sym_dir.name
          # Prefer 1h.parquet (merged), fall back to 1h_factor.parquet
          src = sym_dir / "1h.parquet"
          if not src.exists():
              src = sym_dir / "1h_factor.parquet"
              if not src.exists():
                  continue
          df = pl.read_parquet(src)
          if df.is_empty():
              continue
          rate_col = "funding_rate_hourly" if "funding_rate_hourly" in df.columns else "funding_rate"
          ft = (
              df.with_columns(
                  pl.col("timestamp").cast(pl.Datetime("ns", "UTC")).alias("date"),
                  pl.col(rate_col).cast(pl.Float64).alias("open"),
              )
              .with_columns(
                  pl.lit(0.0).alias("high"),
                  pl.lit(0.0).alias("low"),
                  pl.lit(0.0).alias("close"),
                  pl.lit(0.0).alias("volume"),
              )
              .select(["date", "open", "high", "low", "close", "volume"])
              .sort("date")
              .unique(subset=["date"], keep="first", maintain_order=True)
              .drop_nulls(subset=["open"])
          )
          out = gmx_dir / f"{symbol}_{quote_currency}_{quote_currency}-1h-funding_rate.feather"
          ft.write_ipc(out)
  ```

- [ ] **Step 3.4: Delete `import pandas as pd` and `import pyarrow.feather as pq_feather` from the script if no longer used elsewhere.**

- [ ] **Step 3.5: Apply the same pattern to `extract_funding_fee_per_size.py`** lines 875-899 — replace `pdf = df.select(...).to_pandas(); pq_feather.write_feather(pdf, ...)` with direct `df.select(...).write_ipc(...)`.

- [ ] **Step 3.6: Run new test + full suite.**

- [ ] **Step 3.7: Lint.**

- [ ] **Step 3.8: Show diff and confirm pandas import is gone (`grep "import pandas" scripts/extract_unified_funding.py`).**

---

## Chunk 4: TWAP aggregation

### Task 4: Replace unweighted mean with time-weighted average in `aggregate_hourly_rates`

**Files:**
- Modify: `scripts/extract_funding_factor.py:669-754`
- Modify: `tests/test_funding_factor_aggregate.py` (from Chunk 2a)

**Context:** GMX emits `Funding` events on every position update — events cluster around active trading. Unweighted mean overweights bursts. TWAP weights each rate by the seconds it remained in effect within the hour. Formula and edge cases documented in the spec §4.2.

- [ ] **Step 4.1: Write failing test** — append to `tests/test_funding_factor_aggregate.py`:
  ```python
  def test_twap_weighting():
      """TWAP must weight each rate by its dt."""
      # 3 events in an hour:
      #   t=00:00:00 rate=1.0  → in effect 30 min
      #   t=00:30:00 rate=2.0  → in effect 15 min
      #   t=00:45:00 rate=3.0  → in effect 15 min (until hour boundary)
      # TWAP = (1.0*1800 + 2.0*900 + 3.0*900) / 3600 = 1.75
      base = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
      df = pl.DataFrame({
          "timestamp": [base, base + timedelta(minutes=30), base + timedelta(minutes=45)],
          "symbol": ["ETH"] * 3,
          "market": ["0xm"] * 3,
          "funding_rate_per_second": [1.0, 2.0, 3.0],
      })
      hourly = aggregate_hourly_rates(df)
      assert hourly.height == 1
      assert hourly["funding_rate"].item() == pytest.approx(1.75, rel=1e-9)
      assert hourly["funding_rate_min"].item() == 1.0
      assert hourly["funding_rate_max"].item() == 3.0
      assert hourly["update_count"].item() == 3

  def test_twap_single_event_in_hour():
      """Single event in an hour: TWAP = that rate."""
      base = datetime(2026, 1, 1, 12, 30, tzinfo=timezone.utc)
      df = pl.DataFrame({
          "timestamp": [base],
          "symbol": ["ETH"],
          "market": ["0xm"],
          "funding_rate_per_second": [5.0],
      })
      hourly = aggregate_hourly_rates(df)
      assert hourly["funding_rate"].item() == pytest.approx(5.0, rel=1e-9)
  ```

- [ ] **Step 4.2: Run tests to confirm failure** (the existing mean-based code will return 2.0 for the first test, not 1.75).

- [ ] **Step 4.3: Implement TWAP** in `aggregate_hourly_rates`. Replace the existing mean-based group_by with the TWAP formula in spec §4.2. Be careful with the boundary case: when the next event timestamp is in a different hour, cap dt at the hour boundary (and treat the remainder as the opening dt of the next hour's first record).

  Cleanest split: compute `dt_within_hour` per row first, then group_by.

- [ ] **Step 4.4: Run new tests.**

- [ ] **Step 4.5: Run full suite + lint.**

- [ ] **Step 4.6: Smoke test on real data** (single market, BTC) — re-extract a small slice:
  ```bash
  rm "/Volumes/WD Blue 1tb/VMs/data/gmx/funding/arbitrum/rates/BTC/1h_factor.parquet"
  poetry run python scripts/extract_funding_factor.py --network arbitrum \
    --output-dir "/Volumes/WD Blue 1tb/VMs/data/gmx/funding/arbitrum" \
    --output parquet --market "BTC/USD" \
    --from-block <last-known-block> --to-block <last-known-block + 1000>
  poetry run python -c "import polars as pl; print(pl.read_parquet('/Volumes/WD Blue 1tb/VMs/data/gmx/funding/arbitrum/rates/BTC/1h_factor.parquet').tail(5))"
  ```
  Sanity-check the funding_rate values are within historical range (1e-10 to 1e-8 for BTC).

- [ ] **Step 4.7: Show diff and the smoke-test output for review.**

---

## Chunk 5: Verify and re-extract

### Task 5: Run end-to-end + regenerate feather

- [ ] **Step 5.1: Run full test suite** — must match Chunk 1 baseline pass count (modulo the new tests added in Chunks 2-4, which should all PASS).

- [ ] **Step 5.2: Lint everything touched.**

- [ ] **Step 5.3: Re-run feather export with the new pipeline:**
  ```bash
  make export-freqtrade OVERWRITE=--overwrite KEEP=--keep
  ```

- [ ] **Step 5.4: Sample-check 3-5 markets** (BTC, ETH, SOL, LINK, ARB) — verify the date range matches parquet and feather row counts are sane.

- [ ] **Step 5.5: Final review** — show the user a summary of all changes and the verification output. Get explicit OK before any git commit.

---

## Commit grouping

When the user approves, group commits as:
1. `chore: remove broken user_data symlinks and point Makefile defaults to external drive`
2. `chore: add OVERWRITE flag to make export-freqtrade`
3. `chore: delete dead HAS_* shims, hoist function-local imports`
4. `fix(funding): stop emitting hardcoded longs_pay_shorts in factor output`
5. `fix(funding): forward-fill direction in unified merge instead of defaulting True`
6. `perf(funding): migrate export_feather and fee-per-size writer to pure Polars`
7. `feat(funding): switch hourly aggregation to TWAP (time-weighted)`

Each commit must pass tests + lint independently.
