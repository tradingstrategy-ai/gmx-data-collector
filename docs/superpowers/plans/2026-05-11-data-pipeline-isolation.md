# Data Pipeline Isolation — Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make OI / funding / OHLCV pipelines fully isolated so that operating on one data type cannot damage another. Specifically: stop `export-freqtrade --overwrite` from wiping OHLCV history when the user only intended to refresh funding, and stop the default exporter from deleting source parquet files.

**Architecture:**

The current `FreqtradeExporter.export()` writes four file kinds in a single loop (`-futures`, `-mark`, `-index`, `-funding_rate`). A flag like `--overwrite`, meant to fix the funding schema, applies to *all* writes. Worse, by default the exporter deletes the source candle parquet after writing the feather, removing the recovery path. We split the exporter into two independent code paths (`export_candles` and `export_funding`), make `keep_parquet=True` the default, and require an explicit `unsafe_overwrite=True` to bypass the history-preservation guard. OI and pool-liquidity already only write parquet to their own directories, so they're isolated already — we lock that in with a regression test.

**Tech Stack:** Python 3.11+, Polars, pytest, make. No new dependencies.

**Predecessor:** Builds on `docs/superpowers/plans/2026-05-11-funding-gap-fill.md` (already merged in commit 64c7f12).

**Background:** The 2026-05-11 incident truncated the BTC/ETH/SOL/DOGE/LTC/SEI/TAO OHLCV feathers from ~5 years of history to ~6 months. Root cause:

1. The user ran `make export-freqtrade OVERWRITE=--overwrite` to fix a legacy 16-column funding schema.
2. The flag also applied to OHLCV exports, replacing 2021-2026 feathers with whatever the candle parquet contained at that moment (which, for Chainlink markets, was just the GMX-API 6-month window).
3. Default `keep_parquet=False` then deleted the candle parquet, so there was no on-disk recovery — re-running the Chainlink HyperSync backfill was the only path.

---

## Chunk 1: Stop the bleeding (immediate guardrails)

Goal: even *without* splitting the exporter, make the existing `export-freqtrade` command unable to repeat the 2026-05-11 incident.

### Task 1: Change `keep_parquet` default to `True`

The exporter deletes source parquet by default. That's the wrong default for a tool whose job is to *export* (not move) data.

**Files:**
- Modify: `src/gmx_historical_data/freqtrade_exporter.py` (constructor + `export()` signature)
- Modify: `src/gmx_historical_data/cli.py` (the `export-freqtrade` Click command — flip the `--keep` flag semantics)
- Modify: `Makefile` (line 53: `KEEP ?=` → remove the variable; remove `$(KEEP)` passthrough on line 219)

**Context for implementer:**

Today the CLI exposes `--keep` (default false, meaning "delete the parquet"). After this change, *keeping is the default*. We add an explicit `--delete-source` flag for the rare case someone genuinely wants the old behavior. The Makefile `KEEP` variable becomes unnecessary; remove it to avoid stale documentation.

- [ ] **Step 1.1: Write a failing test.**

Create `tests/test_freqtrade_exporter_isolation.py`:

```python
"""Tests for FreqtradeExporter isolation guarantees.

These tests lock in the post-2026-05-11 contract:

- Source parquet is preserved by default.
- Overwrite cannot shrink existing feather history.
- Candle exports don't touch funding files (and vice versa).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd
import polars as pl
import pytest

from gmx_historical_data.freqtrade_exporter import FreqtradeExporter
from gmx_historical_data.storage import ParquetStorage


def _make_candle_df(start: datetime, hours: int) -> pd.DataFrame:
    """Build a minimal OHLCV DataFrame in the on-disk schema."""
    return pd.DataFrame(
        {
            "timestamp": pd.date_range(start=start, periods=hours, freq="1h", tz="UTC"),
            "open": [100.0] * hours,
            "high": [101.0] * hours,
            "low": [99.0] * hours,
            "close": [100.5] * hours,
            "symbol": ["BTC"] * hours,
        }
    )


@pytest.fixture
def populated_data_dir(tmp_path: Path) -> Path:
    """Set up a data dir with one symbol's candle parquet."""
    data_dir = tmp_path / "data"
    storage = ParquetStorage(data_dir)
    df = _make_candle_df(datetime(2026, 1, 1, tzinfo=UTC), hours=24)
    storage.save_candles(df, timeframe="1h", symbol="BTC")
    return data_dir


def test_export_preserves_candle_parquet_by_default(populated_data_dir, tmp_path):
    """The exporter MUST NOT delete the candle parquet by default."""
    exporter = FreqtradeExporter(populated_data_dir, tmp_path / "feathers")
    exporter.export(symbols=["BTC"], timeframes=["1h"])

    candle_parquet = populated_data_dir / "candles" / "arbitrum" / "BTC" / "1h.parquet"
    assert candle_parquet.exists(), (
        "FreqtradeExporter deleted the candle parquet by default — "
        "this is the 2026-05-11 regression path."
    )
```

- [ ] **Step 1.2: Run the test — expect FAIL.**

```bash
export JSON_RPC_ARBITRUM=$ARBITRUM_CHAIN_JSON_RPC
poetry run python -m pytest tests/test_freqtrade_exporter_isolation.py::test_export_preserves_candle_parquet_by_default -v
```

Expected: FAIL (default is still `keep_parquet=False`, so the parquet is deleted).

- [ ] **Step 1.3: Flip the default in the exporter.**

In `src/gmx_historical_data/freqtrade_exporter.py`:

```python
# OLD (line 63)
    keep_parquet: bool = False,
# NEW
    keep_parquet: bool = True,
```

Update the docstring (`:param keep_parquet:`) accordingly: "Default ``True``. If ``False`` the source candle parquet is deleted after a successful feather export (legacy behavior, kept only for explicit migration workflows)."

- [ ] **Step 1.4: Flip the CLI flag.**

In `src/gmx_historical_data/cli.py`, replace the `--keep` flag with `--delete-source` (default `False`). The CLI passes `keep_parquet = not delete_source` to the exporter. Update help text:

```
--delete-source   Delete source candle parquet after writing the feather. 
                  Default is to keep sources — only set this for explicit
                  parquet→feather migrations.
```

- [ ] **Step 1.5: Run the test — expect PASS.**

```bash
poetry run python -m pytest tests/test_freqtrade_exporter_isolation.py::test_export_preserves_candle_parquet_by_default -v
```

- [ ] **Step 1.6: Drop the `KEEP` knob from the Makefile.**

In `Makefile`:
- Delete lines 52-53 (`# Pass --keep ... KEEP ?=`).
- In the `export-freqtrade` target body, drop the `$(KEEP)` argument (currently line 219).

- [ ] **Step 1.7: Show the user the diff and pause for OK.**

Don't proceed to Task 2 until the user confirms the flip is the behavior they want.

### Task 2: Stop `--overwrite` from bypassing the history guard

`_write(overwrite=True)` currently skips `_assert_history_preserved`. That's how the 6-month parquet clobbered the 5-year feather. Make `--overwrite` re-run the merge check; add `--unsafe-overwrite` for the genuine schema-migration use case.

**Files:**
- Modify: `src/gmx_historical_data/freqtrade_exporter.py:328-377` (the `_write` method + `export()` signature)
- Modify: `src/gmx_historical_data/cli.py` (add `--unsafe-overwrite`)

- [ ] **Step 2.1: Write a failing test.**

Append to `tests/test_freqtrade_exporter_isolation.py`:

```python
def test_overwrite_cannot_shrink_history(populated_data_dir, tmp_path):
    """`--overwrite` MUST still run the history-preservation guard.

    Replicates the 2026-05-11 root cause: a feather with 365 days of history,
    a candle parquet with only the most recent 30, and an overwrite call. The
    full-fix behavior is to refuse the write rather than silently truncate.
    """
    feather_dir = tmp_path / "feathers"
    exporter = FreqtradeExporter(populated_data_dir, feather_dir)

    # Step 1: full history goes into the feather (365 hours).
    storage = ParquetStorage(populated_data_dir)
    long_df = _make_candle_df(datetime(2025, 1, 1, tzinfo=UTC), hours=365 * 24)
    storage.save_candles(long_df, timeframe="1h", symbol="BTC", overwrite=True)
    exporter.export(symbols=["BTC"], timeframes=["1h"])

    feather_path = feather_dir / "gmx" / "futures" / "BTC_USDC_USDC-1h-futures.feather"
    assert feather_path.exists()
    earliest_before = pl.read_ipc(feather_path)["date"].min()

    # Step 2: shrink the parquet to the last 30 hours, then export with overwrite.
    short_df = _make_candle_df(datetime(2025, 12, 31, tzinfo=UTC), hours=30)
    storage.save_candles(short_df, timeframe="1h", symbol="BTC", overwrite=True)

    with pytest.raises(ValueError, match="would shorten history"):
        exporter.export(symbols=["BTC"], timeframes=["1h"], overwrite=True)

    # Step 3: feather is untouched.
    earliest_after = pl.read_ipc(feather_path)["date"].min()
    assert earliest_after == earliest_before, (
        "overwrite=True silently truncated history — this is the 2026-05-11 bug."
    )


def test_unsafe_overwrite_allows_shrink(populated_data_dir, tmp_path):
    """`unsafe_overwrite=True` MUST bypass the guard (for schema migrations)."""
    feather_dir = tmp_path / "feathers"
    exporter = FreqtradeExporter(populated_data_dir, feather_dir)

    storage = ParquetStorage(populated_data_dir)
    long_df = _make_candle_df(datetime(2025, 1, 1, tzinfo=UTC), hours=365 * 24)
    storage.save_candles(long_df, timeframe="1h", symbol="BTC", overwrite=True)
    exporter.export(symbols=["BTC"], timeframes=["1h"])

    short_df = _make_candle_df(datetime(2025, 12, 31, tzinfo=UTC), hours=30)
    storage.save_candles(short_df, timeframe="1h", symbol="BTC", overwrite=True)

    # No exception — explicit opt-in.
    exporter.export(symbols=["BTC"], timeframes=["1h"], unsafe_overwrite=True)

    feather_path = feather_dir / "gmx" / "futures" / "BTC_USDC_USDC-1h-futures.feather"
    assert pl.read_ipc(feather_path).height == 30
```

- [ ] **Step 2.2: Run the tests — expect FAIL.**

```bash
poetry run python -m pytest tests/test_freqtrade_exporter_isolation.py -v
```

Expected: 2 failures (`overwrite=True` currently bypasses the guard; `unsafe_overwrite` doesn't exist).

- [ ] **Step 2.3: Update `_write` to enforce the guard on overwrite.**

In `src/gmx_historical_data/freqtrade_exporter.py`:

```python
def _write(
    self,
    df: pl.DataFrame,
    path: Path,
    fmt: str,
    overwrite: bool = False,
    unsafe_overwrite: bool = False,
) -> None:
    """Merge-write a dataframe to a feather/parquet file.

    Behaviour matrix:

    +----------------+---------------------+------------------------------------+
    | Flags          | Existing file?      | Effect                             |
    +================+=====================+====================================+
    | default        | yes                 | merge, history guard ON            |
    +----------------+---------------------+------------------------------------+
    | overwrite      | yes                 | merge, history guard ON            |
    +----------------+---------------------+------------------------------------+
    | unsafe_overwrt | yes                 | replace entirely, no guard         |
    +----------------+---------------------+------------------------------------+
    | any            | no                  | write new file                     |
    +----------------+---------------------+------------------------------------+

    The `overwrite` flag exists for backward CLI compatibility but is now a
    near no-op: it still runs the history check.  Use `unsafe_overwrite=True`
    when you genuinely need to discard history (schema migration).

    :param df: New Polars dataframe.
    :param path: Output file path.
    :param fmt: ``'feather'`` or ``'parquet'``.
    :param overwrite: Reserved for CLI compatibility; equivalent to default.
    :param unsafe_overwrite: If ``True``, bypass the history guard and
        replace the file entirely.
    :raises ValueError: If the merge would shrink history (and
        ``unsafe_overwrite`` is not set).
    """
    if unsafe_overwrite or not path.exists():
        if fmt == "feather":
            df.write_ipc(path)
        else:
            df.write_parquet(str(path))
        return

    # Both default and `overwrite=True` paths run the merge + guard.
    file_size = path.stat().st_size
    existing = pl.read_ipc(path) if fmt == "feather" else pl.read_parquet(path)
    existing_stats = _coverage_stats(existing, ts_col="date")
    incoming_stats = _coverage_stats(df, ts_col="date")
    merged = (
        pl.concat([existing, df])
        .unique(subset=["date"], keep="last", maintain_order=False)
        .sort("date")
    )
    merged_stats = _coverage_stats(merged, ts_col="date")
    _assert_history_preserved(
        existing_stats, incoming_stats, merged_stats,
        ts_label="date", location=str(path),
    )
    logger.debug(
        "Merged %s: existing=%d rows (%.1f KB), new=%d rows, merged=%d rows",
        path, existing_stats["rows"], file_size / 1024,
        incoming_stats["rows"], merged_stats["rows"],
    )

    if fmt == "feather":
        merged.write_ipc(path)
    else:
        merged.write_parquet(str(path))
```

Also thread `unsafe_overwrite` through `export()` and its callsites (lines 138, 152, 164, 181).

- [ ] **Step 2.4: Add `--unsafe-overwrite` to the CLI.**

In `src/gmx_historical_data/cli.py`, the `export-freqtrade` command:

```python
@click.option(
    "--unsafe-overwrite",
    is_flag=True,
    default=False,
    help=(
        "Replace existing feathers entirely, bypassing the history-preservation "
        "guard. Only set this for schema migrations where you intentionally "
        "discard old data. The 2026-05-11 incident happened because --overwrite "
        "had this behavior by default — it no longer does."
    ),
)
```

`--overwrite` stays for backward compat but no longer bypasses the guard.

- [ ] **Step 2.5: Run the tests — expect PASS.**

```bash
poetry run python -m pytest tests/test_freqtrade_exporter_isolation.py -v
```

- [ ] **Step 2.6: Show user the diff. Confirm before continuing.**

---

## Chunk 2: Isolate the export paths by data type

Goal: `make export-candles` cannot touch funding feathers; `make export-funding` cannot touch OHLCV feathers. OI and pool-liquidity already don't share output paths — we just lock that in with regression tests.

### Task 3: Split `FreqtradeExporter.export()` into two methods

**Files:**
- Modify: `src/gmx_historical_data/freqtrade_exporter.py` (split `export()` into `export_candles()` + `export_funding()`)

**Context:** Today's `export()` is one symbol×timeframe loop that writes 4 file kinds in sequence. We split into two independent passes. Each pass touches only its own file kinds and its own source directory. The thin `export()` wrapper stays for backward compat and calls both.

- [ ] **Step 3.1: Write failing tests.**

Append to `tests/test_freqtrade_exporter_isolation.py`:

```python
@pytest.fixture
def data_dir_with_funding(populated_data_dir, tmp_path) -> Path:
    """Populated data dir + a funding parquet for BTC."""
    funding_dir = populated_data_dir / "funding" / "arbitrum" / "rates" / "BTC"
    funding_dir.mkdir(parents=True, exist_ok=True)
    funding_df = pl.DataFrame(
        {
            "timestamp": pl.datetime_range(
                datetime(2026, 1, 1, tzinfo=UTC),
                datetime(2026, 1, 1, 23, tzinfo=UTC),
                interval="1h",
                time_zone="UTC",
                eager=True,
            ),
            "funding_rate": [1e-9] * 24,
            "funding_rate_hourly": [3.6e-6] * 24,
        }
    )
    funding_df.write_parquet(funding_dir / "1h.parquet")
    return populated_data_dir


def test_export_candles_does_not_touch_funding(data_dir_with_funding, tmp_path):
    """`export_candles` must not create or modify funding_rate feathers."""
    feather_dir = tmp_path / "feathers"
    exporter = FreqtradeExporter(data_dir_with_funding, feather_dir)
    exporter.export_candles(symbols=["BTC"], timeframes=["1h"])

    gmx_dir = feather_dir / "gmx" / "futures"
    assert (gmx_dir / "BTC_USDC_USDC-1h-futures.feather").exists()
    assert (gmx_dir / "BTC_USDC_USDC-1h-mark.feather").exists()
    assert (gmx_dir / "BTC_USDC_USDC-1h-index.feather").exists()
    assert not (gmx_dir / "BTC_USDC_USDC-1h-funding_rate.feather").exists(), (
        "export_candles created a funding_rate feather — paths not isolated."
    )


def test_export_funding_does_not_touch_candles(data_dir_with_funding, tmp_path):
    """`export_funding` must not create or modify -futures/-mark/-index feathers."""
    feather_dir = tmp_path / "feathers"
    exporter = FreqtradeExporter(data_dir_with_funding, feather_dir)
    exporter.export_funding(symbols=["BTC"], timeframes=["1h"])

    gmx_dir = feather_dir / "gmx" / "futures"
    assert (gmx_dir / "BTC_USDC_USDC-1h-funding_rate.feather").exists()
    for forbidden in (
        "BTC_USDC_USDC-1h-futures.feather",
        "BTC_USDC_USDC-1h-mark.feather",
        "BTC_USDC_USDC-1h-index.feather",
    ):
        assert not (gmx_dir / forbidden).exists(), (
            f"export_funding created {forbidden} — paths not isolated."
        )
```

- [ ] **Step 3.2: Run tests — expect FAIL** (methods don't exist).

```bash
poetry run python -m pytest tests/test_freqtrade_exporter_isolation.py -v -k "isolated"
```

- [ ] **Step 3.3: Refactor `FreqtradeExporter`.**

Extract the existing `export()` body into two pure-data-type methods:

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
) -> dict[str, dict]:
    """Export OHLCV + mark + index feathers only.  Touches only ``candles/``."""
    # Walk candle_symbols only.  Write -futures, -mark, -index.
    # NEVER reads from self.funding_dir.
    # NEVER writes a -funding_rate.feather.
    ...

def export_funding(
    self,
    symbols: list[str] | None = None,
    timeframes: list[str] | None = None,
    output_format: str = "feather",
    trading_mode: str = "futures",
    quote_currency: str = "USDC",
    overwrite: bool = False,
    unsafe_overwrite: bool = False,
) -> dict[str, dict]:
    """Export funding_rate feathers only.  Touches only ``funding/``."""
    # Walk funding_symbols only.  Write -funding_rate only.
    # NEVER reads from self.storage (candles).
    # NEVER writes -futures / -mark / -index.
    ...

def export(self, **kwargs) -> dict[str, dict]:
    """Backward-compat wrapper: runs candles then funding.

    .. deprecated:: 2026-05-11
        Prefer ``export_candles()`` + ``export_funding()`` so callers can
        run them independently.  This wrapper calls both in sequence.
    """
    candles = self.export_candles(**kwargs)
    funding = self.export_funding(**{k: v for k, v in kwargs.items() if k != "keep_parquet"})
    # Merge stats dicts ...
    return merged_stats
```

Note: `export_funding` has no `keep_parquet` parameter — funding parquet is owned by the unified-funding pipeline, never deleted by the exporter.

- [ ] **Step 3.4: Move source-parquet cleanup into `export_candles` only.**

The existing `_cleanup_source_parquet` (line 317) only deletes the candle path. Rename it to `_cleanup_candle_source` for clarity. `export_funding` must not have any cleanup logic at all (zero risk of touching the candle directory).

- [ ] **Step 3.5: Run tests — expect PASS.**

```bash
poetry run python -m pytest tests/test_freqtrade_exporter_isolation.py -v
```

- [ ] **Step 3.6: Run the full suite — confirm no regressions.**

```bash
export JSON_RPC_ARBITRUM=$ARBITRUM_CHAIN_JSON_RPC
poetry run python -m pytest tests/ -q \
  --ignore=tests/test_hybrid_collection.py \
  --ignore=tests/test_gmx_event_collector.py \
  --ignore=tests/test_integration_event_based.py \
  --ignore=tests/test_integration_gmx_first.py \
  --ignore=tests/test_live_funding.py \
  --ignore=tests/test_gmx_market_mapper.py \
  --ignore=tests/test_gmx_token_discovery.py 2>&1 | tail -5
```

Expected: prior pass count + ~4 new isolation tests.

### Task 4: Add CLI subcommands and Makefile targets

**Files:**
- Modify: `src/gmx_historical_data/cli.py` (add `export-candles` + `export-funding` Click commands)
- Modify: `Makefile` (add `export-candles`, `export-funding` targets; rewire `refresh-data` / `full-data` to use them)

- [ ] **Step 4.1: Add CLI subcommands.**

In `src/gmx_historical_data/cli.py`:

```python
@cli.command("export-candles")
@click.option("--data-dir", type=click.Path(path_type=Path), default=Path("data"))
@click.option("--output-dir", type=click.Path(path_type=Path), default=Path("freqtrade_data"))
@click.option("--symbol", "symbols", multiple=True, help="Specific symbols (repeatable).")
@click.option("--timeframe", "timeframes", multiple=True)
@click.option("--overwrite", is_flag=True, default=False,
              help="Merge-with-guard (history-preserving).  Bug-compat alias.")
@click.option("--unsafe-overwrite", is_flag=True, default=False,
              help="Replace files entirely, bypassing the history guard.  "
                   "Schema-migration use only.")
@click.option("--delete-source", is_flag=True, default=False,
              help="Delete the source candle parquet after a successful "
                   "feather export.  Default keeps the source.")
def export_candles_cli(...):
    """Export GMX OHLCV (candles + mark + index) feathers only.  Funding untouched."""
    exporter = FreqtradeExporter(data_dir, output_dir)
    exporter.export_candles(
        symbols=list(symbols) or None,
        timeframes=list(timeframes) or None,
        overwrite=overwrite,
        unsafe_overwrite=unsafe_overwrite,
        keep_parquet=not delete_source,
    )


@cli.command("export-funding")
@click.option("--data-dir", type=click.Path(path_type=Path), default=Path("data"))
@click.option("--output-dir", type=click.Path(path_type=Path), default=Path("freqtrade_data"))
@click.option("--symbol", "symbols", multiple=True)
@click.option("--timeframe", "timeframes", multiple=True)
@click.option("--overwrite", is_flag=True, default=False)
@click.option("--unsafe-overwrite", is_flag=True, default=False)
def export_funding_cli(...):
    """Export GMX funding_rate feathers only.  OHLCV untouched."""
    exporter = FreqtradeExporter(data_dir, output_dir)
    exporter.export_funding(
        symbols=list(symbols) or None,
        timeframes=list(timeframes) or None,
        overwrite=overwrite,
        unsafe_overwrite=unsafe_overwrite,
    )
```

`export-freqtrade` stays as a thin wrapper that runs both, kept for backward compat.

- [ ] **Step 4.2: Add Makefile targets.**

In `Makefile`, after the existing `export-freqtrade` target:

```makefile
# Export OHLCV (candles + mark + index) feathers only.
# Does NOT touch funding feathers.
export-candles:
	@echo "Exporting OHLCV candles to FreqTrade format..."
	@echo "  Data:       $(DATA_DIR)"
	@echo "  Output:     $(FEATHER_DIR)"
	@echo ""
	@mkdir -p "$(FEATHER_DIR)"
	$(NICE) poetry run python -m gmx_historical_data.cli export-candles \
		--data-dir "$(DATA_DIR)" \
		--output-dir "$(FEATHER_DIR)" \
		$(if $(SYMBOL),--symbol $(SYMBOL),) \
		$(if $(UNSAFE_OVERWRITE),--unsafe-overwrite,) \
		$(if $(DELETE_SOURCE),--delete-source,)

# Export funding_rate feathers only.
# Does NOT touch OHLCV feathers.
export-funding:
	@echo "Exporting funding rates to FreqTrade format..."
	@echo "  Data:       $(DATA_DIR)"
	@echo "  Output:     $(FEATHER_DIR)"
	@echo ""
	@mkdir -p "$(FEATHER_DIR)"
	$(NICE) poetry run python -m gmx_historical_data.cli export-funding \
		--data-dir "$(DATA_DIR)" \
		--output-dir "$(FEATHER_DIR)" \
		$(if $(SYMBOL),--symbol $(SYMBOL),) \
		$(if $(UNSAFE_OVERWRITE),--unsafe-overwrite,)
```

Add to `.PHONY` (line 71): `export-candles export-funding`.

- [ ] **Step 4.3: Update `refresh-data` / `full-data` chains.**

Replace `export-freqtrade` in the orchestrator targets:

```makefile
refresh-data: collect-update funding-unified-resume extract-all-resume export-candles export-funding
	@echo "Incremental refresh complete: candles + funding + OI + liquidity + isolated exports"

full-data: collect-full funding-unified extract-all export-candles export-funding
	@echo "Full data download complete: candles + funding + OI + liquidity + isolated exports"

full-data-nn: collect-full-nn funding-unified extract-all export-candles export-funding
	@echo "Full data download complete (no-nice): candles + funding + OI + liquidity + isolated exports"
```

(And the `-cex` variants on lines 453-463 similarly.)

- [ ] **Step 4.4: Verify the Makefile.**

```bash
make -n export-candles SYMBOL=BTC
make -n export-funding SYMBOL=BTC
make -n refresh-data
```

Expected: no syntax errors; commands echo what they would run; `refresh-data` shows the new isolated export pair.

- [ ] **Step 4.5: Show user the diff. Confirm before continuing.**

### Task 5: Lock in OI / pool-liquidity isolation with regression tests

OI and pool-liquidity already write only to their own parquet directories. Add tests that fail if a future change ever makes them write into `candles/` or `funding/`.

**Files:**
- Create: `tests/test_pipeline_isolation_paths.py`

- [ ] **Step 5.1: Write the regression test.**

```python
"""Static analysis: each extractor script must only write to its own output dir.

These tests grep the extractor source for filesystem writes that escape the
expected directory.  They're cheap to run and will catch a future PR that
accidentally couples one pipeline to another.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]


def _read(path: str) -> str:
    return (REPO / path).read_text()


def test_oi_extractor_does_not_write_outside_oi_dir():
    src = _read("scripts/extract_open_interest.py")
    # The script writes via OUTPUT_DIR / ... .  It must not reference candles/
    # or funding/ or futures/ paths.
    assert "candles" not in src.lower() or "# allow:" in src, (
        "extract_open_interest.py references 'candles' — pipeline isolation broken."
    )
    forbidden = ("/candles/", "/funding/", "gmx/futures/")
    for needle in forbidden:
        assert needle not in src, (
            f"extract_open_interest.py writes to '{needle}' — must stay in OI dir."
        )


def test_pool_liquidity_does_not_write_outside_its_dir():
    src = _read("scripts/extract_pool_liquidity.py")
    forbidden = ("/candles/", "/funding/", "gmx/futures/")
    for needle in forbidden:
        assert needle not in src, (
            f"extract_pool_liquidity.py writes to '{needle}' — must stay in pool dir."
        )


def test_unified_funding_does_not_write_to_candles():
    src = _read("scripts/extract_unified_funding.py")
    # The script may legitimately write feather to gmx/futures/, but ONLY
    # -funding_rate.feather files.
    candle_writes = re.findall(r'\.write_(parquet|ipc)\(.*candles', src)
    assert not candle_writes, (
        "extract_unified_funding.py writes into a candles/ path — broken isolation."
    )
```

- [ ] **Step 5.2: Run the test — expect PASS.**

```bash
poetry run python -m pytest tests/test_pipeline_isolation_paths.py -v
```

(If it fails: there's a real isolation violation in the current codebase that this plan didn't anticipate. Pause and investigate before continuing.)

- [ ] **Step 5.3: Show user the test result. Confirm before continuing.**

---

## Chunk 3: Documentation + final verification

### Task 6: Update Makefile help text

**Files:**
- Modify: `Makefile` (the `help` target body, lines 80-122)

- [ ] **Step 6.1: Replace the export-freqtrade help line.**

```
@echo "  export-freqtrade     [DEPRECATED] Use export-candles + export-funding"
@echo "  export-candles       Export OHLCV (candles+mark+index) feathers ONLY"
@echo "  export-funding       Export funding_rate feathers ONLY"
```

- [ ] **Step 6.2: Add the data-isolation note.**

Append a new section to `help`:

```
@echo ""
@echo "Data isolation guarantees (post 2026-05-11):"
@echo "  - 'export-candles' touches only OHLCV feathers; funding is left alone."
@echo "  - 'export-funding' touches only funding feathers; OHLCV is left alone."
@echo "  - 'oi', 'pool-liquidity' write only to their own parquet directories."
@echo "  - Source parquet is preserved by default — use DELETE_SOURCE=1 to remove."
@echo "  - --overwrite no longer bypasses the history guard; use --unsafe-overwrite"
@echo "    explicitly for schema migrations."
```

### Task 7: End-to-end smoke test on real data

- [ ] **Step 7.1: Record current feather mtimes.**

```bash
ls -lt "/Volumes/WD Blue 1tb/VMs/data/gmx/futures/" | head -20 > /tmp/feather_mtimes_before.txt
```

- [ ] **Step 7.2: Run `make export-funding` and check no candle feathers changed.**

```bash
export JSON_RPC_ARBITRUM=$ARBITRUM_CHAIN_JSON_RPC
make export-funding SYMBOL=BTC

# Verify: only the BTC funding_rate feather changed mtime.
find "/Volumes/WD Blue 1tb/VMs/data/gmx/futures/" \
  -name "BTC_USDC_USDC-1h-*" -newer /tmp/feather_mtimes_before.txt
```

Expected: only `BTC_USDC_USDC-1h-funding_rate.feather` shows up.

- [ ] **Step 7.3: Run `make export-candles` and check no funding feathers changed.**

```bash
ls -lt "/Volumes/WD Blue 1tb/VMs/data/gmx/futures/" | head -20 > /tmp/feather_mtimes_step3.txt
make export-candles SYMBOL=AVAX  # use a symbol that won't trigger Chainlink work
find "/Volumes/WD Blue 1tb/VMs/data/gmx/futures/" \
  -name "AVAX_USDC_USDC-1h-funding_rate.feather" -newer /tmp/feather_mtimes_step3.txt
```

Expected: no output (the funding feather is untouched).

- [ ] **Step 7.4: Show the user the smoke-test output. Get OK to commit.**

### Task 8: Commit and document

- [ ] **Step 8.1: Show the user the full diff.**

```bash
git status
git diff --stat
```

- [ ] **Step 8.2: Wait for explicit user OK.**

Do NOT commit autonomously. The user must review.

- [ ] **Step 8.3: Suggested commit message (single commit).**

```
feat(pipelines): isolate OHLCV/funding/OI exports + tighten safety defaults

Three coupled changes that together prevent a repeat of the 2026-05-11
incident, where `make export-freqtrade OVERWRITE=--overwrite` (intended
for a funding schema migration) silently truncated the OHLCV feathers
from ~5 years to ~6 months for all Chainlink markets.

1. Split FreqtradeExporter.export() into export_candles() and
   export_funding().  Each method writes only its own file kinds and
   reads only from its own source directory.  No cross-pipeline writes
   are possible.  CLI gains `export-candles` and `export-funding`
   subcommands; legacy `export-freqtrade` still runs both for
   backward compat.

2. Flip safety defaults:
   - `keep_parquet` is now True by default.  Use --delete-source to
     opt back into the old delete-after-export behavior.
   - `--overwrite` no longer bypasses the history-preservation guard.
     A merge that would shrink existing history raises ValueError.
     Use `--unsafe-overwrite` for genuine schema migrations.

3. Lock isolation in with static-analysis tests
   (tests/test_pipeline_isolation_paths.py): a future PR that makes
   the OI or pool-liquidity extractor write into candles/ or funding/
   will fail CI.

Makefile orchestrators (refresh-data, full-data, *-cex variants)
switch from export-freqtrade to the explicit pair.

Spec / context: docs/superpowers/plans/2026-05-11-data-pipeline-isolation.md
```

---

## Open questions for the user

These are baked-in choices in the plan that the user may want to override:

1. **Should `export-freqtrade` (singular) be removed entirely, or kept as a wrapper?**
   Current plan keeps it as a wrapper for backward compat. Removing it would force every caller to upgrade.

2. **Should `--overwrite` be removed entirely, or kept as a no-op?**
   Current plan keeps it as a near-no-op (alias for default). Removing it would break existing scripts.

3. **OI/pool-liquidity have no feather export today.** Should they get one in this plan, or is that out of scope?
   Current plan: out of scope (only adds regression tests). If you want OI feathers exported, that's a separate plan.

4. **`unsafe_overwrite` name.** Alternatives: `--force-replace`, `--truncate-history`. Current name is intentionally scary.
