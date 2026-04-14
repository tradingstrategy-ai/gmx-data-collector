# History-Preserving Quickstart and Export Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make full collection and Freqtrade export preserve full on-chain history when quick seed is used, fail fast on truncation risk, and add `--keep` / `-k` plus Makefile passthrough for retaining parquet after export.

**Architecture:** Add a shared history-preservation invariant at the storage and export write boundaries so merge failures and coverage regressions raise instead of silently overwriting old data. Extend the export command with explicit parquet cleanup control and thread the new flag through `Makefile` full-collection workflows so quick-seeded API tail data can coexist safely with older on-chain history.

**Tech Stack:** Python 3.11+, Typer CLI, Pandas, Polars, PyArrow, pytest, GNU Make

---

## File Structure

**Files:**
- Modify: `src/gmx_historical_data/storage.py`
- Modify: `src/gmx_historical_data/freqtrade_exporter.py`
- Modify: `src/gmx_historical_data/cli.py`
- Modify: `Makefile`
- Modify: `tests/test_storage_list.py`
- Modify: `tests/test_freqtrade_exporter.py`
- Create: `tests/test_export_cleanup.py`
- Reference: `docs/superpowers/specs/2026-04-14-history-preserving-quickseed-export-design.md`

**Responsibilities:**
- `storage.py`: enforce non-destructive merge invariants for candle parquet writes.
- `freqtrade_exporter.py`: enforce the same invariants for feather/parquet export writes and implement optional parquet cleanup.
- `cli.py`: expose `--keep` / `-k` on `export-freqtrade` and pass cleanup intent into the exporter.
- `Makefile`: allow `collect-full`, `collect-full-nn`, `full-data`, and `full-data-nn` flows to pass the keep option through.
- `tests/test_storage_list.py`: prove candle parquet writes preserve earliest history and fail hard on unsafe merge paths.
- `tests/test_freqtrade_exporter.py`: prove exported feather files preserve earliest history and fail hard instead of overwriting on merge errors.
- `tests/test_export_cleanup.py`: verify default cleanup removes parquet after feather export and `--keep` preserves it.

### Task 1: Harden Parquet Candle Writes

**Files:**
- Modify: `src/gmx_historical_data/storage.py`
- Test: `tests/test_storage_list.py`

- [ ] **Step 1: Write the failing storage safety tests**

```python
def test_save_candles_raises_when_existing_merge_cannot_be_read(monkeypatch):
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = ParquetStorage(Path(tmpdir))

        historic = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(["2022-01-01", "2022-01-02"], utc=True),
                "open": [100.0, 101.0],
                "high": [105.0, 106.0],
                "low": [99.0, 100.0],
                "close": [104.0, 105.0],
                "symbol": ["BTC", "BTC"],
            }
        )
        storage.save_candles(historic, "1d", "BTC")

        incoming = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(["2022-01-03"], utc=True),
                "open": [102.0],
                "high": [107.0],
                "low": [101.0],
                "close": [106.0],
                "symbol": ["BTC"],
            }
        )

        def boom(*_args, **_kwargs):
            raise RuntimeError("cannot read existing parquet")

        monkeypatch.setattr("gmx_historical_data.storage.pl.read_parquet", boom)

        with pytest.raises(RuntimeError, match="cannot read existing parquet"):
            storage.save_candles(incoming, "1d", "BTC")

        result = storage.read_candles("1d", "BTC")
        assert len(result) == 2
        assert result["timestamp"].min() == pd.Timestamp("2022-01-01", tz="UTC")


def test_save_candles_raises_when_merge_would_shorten_history(monkeypatch):
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = ParquetStorage(Path(tmpdir))

        historic = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(["2022-01-01", "2022-01-02", "2022-01-03"], utc=True),
                "open": [100.0, 101.0, 102.0],
                "high": [105.0, 106.0, 107.0],
                "low": [99.0, 100.0, 101.0],
                "close": [104.0, 105.0, 106.0],
                "symbol": ["BTC", "BTC", "BTC"],
            }
        )
        storage.save_candles(historic, "1d", "BTC")

        recent_only = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(["2022-01-03", "2022-01-04"], utc=True),
                "open": [200.0, 201.0],
                "high": [205.0, 206.0],
                "low": [199.0, 200.0],
                "close": [204.0, 205.0],
                "symbol": ["BTC", "BTC"],
            }
        )

        original_from_pandas = pl.from_pandas

        def truncate_incoming(df):
            frame = original_from_pandas(df)
            return frame.filter(pl.col("timestamp") >= pl.datetime(2022, 1, 3, time_unit="us"))

        monkeypatch.setattr("gmx_historical_data.storage.pl.from_pandas", truncate_incoming)

        with pytest.raises(ValueError, match="would shorten history"):
            storage.save_candles(recent_only, "1d", "BTC")
```

- [ ] **Step 2: Run the targeted storage tests to verify they fail**

Run: `pytest tests/test_storage_list.py -k "save_candles" -v`
Expected: FAIL because `save_candles()` currently logs merge failures and can still write incoming-only data without enforcing earliest-history invariants.

- [ ] **Step 3: Add explicit coverage validation helpers in `storage.py`**

```python
def _coverage_stats(df: pl.DataFrame, ts_col: str) -> dict[str, object]:
    if df.is_empty():
        return {"rows": 0, "earliest": None, "latest": None}
    return {
        "rows": df.height,
        "earliest": df.select(pl.col(ts_col).min()).item(),
        "latest": df.select(pl.col(ts_col).max()).item(),
    }


def _assert_history_preserved(
    existing_stats: dict[str, object],
    incoming_stats: dict[str, object],
    merged_stats: dict[str, object],
    *,
    ts_label: str,
    location: str,
) -> None:
    if existing_stats["rows"] == 0:
        return

    if merged_stats["earliest"] is None or merged_stats["earliest"] > existing_stats["earliest"]:
        raise ValueError(
            f"{location}: merge would shorten history for {ts_label}: "
            f"existing earliest={existing_stats['earliest']}, merged earliest={merged_stats['earliest']}"
        )

    expected_latest = max(
        ts for ts in (existing_stats["latest"], incoming_stats["latest"]) if ts is not None
    )
    if merged_stats["latest"] is None or merged_stats["latest"] < expected_latest:
        raise ValueError(
            f"{location}: merge would lose tail coverage for {ts_label}: "
            f"expected latest={expected_latest}, merged latest={merged_stats['latest']}"
        )
```

- [ ] **Step 4: Make `save_candles()` fail hard instead of writing incoming-only data**

```python
incoming = pl.from_pandas(df).with_columns(
    pl.col("timestamp").cast(pl.Datetime("us", "UTC"))
)
incoming_stats = _coverage_stats(incoming, "timestamp")

if not overwrite and output_path.exists():
    existing = pl.read_parquet(output_path).with_columns(
        pl.col("timestamp").cast(pl.Datetime("us", "UTC"))
    )
    existing_stats = _coverage_stats(existing, "timestamp")
    merged = (
        pl.concat([existing, incoming])
        .unique(subset=["timestamp"], keep="last", maintain_order=True)
        .sort("timestamp")
    )
    merged_stats = _coverage_stats(merged, "timestamp")
    _assert_history_preserved(
        existing_stats,
        incoming_stats,
        merged_stats,
        ts_label="timestamp",
        location=f"save_candles({symbol}/{timeframe})",
    )
    incoming = merged
```

- [ ] **Step 5: Re-run the storage tests**

Run: `pytest tests/test_storage_list.py -k "save_candles" -v`
Expected: PASS with merge-by-default behavior preserved, `overwrite=True` still replacing data intentionally, and new safety tests raising before any truncating write.

- [ ] **Step 6: Commit the storage safety change**

```bash
git add src/gmx_historical_data/storage.py tests/test_storage_list.py
git commit -m "fix: fail fast on candle history truncation"
```

### Task 2: Harden Freqtrade Export Writes and Add Cleanup Control

**Files:**
- Modify: `src/gmx_historical_data/freqtrade_exporter.py`
- Modify: `src/gmx_historical_data/cli.py`
- Modify: `tests/test_freqtrade_exporter.py`
- Create: `tests/test_export_cleanup.py`

- [ ] **Step 1: Write failing exporter safety and cleanup tests**

```python
def test_export_preserves_existing_longer_history(sample_storage):
    with tempfile.TemporaryDirectory() as output_dir:
        futures_dir = Path(output_dir) / "gmx" / "futures"
        futures_dir.mkdir(parents=True, exist_ok=True)

        existing = pd.DataFrame(
            {
                "date": pd.to_datetime(["2022-01-01", "2022-01-02", "2022-01-03"], utc=True),
                "open": [1.0, 2.0, 3.0],
                "high": [1.0, 2.0, 3.0],
                "low": [1.0, 2.0, 3.0],
                "close": [1.0, 2.0, 3.0],
                "volume": [0.0, 0.0, 0.0],
            }
        )
        existing.to_feather(futures_dir / "ETH_USDC_USDC-1h-futures.feather")

        exporter = FreqtradeExporter(sample_storage, Path(output_dir))
        exporter.export(symbols=["ETH"], timeframes=["1h"])

        result = pd.read_feather(futures_dir / "ETH_USDC_USDC-1h-futures.feather")
        assert result["date"].min() == pd.Timestamp("2022-01-01", tz="UTC")


def test_export_raises_when_existing_merge_fails(sample_storage, monkeypatch):
    with tempfile.TemporaryDirectory() as output_dir:
        exporter = FreqtradeExporter(sample_storage, Path(output_dir))
        exporter.export(symbols=["ETH"], timeframes=["1h"])

        def boom(*_args, **_kwargs):
            raise RuntimeError("cannot read existing feather")

        monkeypatch.setattr("gmx_historical_data.freqtrade_exporter.pl.read_ipc", boom)

        with pytest.raises(RuntimeError, match="cannot read existing feather"):
            exporter.export(symbols=["ETH"], timeframes=["1h"])


def test_export_cleanup_removes_parquet_by_default(tmp_path):
    data_dir = tmp_path / "user_data"
    output_dir = tmp_path / "user_data"
    storage = ParquetStorage(data_dir)

    df = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2024-01-01 00:00:00"], utc=True),
            "open": [1.0],
            "high": [1.0],
            "low": [1.0],
            "close": [1.0],
            "symbol": ["ETH"],
        }
    )
    storage.save_candles(df, "1h", "ETH")

    exporter = FreqtradeExporter(data_dir, output_dir)
    exporter.export(symbols=["ETH"], timeframes=["1h"], keep_parquet=False)

    assert not (data_dir / "candles" / "arbitrum" / "ETH" / "1h.parquet").exists()


def test_export_cleanup_kept_when_requested(tmp_path):
    data_dir = tmp_path / "user_data"
    output_dir = tmp_path / "user_data"
    storage = ParquetStorage(data_dir)

    df = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2024-01-01 00:00:00"], utc=True),
            "open": [1.0],
            "high": [1.0],
            "low": [1.0],
            "close": [1.0],
            "symbol": ["ETH"],
        }
    )
    storage.save_candles(df, "1h", "ETH")

    exporter = FreqtradeExporter(data_dir, output_dir)
    exporter.export(symbols=["ETH"], timeframes=["1h"], keep_parquet=True)

    assert (data_dir / "candles" / "arbitrum" / "ETH" / "1h.parquet").exists()
```

- [ ] **Step 2: Run the exporter tests to verify they fail**

Run: `pytest tests/test_freqtrade_exporter.py tests/test_export_cleanup.py -v`
Expected: FAIL because exporter merge failures are currently non-fatal and there is no `keep_parquet` cleanup control yet.

- [ ] **Step 3: Add shared export-side coverage validation and fatal merge behavior**

```python
def _coverage_stats(self, df: pl.DataFrame, ts_col: str = "date") -> dict[str, object]:
    if df.is_empty():
        return {"rows": 0, "earliest": None, "latest": None}
    return {
        "rows": df.height,
        "earliest": df.select(pl.col(ts_col).min()).item(),
        "latest": df.select(pl.col(ts_col).max()).item(),
    }


def _assert_history_preserved(self, existing_stats, incoming_stats, merged_stats, path: Path) -> None:
    if existing_stats["rows"] == 0:
        return
    if merged_stats["earliest"] > existing_stats["earliest"]:
        raise ValueError(
            f"{path}: export merge would shorten history "
            f"({existing_stats['earliest']} -> {merged_stats['earliest']})"
        )
```

```python
if not overwrite and path.exists():
    existing = pl.read_ipc(path) if fmt == "feather" else pl.read_parquet(path)
    existing_stats = self._coverage_stats(existing)
    incoming_stats = self._coverage_stats(df)
    merged = (
        pl.concat([existing, df])
        .unique(subset=["date"], keep="last", maintain_order=False)
        .sort("date")
    )
    merged_stats = self._coverage_stats(merged)
    self._assert_history_preserved(existing_stats, incoming_stats, merged_stats, path)
    df = merged
```

- [ ] **Step 4: Add parquet cleanup support to exporter and CLI**

```python
def export(
    self,
    symbols: list[str] | None = None,
    timeframes: list[str] | None = None,
    output_format: str = "feather",
    trading_mode: str = "futures",
    quote_currency: str = "USDC",
    overwrite: bool = False,
    keep_parquet: bool = False,
) -> dict[str, dict]:
```

```python
keep: bool = typer.Option(
    False,
    "--keep",
    "-k",
    help="Keep candle/funding parquet source files after successful feather export.",
)
```

```python
results = exporter.export(
    symbols=symbols_to_export,
    timeframes=timeframes_to_export,
    output_format=output_format,
    overwrite=overwrite,
    keep_parquet=keep,
)
```

- [ ] **Step 5: Implement cleanup only after successful feather export**

```python
def _cleanup_source_parquet(self, symbol: str, timeframe: str) -> None:
    candle_path = self.data_dir / "candles" / "arbitrum" / symbol / f"{timeframe}.parquet"
    funding_path = self.funding_dir / symbol / f"{timeframe}.parquet"

    for path in (candle_path, funding_path):
        if path.exists():
            path.unlink()


if output_format == "feather" and not keep_parquet:
    self._cleanup_source_parquet(symbol, tf)
```

- [ ] **Step 6: Re-run exporter and cleanup tests**

Run: `pytest tests/test_freqtrade_exporter.py tests/test_export_cleanup.py -v`
Expected: PASS with fatal merge behavior, preserved earliest history, default parquet cleanup after feather export, and `--keep` preserving source parquet files.

- [ ] **Step 7: Commit the exporter and CLI change**

```bash
git add src/gmx_historical_data/freqtrade_exporter.py src/gmx_historical_data/cli.py tests/test_freqtrade_exporter.py tests/test_export_cleanup.py
git commit -m "fix: preserve export history and add keep flag"
```

### Task 3: Wire `--keep` Through Makefile Full Workflows

**Files:**
- Modify: `Makefile`
- Test: `tests/test_export_cleanup.py`

- [ ] **Step 1: Add a failing regression test or assertion for Makefile passthrough expectations**

```python
def test_makefile_export_supports_keep_flag():
    makefile = Path("Makefile").read_text()
    assert "KEEP ?=" in makefile
    assert "$(KEEP)" in makefile
    assert "--keep" in makefile
```

- [ ] **Step 2: Run the focused cleanup test file to verify the Makefile assertion fails**

Run: `pytest tests/test_export_cleanup.py -v`
Expected: FAIL because the Makefile does not yet expose a keep-variable passthrough.

- [ ] **Step 3: Update Makefile variables and targets**

```make
KEEP ?=

collect-full:
	$(call COLLECT_CMD,full historical,full,$(QUICKSTART))

collect-full-nn:
	@echo "Starting full historical candle collection (no-nice, concurrency 10)..."
	$(if $(KEEP),@echo "  Keep parquet: $(KEEP)",)
```

```make
export-freqtrade:
	@echo "Exporting to FreqTrade format..."
	@echo "  Data:       $(DATA_DIR)"
	@echo "  Output:     $(FEATHER_DIR)"
	$(if $(KEEP),@echo "  Keep:       $(KEEP)",)
	@mkdir -p "$(FEATHER_DIR)"
	$(NICE) poetry run python -m gmx_historical_data.cli export-freqtrade \
		--data-dir "$(DATA_DIR)" \
		--output-dir "$(FEATHER_DIR)" \
		$(KEEP)
```

```make
# Example:
# make full-data QUICKSTART=--quickstart KEEP=--keep
```

- [ ] **Step 4: Re-run the cleanup/Makefile tests**

Run: `pytest tests/test_export_cleanup.py -v`
Expected: PASS with Makefile passthrough assertions satisfied.

- [ ] **Step 5: Commit the Makefile passthrough change**

```bash
git add Makefile tests/test_export_cleanup.py
git commit -m "build: expose parquet keep flag in full workflows"
```

### Task 4: Verify End-to-End Safety on the Full Collection Path

**Files:**
- Modify: `tests/test_freqtrade_exporter.py`
- Modify: `tests/test_storage_list.py`
- Reference: `src/gmx_historical_data/cli.py`
- Reference: `src/gmx_historical_data/data_coverage_analyzer.py`

- [ ] **Step 1: Add an integration-style regression test describing seeded recent coverage plus older history**

```python
def test_seeded_recent_data_plus_existing_history_keeps_earliest_timestamp(tmp_path):
    storage = ParquetStorage(tmp_path)

    historic = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2024-04-01", "2024-04-02"], utc=True),
            "open": [10.0, 11.0],
            "high": [10.0, 11.0],
            "low": [10.0, 11.0],
            "close": [10.0, 11.0],
            "symbol": ["TOKEN", "TOKEN"],
        }
    )
    storage.save_candles(historic, "1d", "TOKEN")

    seeded_recent = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2025-01-01", "2025-01-02"], utc=True),
            "open": [20.0, 21.0],
            "high": [20.0, 21.0],
            "low": [20.0, 21.0],
            "close": [20.0, 21.0],
            "symbol": ["TOKEN", "TOKEN"],
        }
    )
    storage.save_candles(seeded_recent, "1d", "TOKEN")

    result = storage.read_candles("1d", "TOKEN")
    assert result["timestamp"].min() == pd.Timestamp("2024-04-01", tz="UTC")
    assert result["timestamp"].max() == pd.Timestamp("2025-01-02", tz="UTC")
```

- [ ] **Step 2: Run the combined regression suite**

Run: `pytest tests/test_storage_list.py tests/test_freqtrade_exporter.py tests/test_export_cleanup.py -v`
Expected: PASS with no regressions in merge-by-default behavior and with explicit failures on unsafe truncation attempts.

- [ ] **Step 3: Run one CLI-level smoke test for export option wiring**

Run: `pytest tests/test_freqtrade_exporter.py -k "keep or export" -v`
Expected: PASS, confirming the code path used by `export-freqtrade` supports the new keep behavior.

- [ ] **Step 4: Commit the verification adjustments**

```bash
git add tests/test_storage_list.py tests/test_freqtrade_exporter.py tests/test_export_cleanup.py
git commit -m "test: cover seeded history preservation workflow"
```

## Self-Review

**Spec coverage:** Covered storage safety, exporter safety, quick-seed/history-preserving merges, full-collect/no-nice workflow passthrough, fail-fast behavior, and `--keep` parquet retention.

**Placeholder scan:** No `TBD`, `TODO`, “handle edge cases”, or task references that require reading another task to understand the work.

**Type consistency:** Plan uses `keep_parquet` in Python, `--keep` / `-k` in CLI, and `KEEP=--keep` in Makefile consistently.

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-04-14-history-preserving-quickseed-export.md`. Two execution options:

**1. Subagent-Driven (recommended)** - I dispatch a fresh subagent per task, review between tasks, fast iteration

**2. Inline Execution** - Execute tasks in this session using executing-plans, batch execution with checkpoints

Which approach?
