# CLI `collect` Command Refactor Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Simplify the `collect` CLI command in `cli.py` by eliminating code duplication, breaking up oversized methods, and unifying scattered collection logic into coherent helpers.

**Architecture:** Extract 4 shared helpers from duplicated inline code. Break the 367-line `collect_symbol()` into focused sub-methods. Unify symbol filtering. Add checkpoint saving to the non-Chainlink path. Remove dead code. All changes are internal refactors — no CLI interface changes, no new features.

**Tech Stack:** Python 3.11, typer, pandas, asyncio, rich

---

## File Structure

All changes are in a single file:

| File | Action | Responsibility |
|------|--------|----------------|
| `src/gmx_historical_data/cli.py` | **Modify** | Refactor DataCollector methods and _cli_impl |
| `tests/test_cli_refactor.py` | **Create** | Smoke tests for the extracted helpers |

**Key design decisions:**
- Keep everything in `cli.py` (don't split to new files) — the class/function boundaries are the right abstraction, not file boundaries
- All extracted methods are private (`_` prefix) on `DataCollector`
- The public API (`collect_symbol`, `collect_all_symbols`, `collect_non_chainlink_markets`, CLI flags) stays identical
- Tests focus on the new helpers, not re-testing existing CLI behavior

---

## Chunk 1: Extract shared helpers and remove dead code

### Task 1: Extract `_fetch_gmx_candles_for_timeframes()` helper

**Why:** The same async `fetch_timeframe()` pattern is defined inline 3 times (lines 267, 320, 1054) with slight variations. Extract into one reusable method.

**Files:**
- Modify: `src/gmx_historical_data/cli.py` — DataCollector class

- [ ] **Step 1: Add the new helper method to DataCollector**

Add this method to `DataCollector`, after `check_and_report_data_loss()` (around line 192):

```python
async def _fetch_gmx_candles_for_timeframes(
    self,
    symbol: str,
    timeframes: list[str] | None = None,
    boundaries_by_tf: dict | None = None,
    timeout: float = 120.0,
) -> dict[str, pd.DataFrame]:
    """Fetch GMX API candles for multiple timeframes in parallel.

    :param symbol: Token symbol (e.g., 'ETH')
    :param timeframes: List of timeframe strings (default: all TIMEFRAMES)
    :param boundaries_by_tf: Optional boundary dict; if a timeframe's
        ``gmx_api_needed`` is False, it is skipped. If the boundary mode
        is INCREMENTAL, results are filtered to ``gmx_api_start``.
    :param timeout: Per-timeframe timeout in seconds.
    :returns: Dict mapping timeframe -> DataFrame of candles.
    """
    if not self.gmx_fetcher:
        return {}

    tfs = timeframes or TIMEFRAMES

    async def _fetch_one(tf: str) -> tuple[str, pd.DataFrame]:
        # Skip if boundary says not needed
        if boundaries_by_tf:
            bounds = boundaries_by_tf.get(tf)
            if bounds and not bounds.gmx_api_needed:
                return tf, pd.DataFrame()

        gmx_period = map_timeframe_to_gmx_period(tf)
        try:
            df = await asyncio.wait_for(
                asyncio.to_thread(
                    self.gmx_fetcher.fetch_gmx_candles, symbol, gmx_period
                ),
                timeout=timeout,
            )

            # Apply boundary filter for incremental mode
            if boundaries_by_tf:
                bounds = boundaries_by_tf.get(tf)
                if (
                    bounds
                    and bounds.mode == FetchMode.INCREMENTAL
                    and bounds.gmx_api_start
                ):
                    df = df[df["timestamp"] >= bounds.gmx_api_start]

            return tf, df
        except TimeoutError:
            console.print(f"  [yellow]⏱ {tf}: Timeout after {timeout}s[/yellow]")
            return tf, pd.DataFrame()
        except Exception as e:
            console.print(f"  [yellow]⚠ {tf}: {e}[/yellow]")
            return tf, pd.DataFrame()

    tasks = [_fetch_one(tf) for tf in tfs]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    candles: dict[str, pd.DataFrame] = {}
    for result in results:
        if isinstance(result, Exception):
            console.print(f"  [red]✗ Error fetching timeframe: {result}[/red]")
            continue
        tf, df = result
        if not df.empty:
            candles[tf] = df
    return candles
```

- [ ] **Step 2: Replace the 3 inline `fetch_timeframe()` definitions**

**Location A** — `collect_symbol()`, lines ~267-342 (both branches of the if/elif):

Replace the entire block from `# Fetch only timeframes that need updates` through the `elif self.use_gmx_api and self.gmx_fetcher:` fallback with:

```python
        if self.use_gmx_api and self.gmx_fetcher:
            gmx_candles = await self._fetch_gmx_candles_for_timeframes(
                symbol,
                boundaries_by_tf=fetch_boundaries_by_tf or None,
            )
            # Log fetched timeframes
            for tf, df in gmx_candles.items():
                earliest, latest = df["timestamp"].min(), df["timestamp"].max()
                console.print(
                    f"  [green]✓[/green] {tf}: [cyan]{len(df):,}[/cyan] candles "
                    f"from GMX [dim]({earliest} to {latest})[/dim]"
                )
```

Keep the `if not gmx_candles:` early return that follows.

Also keep the boundary-calculator loop above that populates `fetch_boundaries_by_tf` and loads existing data for skipped timeframes (the block at lines 242-264). That stays as-is — we just remove the inline `fetch_timeframe` definitions and result-processing.

**Location B** — `collect_non_chainlink_markets()`, lines ~1054-1083:

Replace the per-symbol GMX fetch loop with:

```python
            if self.use_gmx_api and self.gmx_fetcher:
                gmx_candles = await self._fetch_gmx_candles_for_timeframes(symbol)
                if gmx_candles:
                    gmx_data_by_symbol[symbol] = gmx_candles
                    for tf, df in gmx_candles.items():
                        console.print(
                            f"  [green]✓[/green] {tf}: {len(df):,} candles from GMX API"
                        )
                else:
                    console.print("  [yellow]○[/yellow] No GMX API data available")
```

- [ ] **Step 3: Remove the dead code fallback path**

Delete the `elif self.use_gmx_api and self.gmx_fetcher:` branch (old lines 318-342) entirely. This was a fallback for when `boundary_calculator` was None, but `boundary_calculator` is always set when `use_gmx_api` is True (see `__init__` lines 136-143). This path was dead code.

- [ ] **Step 4: Verify syntax and imports**

Run: `python -c "import ast; ast.parse(open('src/gmx_historical_data/cli.py').read()); print('OK')"`
Expected: `OK`

Run: `poetry run python -c "from gmx_historical_data.cli import DataCollector; print('OK')"`
Expected: `OK`

- [ ] **Step 5: Commit**

```bash
git add src/gmx_historical_data/cli.py
git commit -m "refactor: extract _fetch_gmx_candles_for_timeframes helper, remove dead fallback"
```

---

### Task 2: Extract `_filter_and_categorize_symbols()` helper

**Why:** Symbol filtering (exclusion check) and Chainlink/non-Chainlink categorization is done 3 different ways in 3 locations.

**Files:**
- Modify: `src/gmx_historical_data/cli.py` — DataCollector class + _cli_impl

- [ ] **Step 1: Add the helper as a module-level function**

Add after the imports / before the `DataCollector` class:

```python
def _filter_and_categorize_symbols(
    symbols: list[str],
    chainlink_only: bool = False,
) -> tuple[list[str], list[str], int]:
    """Filter excluded symbols and categorize into Chainlink vs non-Chainlink.

    :param symbols: Raw symbol list to filter.
    :param chainlink_only: If True, non-Chainlink list is returned empty.
    :returns: Tuple of (chainlink_symbols, non_chainlink_symbols, excluded_count).
    """
    non_chainlink_set = set(get_gmx_markets_without_chainlink_feeds())
    chainlink = []
    non_chainlink = []
    excluded = 0

    for s in symbols:
        if is_excluded_symbol(s):
            excluded += 1
            continue
        if s in non_chainlink_set:
            if not chainlink_only:
                non_chainlink.append(s)
        else:
            chainlink.append(s)

    return chainlink, non_chainlink, excluded
```

- [ ] **Step 2: Use the helper in `_cli_impl()` single-symbol path**

Replace lines ~1560-1577 (symbol parsing, filtering, separation) with:

```python
            symbols_list = [s.strip().upper() for s in symbol.split(",") if s.strip()]
            chainlink_symbols, non_chainlink_symbols, excluded_count = (
                _filter_and_categorize_symbols(symbols_list, chainlink_only)
            )
            if excluded_count > 0:
                console.print(
                    f"[yellow]Warning: {excluded_count} symbol(s) excluded "
                    f"(deprecated/problematic)[/yellow]"
                )
            if not chainlink_symbols and not non_chainlink_symbols:
                console.print("[red]No valid symbols to collect[/red]")
                raise typer.Exit(0)
```

- [ ] **Step 3: Use the helper in `collect_all_symbols()` oracle-mode path**

Replace lines ~811-833 (discover, filter, count, print) with:

```python
            if chainlink_only:
                console.print("\n[bold]Loading Chainlink-feed markets...[/bold]")
                all_symbols = get_gmx_markets_with_chainlink_feeds()
            else:
                console.print("\n[bold]Discovering GMX tokens...[/bold]")
                all_symbols = self.gmx_discovery.get_supported_symbols()

            chainlink_syms, non_chainlink_syms, excluded_count = (
                _filter_and_categorize_symbols(all_symbols, chainlink_only)
            )
            # For collect_all_symbols we process all non-excluded together
            symbols = chainlink_syms + non_chainlink_syms

            console.print(
                f"  [green]✓[/green] Found [cyan]{len(symbols)}[/cyan] markets"
            )
            if excluded_count > 0:
                console.print(
                    f"  [dim]Excluded {excluded_count} deprecated/problematic symbol(s)[/dim]"
                )
```

- [ ] **Step 4: Verify syntax and imports**

Run: `python -c "import ast; ast.parse(open('src/gmx_historical_data/cli.py').read()); print('OK')"`
Expected: `OK`

- [ ] **Step 5: Commit**

```bash
git add src/gmx_historical_data/cli.py
git commit -m "refactor: extract _filter_and_categorize_symbols helper"
```

---

### Task 3: Extract `_merge_and_save_candles()` helper

**Why:** The "combine multiple DataFrames + dedup + save" logic appears with variations in `collect_symbol()` (lines 488-531) and `collect_non_chainlink_markets()` (lines 1205-1250).

**Files:**
- Modify: `src/gmx_historical_data/cli.py` — DataCollector class

- [ ] **Step 1: Add the helper method**

```python
def _merge_and_save_candles(
    self,
    symbol: str,
    timeframe: str,
    *dataframes: pd.DataFrame | None,
    merge_with_existing: bool = False,
) -> int:
    """Combine, deduplicate, and save candle DataFrames for one timeframe.

    :param symbol: Token symbol.
    :param timeframe: Timeframe string (e.g., '1h').
    :param dataframes: One or more DataFrames to combine (None values ignored).
    :param merge_with_existing: If True, load existing candles from storage
        and merge with them (for incremental mode).
    :returns: Number of candles saved, or 0 if nothing to save.
    """
    # Collect non-empty DataFrames
    dfs = [df for df in dataframes if df is not None and not df.empty]
    if not dfs:
        return 0

    if len(dfs) == 1:
        combined = dfs[0]
    else:
        combined = pd.concat(dfs, ignore_index=True)

    # Merge with existing storage if incremental
    if merge_with_existing:
        existing = self.storage.read_candles(timeframe, symbol)
        if not existing.empty:
            combined = pd.concat([existing, combined], ignore_index=True)

    # Deduplicate and sort
    combined = combined.drop_duplicates(subset=["timestamp"], keep="last")
    combined = combined.sort_values("timestamp").reset_index(drop=True)

    self.storage.save_candles(combined, timeframe, symbol)
    return len(combined)
```

- [ ] **Step 2: Use it in `collect_symbol()` Step 4**

Replace the for-loop at lines ~488-531 with:

```python
        for timeframe in TIMEFRAMES:
            boundaries = fetch_boundaries_by_tf.get(timeframe) if fetch_boundaries_by_tf else None

            if boundaries and boundaries.mode == FetchMode.NO_FETCH:
                console.print(f"  [green]✓[/green] {timeframe}: Already up to date, skipped")
                continue

            chainlink_df = chainlink_candles.get(timeframe)
            gmx_df = gmx_candles.get(timeframe)

            # If both sources exist, combine them using the dedicated combiner
            if chainlink_df is not None and gmx_df is not None and not gmx_df.empty:
                merged_df = combine_gmx_and_chainlink_data(gmx_df, chainlink_df)
            else:
                merged_df = chainlink_df if chainlink_df is not None else gmx_df

            is_incremental = boundaries and boundaries.mode == FetchMode.INCREMENTAL
            count = self._merge_and_save_candles(
                symbol, timeframe, merged_df,
                merge_with_existing=is_incremental,
            )

            if count > 0:
                stored = self.storage.read_candles(timeframe, symbol)
                earliest, latest = stored["timestamp"].min(), stored["timestamp"].max()
                console.print(
                    f"  [green]✓[/green] {timeframe}: {count:,} candles saved "
                    f"[dim]({earliest} to {latest})[/dim]"
                )
```

- [ ] **Step 3: Use it in `collect_non_chainlink_markets()` Step 3**

Replace the merge loop at lines ~1205-1250 with:

```python
                for timeframe in TIMEFRAMES:
                    gmx_df = gmx_candles.get(timeframe)
                    oracle_df = None

                    if token_events:
                        oracle_df = aggregate_oracle_events_to_ohlcv(
                            token_events, timeframe, symbol, token_decimals=decimals
                        )
                        if oracle_df.empty:
                            oracle_df = None

                    # Filter oracle to before GMX coverage to avoid overlap
                    if gmx_df is not None and oracle_df is not None:
                        gmx_earliest = gmx_df["timestamp"].min()
                        oracle_df = oracle_df[oracle_df["timestamp"] < gmx_earliest]
                        if oracle_df.empty:
                            oracle_df = None

                    count = self._merge_and_save_candles(
                        symbol, timeframe, oracle_df, gmx_df,
                    )
                    if count > 0:
                        sources = []
                        if oracle_df is not None:
                            sources.append(f"oracle: {len(oracle_df):,}")
                        if gmx_df is not None:
                            sources.append(f"GMX: {len(gmx_df):,}")
                        console.print(
                            f"  [green]✓[/green] {timeframe}: {count:,} candles "
                            f"({' + '.join(sources) if sources else 'saved'})"
                        )
```

- [ ] **Step 4: Verify syntax**

Run: `python -c "import ast; ast.parse(open('src/gmx_historical_data/cli.py').read()); print('OK')"`

- [ ] **Step 5: Commit**

```bash
git add src/gmx_historical_data/cli.py
git commit -m "refactor: extract _merge_and_save_candles helper"
```

---

## Chunk 2: Break up collect_symbol and add non-Chainlink checkpoints

### Task 4: Break `collect_symbol()` into focused sub-methods

**Why:** `collect_symbol()` is 367 lines doing 4 distinct steps. Break into an orchestrator that calls focused helpers.

**Files:**
- Modify: `src/gmx_historical_data/cli.py` — DataCollector class

- [ ] **Step 1: Extract `_backfill_chainlink_or_oracle()` from Step 3**

Move lines ~364-483 (the entire Chainlink backfill + oracle fallback section) into:

```python
async def _backfill_chainlink_or_oracle(
    self,
    symbol: str,
    chainlink_feed_address: str | None,
    fetch_boundaries_by_tf: dict,
) -> dict[str, pd.DataFrame]:
    """Backfill historical candles via Chainlink RPC or oracle event fallback.

    :param symbol: Token symbol.
    :param chainlink_feed_address: Chainlink feed proxy address, or None.
    :param fetch_boundaries_by_tf: Boundary dict from Step 1.
    :returns: Dict mapping timeframe -> DataFrame of historical candles.
    """
    chainlink_candles = {}

    if chainlink_feed_address:
        boundaries_1h = fetch_boundaries_by_tf.get("1h")

        if boundaries_1h and boundaries_1h.chainlink_needed:
            console.print("\n[bold]Backfilling with Chainlink data...[/bold]")
            # ... (move existing Chainlink backfill code here verbatim)
        else:
            console.print("\n[green]✓[/green] Chainlink backfill not needed - data is complete")
    else:
        # No Chainlink feed — oracle fallback
        if self.hypersync is None:
            console.print(
                "\n[yellow]⚠ No Chainlink feed found and HyperSync not initialized[/yellow]"
            )
        else:
            console.print("\n[bold]Backfilling with oracle events...[/bold]")
            try:
                await self._collect_symbol_via_oracle_fallback(symbol)
                for tf in TIMEFRAMES:
                    stored_df = self.storage.read_candles(tf, symbol)
                    if not stored_df.empty:
                        chainlink_candles[tf] = stored_df
            except Exception as e:
                console.print(f"[yellow]  Oracle fallback failed: {e}[/yellow]")

    return chainlink_candles
```

- [ ] **Step 2: Extract `_save_symbol_checkpoint()` from end of collect_symbol**

Move the checkpoint-saving block (lines ~533-557) into:

```python
def _save_symbol_checkpoint(self, symbol: str) -> None:
    """Save a completion checkpoint for a symbol after successful collection.

    :param symbol: Token symbol that was collected.
    """
    try:
        latest_timestamp = 0
        total_candles = 0
        for tf in TIMEFRAMES:
            stored_df = self.storage.read_candles(tf, symbol)
            if not stored_df.empty:
                total_candles += len(stored_df)
                ts_max = stored_df["timestamp"].max()
                if hasattr(ts_max, "timestamp"):
                    ts_val = int(ts_max.timestamp())
                else:
                    ts_val = int(ts_max)
                latest_timestamp = max(latest_timestamp, ts_val)
        self.checkpoint_mgr.save_checkpoint(
            Checkpoint(
                symbol=symbol,
                last_block=0,
                last_timestamp=latest_timestamp,
                total_events=total_candles,
                last_updated=datetime.utcnow().isoformat(),
            )
        )
    except Exception as e:
        console.print(
            f"  [yellow]Warning: Could not save checkpoint for {symbol}: {e}[/yellow]"
        )
```

- [ ] **Step 3: Simplify `collect_symbol()` to an orchestrator**

After extraction, `collect_symbol()` should look like:

```python
async def collect_symbol(self, symbol: str, full: bool = False) -> None:
    """Collect data for a single symbol using GMX-first approach.

    :param symbol: Token symbol (e.g., 'ETH')
    :param full: If True, collect from genesis; if False, resume from checkpoint
    """
    console.print()
    console.print(
        Panel(f"[bold cyan]Collecting data for {symbol}[/bold cyan]", box=box.ROUNDED)
    )

    # Step 0: Check for data loss
    if self.adaptive_gap_detector:
        # ... (keep existing gap check code, ~20 lines)

    # Step 0.5: Determine mode and boundaries
    fetch_mode = FetchMode.FULL if full else FetchMode.INCREMENTAL
    console.print(f"\n[bold]Collection mode: {fetch_mode.value}[/bold]")
    chainlink_available = get_feed_address_for_gmx_symbol(symbol) is not None

    # Step 1: Calculate boundaries and load skipped timeframes
    fetch_boundaries_by_tf = {}
    gmx_candles = {}
    if self.boundary_calculator:
        for tf in TIMEFRAMES:
            boundaries = self.boundary_calculator.calculate_boundaries(
                symbol=symbol, timeframe=tf, mode=fetch_mode,
                chainlink_available=chainlink_available, gmx_earliest=None,
            )
            fetch_boundaries_by_tf[tf] = boundaries
            if not boundaries.gmx_api_needed:
                existing_df = self.storage.read_candles(tf, symbol)
                if not existing_df.empty:
                    gmx_candles[tf] = existing_df

    # Step 2: Fetch GMX API candles
    console.print("\n[bold]Fetching data from GMX API...[/bold]")
    fetched = await self._fetch_gmx_candles_for_timeframes(
        symbol, boundaries_by_tf=fetch_boundaries_by_tf or None,
    )
    gmx_candles.update(fetched)

    if not gmx_candles:
        console.print(f"[yellow]No GMX data available for {symbol}[/yellow]")
        return

    # Step 3: Backfill with Chainlink or oracle
    chainlink_feed_address = (
        get_feed_address_for_gmx_symbol(symbol) if chainlink_available else None
    )
    chainlink_candles = await self._backfill_chainlink_or_oracle(
        symbol, chainlink_feed_address, fetch_boundaries_by_tf,
    )

    # Step 4: Merge and save
    console.print("\n[bold]Saving candles...[/bold]")
    for timeframe in TIMEFRAMES:
        boundaries = fetch_boundaries_by_tf.get(timeframe)
        if boundaries and boundaries.mode == FetchMode.NO_FETCH:
            continue
        chainlink_df = chainlink_candles.get(timeframe)
        gmx_df = gmx_candles.get(timeframe)
        if chainlink_df is not None and gmx_df is not None and not gmx_df.empty:
            merged_df = combine_gmx_and_chainlink_data(gmx_df, chainlink_df)
        else:
            merged_df = chainlink_df if chainlink_df is not None else gmx_df
        is_incremental = boundaries and boundaries.mode == FetchMode.INCREMENTAL
        count = self._merge_and_save_candles(
            symbol, timeframe, merged_df, merge_with_existing=is_incremental,
        )
        if count > 0:
            stored = self.storage.read_candles(timeframe, symbol)
            earliest, latest = stored["timestamp"].min(), stored["timestamp"].max()
            console.print(
                f"  [green]✓[/green] {timeframe}: {count:,} candles "
                f"[dim]({earliest} to {latest})[/dim]"
            )

    # Step 5: Checkpoint
    self._save_symbol_checkpoint(symbol)
    console.print(f"\n[bold green]✓ Collection complete for {symbol}[/bold green]")
```

- [ ] **Step 4: Verify syntax and import test**

Run: `python -c "import ast; ast.parse(open('src/gmx_historical_data/cli.py').read()); print('OK')"`
Run: `poetry run python -c "from gmx_historical_data.cli import DataCollector; print('OK')"`

- [ ] **Step 5: Commit**

```bash
git add src/gmx_historical_data/cli.py
git commit -m "refactor: break collect_symbol into focused sub-methods"
```

---

### Task 5: Add checkpoint saving to `collect_non_chainlink_markets()`

**Why:** `collect_non_chainlink_markets()` doesn't save checkpoints, so interrupted non-Chainlink collection can't be resumed.

**Files:**
- Modify: `src/gmx_historical_data/cli.py` — `collect_non_chainlink_markets()` method

- [ ] **Step 1: Add checkpoint save after each successful symbol in Step 3**

In the `collect_non_chainlink_markets()` method, after the `successful += 1` line (around line 1252), add:

```python
                # Save checkpoint for resume
                self._save_symbol_checkpoint(symbol)
```

- [ ] **Step 2: Add checkpoint-based skip logic at the start of the method**

After the `symbols_to_collect` list is finalized (around line 1034), add:

```python
        # Skip already-checkpointed symbols for resume
        if symbols_to_collect:
            pending = []
            skipped = []
            for s in symbols_to_collect:
                cp = self.checkpoint_mgr.load_checkpoint(s)
                if cp and cp.total_events > 0:
                    skipped.append(s)
                else:
                    pending.append(s)
            if skipped:
                console.print(
                    f"  [green]✓[/green] Skipping [cyan]{len(skipped)}[/cyan] "
                    f"already-collected non-Chainlink symbols"
                )
            symbols_to_collect = pending
```

- [ ] **Step 3: Verify syntax**

Run: `python -c "import ast; ast.parse(open('src/gmx_historical_data/cli.py').read()); print('OK')"`

- [ ] **Step 4: Commit**

```bash
git add src/gmx_historical_data/cli.py
git commit -m "refactor: add checkpoint save/skip to collect_non_chainlink_markets"
```

---

### Task 6: Write smoke tests for extracted helpers

**Files:**
- Create: `tests/test_cli_refactor.py`

- [ ] **Step 1: Write tests**

```python
"""Smoke tests for CLI refactor helpers."""

import pytest
from gmx_historical_data.cli import _filter_and_categorize_symbols


class TestFilterAndCategorizeSymbols:
    """Tests for the symbol filtering/categorization helper."""

    def test_excludes_deprecated_symbols(self):
        """Excluded symbols are filtered out."""
        chainlink, non_chainlink, excluded = _filter_and_categorize_symbols(
            ["ETH", "APE_DEPRECATED", "BTC"]
        )
        assert excluded == 1
        assert "APE_DEPRECATED" not in chainlink
        assert "APE_DEPRECATED" not in non_chainlink

    def test_separates_chainlink_and_non_chainlink(self):
        """Symbols are categorized correctly."""
        chainlink, non_chainlink, _ = _filter_and_categorize_symbols(
            ["ETH", "BTC", "SUI"]  # ETH, BTC have Chainlink; SUI doesn't
        )
        assert "ETH" in chainlink
        assert "BTC" in chainlink
        assert "SUI" in non_chainlink

    def test_chainlink_only_skips_non_chainlink(self):
        """With chainlink_only=True, non-Chainlink symbols are dropped."""
        chainlink, non_chainlink, _ = _filter_and_categorize_symbols(
            ["ETH", "SUI"], chainlink_only=True
        )
        assert "ETH" in chainlink
        assert non_chainlink == []

    def test_empty_input(self):
        """Empty input returns empty results."""
        chainlink, non_chainlink, excluded = _filter_and_categorize_symbols([])
        assert chainlink == []
        assert non_chainlink == []
        assert excluded == 0
```

- [ ] **Step 2: Run the tests**

Run: `export JSON_RPC_ARBITRUM=$ARBITRUM_CHAIN_JSON_RPC && poetry run python -m pytest tests/test_cli_refactor.py -v`
Expected: 4 passed

- [ ] **Step 3: Commit**

```bash
git add tests/test_cli_refactor.py
git commit -m "test: add smoke tests for CLI refactor helpers"
```

---

### Task 7: Final verification

- [ ] **Step 1: Run full test suite**

```bash
export JSON_RPC_ARBITRUM=$ARBITRUM_CHAIN_JSON_RPC
poetry run python -m pytest tests/ -q --ignore=tests/test_gmx_market_mapper.py --ignore=tests/test_integration_event_based.py
```

Expected: All previously-passing tests still pass. The pre-existing `test_chainlink_symbol_mapper` failure is unrelated.

- [ ] **Step 2: Verify import and CLI help**

```bash
poetry run python -c "from gmx_historical_data.cli import DataCollector; print('OK')"
poetry run gmx_historical_data collect --help
```

Expected: No import errors. Help text shows all flags unchanged.

- [ ] **Step 3: Count lines saved**

```bash
wc -l src/gmx_historical_data/cli.py
```

Expected: ~2050-2100 lines (down from 2307). Approximately 200-250 lines saved through deduplication.

- [ ] **Step 4: Final commit if any cleanup needed**

```bash
git add -A
git commit -m "refactor: CLI collect command cleanup — final polish"
```
