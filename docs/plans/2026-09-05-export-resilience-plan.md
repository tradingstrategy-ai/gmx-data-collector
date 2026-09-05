# Export resilience + key rotation — implementation plan

Implements C1/C2/C3 from the design spec
`docs/superpowers/specs/2026-09-05-gmx-candle-export-resilience-and-freshness-observability-design.md`
(read-only, lives in the sibling `fix-gmx-export-resilience` checkout of `gmx-strategies`).
A1/A2/B1 belong to that other repo and are out of scope here.

Root cause being fixed: a HyperSync `429` interrupted two mid-write parquet writes,
leaving truncated files with no footer magic bytes. `export-freqtrade` then read
symbols alphabetically with no per-symbol guard, so the first corrupt file
(`GMX/1m.parquet`) aborted the entire export after only 41/126 symbols.

## Steps (independently testable, in order)

1. **C1a — atomic write helper in `storage.py`.** Add a private
   `_atomic_write_parquet(df, output_path)` that writes to
   `<name>.parquet.tmp` in the same directory, fsyncs the file (and the
   containing directory), then `os.replace()`s onto the target. Route
   `save_candles`, `save_raw_events`, and `save_position_events` through it —
   all three currently call `pl.from_arrow(table).write_parquet(path, ...)`
   directly with no atomicity, so all three share the incident's flaw.
   Testable via a fault-injection unit test that kills the write mid-flight.

2. **C1b — orphaned `.tmp` sweep.** Add `ParquetStorage.sweep_orphaned_tmp_files()`
   that removes stray `*.parquet.tmp` files under `base_dir`, logging each
   removal. Call it once at collection start (`DataCollector.__init__` in
   `cli.py`, and `GMXPeriodicCollector.__init__` in `daemon/periodic_collector.py`)
   — not on every `ParquetStorage()` construction, since the exporter and other
   read-only call sites also construct `ParquetStorage` and must not race a
   concurrently-running collection's legitimate in-flight `.tmp` file.
   Testable standalone: seed a stray `.tmp`, construct a collector, assert it's gone.

3. **C2a — per-symbol guard in `export_candles()`.** Wrap each symbol's
   read/transform/write body in `try/except (ArrowInvalid, OSError)`, log and
   append to a `failed_symbols: list[str]`, `continue` to the next symbol.
   Change the return type to `tuple[dict[str, dict], list[str]]`.

4. **C2b — propagate through `export()`.** `export()` calls `export_candles()`,
   forwards its `failed_symbols` unchanged (funding failures aren't tracked —
   `export_funding()` is out of scope per spec), returns
   `tuple[dict[str, dict], list[str]]` too.

5. **C2c — CLI surfacing.** Update the three call sites in `cli.py`
   (`export_freqtrade_command`, `export_candles_command`) to unpack the new
   tuple, print a failure-summary panel naming the failed symbols, and
   `raise typer.Exit(1)` when the list is non-empty — independent of whatever
   exception path already existed. `export_funding_command` is unchanged
   (calls `export_funding()`, whose signature doesn't change).
   Testable via `test_export_survives_corrupt_parquet`.

6. **C3a — shared HyperSync client-pool factory.** New module
   `src/gmx_historical_data/hypersync_client_factory.py`:
   - `build_hypersync_client_or_rotator(raw_token, endpoint)` — pure function,
     returns a `HyperSyncKeyRotator` for a >1-key pool, a plain single-key
     `HypersyncClient` for exactly one key, `None` for empty/unset. This is
     the unit directly covered by `test_extract_helpers_rotate_key_pool`.
   - `RotatingHypersyncClient` — thin wrapper used by the scripts themselves:
     builds the client pool once, exposes `.client` (currently active),
     proxies `.get_height()` / `.stream()`, and `.rotate_on_error(exc) -> bool`
     which rotates to the next key when `exc` looks like a 429/rate-limit
     response (same keyword heuristic as `oracle_price_collector.retry_with_backoff`)
     and returns whether rotation happened, mirroring the `clients` +
     `client_index` idiom `OraclePriceCollector` already uses for `collect-update`.

7. **C3b — wire the 7 extract scripts onto the shared helper.** Replace each
   script's duplicated
   `api_token = raw_token.replace(",", " ").split()[0] if raw_token else None`
   with a call through `RotatingHypersyncClient`, and make each script's
   `_stream_with_retry` call `client.rotate_on_error(e)` before falling into
   its normal backoff/retry — so a `429` mid-stream rotates immediately
   instead of just burning a retry against the same throttled key.
   Scripts: `extract_borrowing_factor.py`, `extract_funding_fee_per_size.py`,
   `extract_funding_factor.py`, `extract_open_interest.py`,
   `extract_claimable_fee_per_size.py`, `extract_oracle_prices_raw.py` (each
   has its own `create_client`/`_stream_with_retry` pair), plus
   `extract_pool_liquidity.py` (builds its client inline in `async_main` and
   reuses `extract_open_interest`'s `_stream_with_retry` via import, so it
   inherits rotation once that shared function is fixed — only its client
   construction needs touching).

8. **Tests.** `tests/test_atomic_parquet_writes.py` (C1),
   `tests/test_export_survives_corrupt_parquet.py` (C2),
   `tests/test_hypersync_client_factory.py` (C3). No real network calls —
   HyperSync client construction is mocked/monkeypatched.

## Explicitly out of scope here

- A1 (corrupt-file preflight/quarantine), A2 (freshness verdict line), B1
  (sleeve coverage telemetry) — all live in the `gmx-strategies` repo, a
  different agent's PR.
- `export_funding()` — the spec only calls out `export_candles()`/`export()`
  for the per-symbol guard.
- Any change to `regime_max_stale_days`, gate logic, or what the sleeve trades.
