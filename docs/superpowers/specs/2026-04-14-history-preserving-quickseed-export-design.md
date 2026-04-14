# History-Preserving Quickstart and Export Design

**Date:** 2026-04-14

**Problem**

The repository now supports `quickstart` / quick seed, which seeds local `user_data/`
from the GitHub `data/daily-collection` branch. That branch contains recent GMX API
coverage collected by CI. This is useful for avoiding redundant fetches, but it
introduces a failure mode: later collection or export steps can treat the seeded
short-history dataset as if it were complete and overwrite older on-chain history.

This shows up most clearly on long-lived markets such as `BTC` and `ETH`, where
historical candles should begin near GMX genesis in 2021, but the resulting dataset
can be truncated down to the much shorter CI/API window.

The same issue matters for non-Chainlink markets. A market may have GMX API or
quick-seeded data beginning in January 2025 while the market actually listed on GMX
in April 2024. In that case, the collector must preserve the recent API tail, fetch
the missing older on-chain history, and merge the two into one continuous dataset.

**Goals**

- Ensure full historical coverage collected from on-chain sources is never silently
  overwritten by quick-seeded or GMX API-derived recent data.
- Treat quick-seeded GitHub snapshot data as equivalent to GMX API coverage, not as
  authoritative full history.
- Make `make collect-full` and the no-`nice` full-collect path safe when quick seed
  is enabled.
- Fail fast if any normal merge/write/export operation would truncate historical
  coverage.
- Add an export option to keep parquet source files after feather export:
  `--keep` / `-k`.
- Wire the parquet retention option through the relevant `make` targets without
  changing default behavior.

**Non-Goals**

- Repairing already-corrupted datasets on disk.
- Introducing a second manual source-of-truth table for GMX listing dates.
- Changing the existing default overwrite behavior when the user explicitly passes
  destructive flags such as `overwrite=True`.

## Source-of-Truth Model

### Data authority

The system should treat data sources in two classes:

1. **Historical authority**
   - Chainlink/oracle-derived on-chain history for Chainlink-backed markets.
   - Oracle event-derived on-chain history for non-Chainlink markets.

2. **Recent tail coverage**
   - GMX API data.
   - Quick-seeded GitHub snapshot data.

Quick seed must be treated exactly like preloaded GMX API data. It can save time by
providing recent coverage that does not need to be refetched, but it must never be
allowed to redefine the historical start of a market.

### Start boundary

The current code already uses observed coverage and on-chain discovery rather than a
manually maintained listing-date map:

- `DataCoverageAnalyzer` determines the earliest stored coverage for a symbol.
- Oracle/event collectors can discover or approximate the earliest real event block.

The design should keep this approach. The historical start boundary is whichever
earliest on-chain coverage the current collectors can establish for that market, not
the earliest timestamp found in seeded/API data.

## Required Behavior

### 1. Full collection remains history-preserving

For every symbol and timeframe, a non-overwrite write must satisfy this rule:

> Existing historical coverage may be extended or refined, but it may not be
> shortened.

That means:

- If local parquet already starts earlier than incoming data, the earlier local
  history must remain after the write.
- If incoming data starts earlier than local parquet, the merged result should begin
  at the earlier incoming timestamp.
- If both overlap, duplicate timestamps may be resolved in favor of the newer row,
  but overall historical coverage must not move forward.

This rule applies to:

- storage writes of candle parquet files
- collector merge/save paths used by full and incremental collection
- Freqtrade export writes for feather/parquet output files

### 2. Quick seed behaves like API tail coverage

Quick-seeded data should only reduce work, not reduce history.

Expected behavior:

- If quick seed already provides recent coverage for a symbol, the collector may skip
  redundant recent fetches.
- If on-chain history before the seeded/API window is missing, the collector must
  still backfill the older range.
- The final merged dataset must contain the older on-chain segment plus the recent
  seeded/API segment.

### 3. Non-Chainlink markets must still backfill older history

For non-Chainlink markets, the collector must not infer “history complete” from the
presence of a quick-seeded or GMX API-derived recent dataset.

Instead:

- coverage analysis should compare the earliest stored candle against the earliest
  on-chain coverage boundary discoverable by the existing event-based path
- if a gap exists, the collector fetches the missing older on-chain range
- merge/save then combines old on-chain history and recent API/seeded history into a
  continuous dataset

### 4. Fail fast on history truncation risk

The current system contains permissive fallback behavior in some merge/write paths.
That is not acceptable for historical integrity.

New rule:

- If a merge cannot be completed safely, raise an exception and fail the command.
- Do not log a warning and continue with “new-only” data.
- Do not silently replace a longer dataset with a shorter one during normal
  full/incremental workflows.

This is intentionally strict: bad data is worse than a failed run.

## Safety Invariants

For any non-overwrite write where an existing file already exists, the implementation
should compute and validate lightweight coverage metadata before final write:

- existing earliest timestamp
- existing latest timestamp
- existing row count
- incoming earliest timestamp
- incoming latest timestamp
- merged earliest timestamp
- merged latest timestamp
- merged row count

The write is valid only if:

- `merged_earliest <= existing_earliest`
- `merged_latest >= max(existing_latest, incoming_latest)` when incoming data exists
- row loss is explained only by exact duplicate collapse on the merge key

If these conditions cannot be verified or are violated, raise.

The invariants do not apply when the caller explicitly requests destructive behavior
via an overwrite flag intended to replace the file.

## Implementation Areas

### A. Parquet storage

`ParquetStorage.save_candles()` is the lowest-level safety boundary for candle data.

Current problem:

- It attempts merge-by-default.
- If merge fails, it logs an error but still proceeds to write the incoming dataset,
  which can destroy older history.

Required change:

- merge failure becomes fatal
- history-preservation checks run before write
- only `overwrite=True` may bypass merge protection

### B. Collector merge/save path

`CollectorCLI._merge_and_save_candles()` combines multiple inputs and delegates to
storage.

Required change:

- continue combining chainlink/oracle/API inputs as today
- add explicit assertions or coverage checks around pre/post merge expectations where
  useful for clearer diagnostics
- ensure full collection paths (`collect-full`, no-`nice` variant, quickstart path)
  never route through destructive overwrite semantics

### C. Freqtrade exporter

`FreqtradeExporter._write()` currently has the same dangerous pattern:

- try merge
- on merge failure, log error
- continue writing the incoming data only

Required change:

- merge failure becomes fatal
- exported feather/parquet files also enforce history-preservation invariants
- `export-freqtrade` must not be able to shorten history built by prior full runs

### D. Export cleanup / parquet retention

The export flow should support keeping parquet source files after feather export.

Required behavior:

- default behavior stays unchanged
- add CLI flag `--keep` / `-k`
- when enabled, skip parquet cleanup after successful export
- when not enabled, cleanup behavior remains the current default
- Makefile targets for full workflows must expose a pass-through variable so users can
  request `--keep` from `make collect-full ...` and the no-`nice` full-collect path

## User-Facing Behavior

### Full collect with quickstart

When the user runs full collection with quick seed enabled:

1. quick seed copies missing recent cache files only
2. collector analyzes current earliest coverage per symbol
3. if history is already complete, it avoids redundant older fetches
4. if history starts too late, it backfills the missing on-chain period
5. merge/write preserves the earliest historical coverage
6. export fails immediately if it would shorten the final feather dataset

### Example: BTC/ETH

- Quick seed may preload recent API coverage.
- Historical candles should still begin near GMX genesis in 2021.
- Any attempt to replace that long history with a shorter recent window must raise.

### Example: non-Chainlink token

- Seed/API data begins `2025-01-01`.
- Earliest real on-chain history begins around `2024-04-01`.
- Full collect must fetch the missing `2024-04-01 -> 2024-12-31` range and merge it
  with the recent tail.
- Final output must begin in April 2024, not January 2025.

## Testing Requirements

The implementation plan should include tests for at least these cases:

1. **Storage merge preserves older history**
   - existing long-history parquet + incoming short recent parquet
   - result keeps the older earliest timestamp

2. **Storage merge failure is fatal**
   - simulate incompatible existing/incoming file state
   - verify no new-only replacement write occurs

3. **Exporter merge preserves older feather history**
   - existing feather with long coverage + new export with shorter coverage
   - result retains earliest historical date

4. **Exporter merge failure is fatal**
   - simulate schema/read/merge failure
   - verify exception is raised and output is not truncated

5. **Quick-seed plus full collect does not mark non-Chainlink history complete too early**
   - seeded recent-only coverage
   - missing older on-chain range still triggers historical fetch plan

6. **`--keep` preserves parquet data**
   - export with `--keep`
   - source parquet remains on disk

7. **Default export cleanup still occurs**
   - export without `--keep`
   - cleanup matches current default behavior

8. **Makefile passthrough works**
   - relevant make target passes the keep option through to the CLI

## Risks and Tradeoffs

### Stricter failures increase run interruptions

This design intentionally turns previously permissive warning paths into hard errors.
That may interrupt long-running jobs sooner, but it is the correct tradeoff because
silent historical truncation is unacceptable.

### Coverage checks must allow valid deduplication

Duplicate timestamp replacement is valid and should not be treated as row loss if the
merged coverage boundaries remain intact. The implementation must distinguish
deduplication from truncation.

### Quick seed remains an optimization, not a completeness proof

The presence of seeded data must never be interpreted as evidence that older history
is unavailable or unnecessary to fetch.

## Accepted Design Summary

The system will enforce a history-preserving contract across parquet storage, collector
merge/save logic, and Freqtrade export. Quick-seeded GitHub data is treated as GMX API
tail coverage only. Full collection remains responsible for backfilling missing older
on-chain history, especially for non-Chainlink markets. Any non-overwrite path that
would shorten history becomes a hard failure. Export gains a new `--keep` / `-k` flag,
and the full-collection make workflows will expose that option without changing the
default cleanup behavior.
