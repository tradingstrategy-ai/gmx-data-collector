# Feather Zstd Compression — Design Spec

**Date:** 2026-05-27
**Status:** Approved (verbal, scoped via brainstorming session)
**Branch:** `feat/feather-zstd-compression`

---

## Problem

The FreqTrade feather export directory (`/Volumes/WD Blue 1tb/VMs/data/gmx/futures/`)
holds **12 GB across 2,750 files**.  Breakdown:

| Timeframe | Size | Share |
|---|---|---|
| 1m | 7.4 GB | 62% |
| 5m | 1.9 GB | 16% |
| Other | ~2.7 GB | 22% |

Three variants per symbol/timeframe (`futures`, `mark`, `index`) each take
~3.4 GB and contain near-identical OHLCV (mark ≈ index because GMX uses
Chainlink for both; futures differs by ~$2 mean on a $110 K BTC).

Files are currently written **uncompressed** by Polars' default `write_ipc`
(see `freqtrade_exporter.py:483` and `:542`).  All numeric columns are `f64`.
The largest individual files contain 2 M–2.5 M rows of 1 m candles
back-filled from CEX sources to 2021 (predates GMX on Arbitrum).

## Goal

Cut the feather footprint by ~78 % (12 GB → ~2.6 GB) without changing
data semantics or breaking FreqTrade compatibility.

## Approach

**Enable zstd compression** on every feather writer that produces files in
the FreqTrade output tree.  Polars `write_ipc(path, compression='zstd')`
uses Arrow's IPC zstd encoding, which:

- keeps the `.feather` filename and on-disk layout
- is read transparently by `pd.read_feather()` (FreqTrade's read path)
- is read transparently by `pl.read_ipc()` (our merge path)
- achieves ~78 % saving on the OHLCV schema we use (measured on LINK 1m)

No schema changes, no f32 downcast (kept for a possible follow-up), no
variant deduplication (user explicitly declined).

### Benchmark (LINK 1m, 121 MB original)

| Format | Size | Saving |
|---|---|---|
| Uncompressed (current) | 121.1 MB | — |
| feather + lz4 | 33.2 MB | -73 % |
| **feather + zstd** | **26.4 MB** | **-78 %** |
| parquet + zstd | 25.6 MB | -79 % |

Zstd is chosen over lz4 because it gives a noticeably better ratio with
negligible read overhead and is the de-facto default in the Arrow stack.

## Scope

**In scope** — patch every feather writer reachable from the FreqTrade
export path:

1. `src/gmx_historical_data/freqtrade_exporter.py:483, 542` — polars writes
2. `scripts/extract_funding_fee_per_size.py:879` — funding_rate feather
3. `scripts/extract_unified_funding.py:761` — funding_rate feather
4. `scripts/extract_unified_funding.py:608` — unified rates feather
   (internal, but uses the same writer signature — flip for consistency)
5. `src/gmx_historical_data/live_funding.py:140` — pyarrow `write_feather`
6. `scripts/collect_daily_snapshot.py:107` — pyarrow `write_feather`

**Plus a one-shot rewrite script** that walks
`{DATA_DIR}/futures/*.feather` and re-encodes each file in place with
zstd.  This is the only way to reclaim space from already-written files —
the writers' default only affects future runs.

**Out of scope**:

- f32 downcast on price columns (~10-15 % extra saving; defer)
- variant deduplication (user said no removal)
- 1m-history trimming (user said no removal)
- migration of `*.parquet` files (already zstd-compressed by default)

## Constraints

- **FreqTrade compatibility**: verified `pd.read_feather()` reads zstd feather
  transparently; no FreqTrade-side change needed.
- **History guard**: the existing merge path
  (`freqtrade_exporter._merge_and_write`) asserts no row loss.  The rewrite
  script must read → write atomically (write to `.tmp`, then `rename`) so
  a crash mid-write cannot leave a half-file.
- **Polars API**: `write_ipc(file, compression='uncompressed'|'lz4'|'zstd')`.
  No compression level knob — zstd defaults internally to level ~3, which
  matches our benchmark.
- **PyArrow API**: `feather.write_feather(df, path, compression='zstd',
  compression_level=3)` for the two pyarrow writers.
- **Idempotency**: the rewrite script must skip files that are already
  zstd-compressed (detect via IPC metadata or a size-on-disk heuristic).

## Risks & Rollback

| Risk | Mitigation |
|---|---|
| Crash during rewrite leaves half-file | Atomic temp-file + rename pattern |
| Pandas/FreqTrade in some env can't read zstd | Verified locally; add a smoke test that round-trips a sample file via `pd.read_feather` |
| Read perf regression on hot path | Benchmark read speed on a 100 MB sample; zstd reads are within 5 % of uncompressed for IPC |
| External drive interruption (USB unmount mid-walk) | Script checkpoints which files it has rewritten; resume by skipping already-zstd files |

Rollback: re-run the rewrite script with `--compression uncompressed` to
expand everything back.  The PR can also be reverted cleanly — no schema,
filename, or directory-structure changes.

## Success Criteria

1. `du -sh "$FUTURES_DIR"` drops from ~12 GB to ≤ 3 GB.
2. `pd.read_feather()` round-trips every rewritten file with identical
   row count + sum-of-`close` (sentinel check) — enforced by the script.
3. New exports via `make export-freqtrade` produce compressed feathers
   on first write.
4. All existing tests pass (`pytest tests/test_freqtrade_exporter*.py`).
5. A new unit test asserts the polars writer emits a compressed IPC file
   that pandas can read back.
