# Full Historical Funding Rate Backfill Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend GMX V2 funding rate history from Aug 2025 back to Aug 2023 (GMX V2 genesis) by running the DataStore backfill phase, then catch up the 12-day stale tail, and re-export FreqTrade feather files.

**Architecture:** Three-source merge pipeline already exists. Currently only Source 2 (HyperSync Funding Factor, blocks 370M+) is populated. This plan adds Source 1 (DataStore RPC reads, blocks 120M-370M) and refreshes Source 2 to present, then re-runs the merge + export steps. Source 3 (direction) already covers the full history (Aug 2023 → May 2026) and needs only a tail catch-up.

**Tech Stack:** Python/Poetry, `extract_funding_datastore.py` (archive RPC + batched eth_call), `extract_unified_funding.py` (orchestrator), HyperSync, Polars, Parquet, FreqTrade feather.

---

## Context & Current State

### What exists on disk
| Path | Period | Status |
|------|--------|--------|
| `funding/arbitrum/rates/{SYM}/1h_factor.parquet` | Aug 2025 → May 14 2026 | ✅ HyperSync V2.2+ |
| `funding/arbitrum/direction/{SYM}/1h.parquet` | Aug 2023 → May 14 2026 | ✅ Complete |
| `funding/arbitrum/raw/fee_per_size/{SYM}/data.parquet` | blocks 120M → 462M | ✅ Complete |
| `funding/arbitrum/raw/funding/{SYM}/partition=0/data.parquet` | blocks 370M → 462M | ✅ V2.2+ |
| `funding/arbitrum/rates/{SYM}/1h_datastore.parquet` | — | ❌ **MISSING** |

### Why data starts Aug 2025 (not Aug 2023)
GMX V2 launched at block 120,000,000 (~Aug 2023). The `Funding` event was only added in V2.2 (~block 370,000,000, Aug 2025). The existing extraction only ran the HyperSync phase (block 370M+). The pre-V2.2 period requires reading `savedFundingFactorPerSecond` from the DataStore contract via an **archive RPC node**.

### Gap summary
- **Historical gap**: 2023-08-26 → 2025-08-19 (blocks 120M → 370M, ~2 years) — needs DataStore backfill
- **Stale tail**: 2026-05-14 → 2026-05-26 (blocks ~462M → current, 12 days) — needs HyperSync resume

### External data root
All data lives on: `/Volumes/WD Blue 1tb/VMs/data/gmx/`
Pass to make targets via: `DATA="/Volumes/WD Blue 1tb/VMs/data/gmx"`

---

## File Structure

No new files created. Existing scripts are used as-is:

| Script | Role |
|--------|------|
| `scripts/extract_funding_datastore.py` | Phase 1: archive RPC reads, writes `rates/{SYM}/1h_datastore.parquet` |
| `scripts/extract_unified_funding.py` | Orchestrator: Phase 2 (HyperSync factor), Phase 3 (direction), Merge |
| `scripts/extract_funding_fee_per_size.py` | Direction detection (called by unified) |
| `Makefile` targets: `funding-full`, `funding-unified-resume`, `funding-unified-merge`, `export-funding` | Convenience wrappers |

---

## Chunk 1: Prerequisites

### Task 1: Verify archive RPC URL in `.env`

**Files:**
- Modify: `.env`

The DataStore phase requires an **archive node** (not a pruned node). The current `.env` has a placeholder.

- [ ] **Step 1: Check current .env**

```bash
grep JSON_RPC_ARBITRUM .env
```

Expected: placeholder `https://arb-mainnet.g.alchemy.com/v2/YOUR_ALCHEMY_KEY`

- [ ] **Step 2: Replace with a real archive RPC URL**

Get an Alchemy, Infura, or QuickNode archive URL and set it:

```bash
# Edit .env — replace the placeholder value, e.g.:
JSON_RPC_ARBITRUM=https://arb-mainnet.g.alchemy.com/v2/<YOUR_KEY>
```

Archive node requirement: the script calls `eth_call` at historical blocks back to block 120,000,000 (~Aug 2023). Pruned nodes only keep recent state and will return zeros or errors for old blocks.

- [ ] **Step 3: Verify RPC works for historical blocks**

```bash
export JSON_RPC_ARBITRUM=$(grep JSON_RPC_ARBITRUM .env | cut -d= -f2-)
poetry run python -c "
from eth_defi.provider.multi_provider import create_multi_provider_web3
import os
w3 = create_multi_provider_web3(os.environ['JSON_RPC_ARBITRUM'])
block = w3.eth.get_block(120_000_000)
print('Block 120M timestamp:', block['timestamp'])
import datetime
print('Date:', datetime.datetime.utcfromtimestamp(block['timestamp']))
"
```

Expected output: `Date: 2023-08-26 ...` (approximately)

If this fails with timeout or returns 0, the RPC is pruned — get an archive node.

---

### Task 2: Verify HyperSync token and disk space

- [ ] **Step 1: Check HyperSync token in .env**

```bash
grep HYPERSYNC_API_TOKEN .env
```

Expected: a real token (not `YOUR_HYPERSYNC_TOKEN`). If missing, get one from https://envio.dev.

- [ ] **Step 2: Check available disk space on external drive**

```bash
df -h "/Volumes/WD Blue 1tb/"
du -sh "/Volumes/WD Blue 1tb/VMs/data/gmx/funding/"
```

The DataStore backfill will add ~1h × 208,333 intervals × 260 symbols worth of rows. Rough estimate: ~200-400 MB additional parquet. Ensure ≥5 GB free.

- [ ] **Step 3: Verify external drive is mounted**

```bash
ls "/Volumes/WD Blue 1tb/VMs/data/gmx/funding/arbitrum/rates/BTC/"
```

Expected: `1h_factor.parquet`

---

## Chunk 2: DataStore Historical Backfill (Phase 1)

This is the slow step (~35-90 minutes depending on archive RPC latency).

### Task 3: Run DataStore backfill (blocks 120M → 370M)

**Files:**
- Reads: `scripts/extract_funding_datastore.py`
- Writes: `/Volumes/WD Blue 1tb/VMs/data/gmx/funding/arbitrum/rates/{SYM}/1h_datastore.parquet`
- Checkpoint: `/Volumes/WD Blue 1tb/VMs/data/gmx/funding/arbitrum/checkpoints/funding_datastore_checkpoint.json`

- [ ] **Step 1: Export env vars and do a quick smoke test on a small range**

```bash
export JSON_RPC_ARBITRUM=$(grep JSON_RPC_ARBITRUM .env | cut -d= -f2-)
poetry run python scripts/extract_funding_datastore.py \
    --from-block 120000000 \
    --to-block 120100000 \
    --output-dir "/Volumes/WD Blue 1tb/VMs/data/gmx/funding" \
    --market "ETH/USD" \
    --output parquet
```

Expected: prints a handful of rows for ETH, creates `rates/arbitrum/ETH/1h_datastore.parquet` with ~3 rows.
If this succeeds, the archive node is working.

- [ ] **Step 2: Run full DataStore backfill with resume enabled**

```bash
export JSON_RPC_ARBITRUM=$(grep JSON_RPC_ARBITRUM .env | cut -d= -f2-)
poetry run python scripts/extract_funding_datastore.py \
    --from-block 120000000 \
    --to-block 370000000 \
    --output-dir "/Volumes/WD Blue 1tb/VMs/data/gmx/funding" \
    --output parquet \
    --resume
```

This samples at 1200-block intervals (~1 hour on Arbitrum) and batches 50 concurrent `eth_call`s per HTTP request. Expected: ~4,167 HTTP batches, ~35-90 minutes depending on RPC latency.

Progress is checkpointed — if interrupted, re-run the same command (the `--resume` flag picks up from the checkpoint).

- [ ] **Step 3: Verify DataStore output**

```bash
poetry run python -c "
import polars as pl, os
base = '/Volumes/WD Blue 1tb/VMs/data/gmx/funding/arbitrum/rates'
for sym in ['BTC', 'ETH', 'SOL']:
    p = f'{base}/{sym}/1h_datastore.parquet'
    if os.path.exists(p):
        df = pl.read_parquet(p)
        print(f'{sym}: {len(df)} rows, ts {df[\"timestamp\"].min()} -> {df[\"timestamp\"].max()}')
    else:
        print(f'{sym}: MISSING')
"
```

Expected: each symbol has ~208,000 rows (one per hour from Aug 2023 to Aug 2025), timestamps from ~1692998400 (Aug 2023) to ~1754035200 (Aug 2025).

---

## Chunk 3: HyperSync Tail Catch-up (Phase 2 + Phase 3)

### Task 4: Resume HyperSync extraction to bring data to today

This covers the 12-day gap (May 14 → today) in both the Funding Factor and Direction phases. HyperSync is fast — expect 2-5 minutes.

**Files:**
- Writes: `raw/funding/{SYM}/partition=0/data.parquet` (appended)
- Writes: `raw/fee_per_size/{SYM}/data.parquet` (appended)
- Writes: `rates/{SYM}/1h_factor.parquet` (updated)
- Writes: `direction/{SYM}/1h.parquet` (updated)

- [ ] **Step 1: Run unified funding resume**

```bash
make funding-unified-resume DATA="/Volumes/WD Blue 1tb/VMs/data/gmx"
```

This runs:
1. `extract_funding_factor.py --resume` (HyperSync Funding events, blocks ~462M → latest)
2. `extract_funding_fee_per_size.py --resume` (HyperSync direction events, blocks ~462M → latest)

- [ ] **Step 2: Verify tail is up to date**

```bash
poetry run python -c "
import polars as pl, datetime
p = '/Volumes/WD Blue 1tb/VMs/data/gmx/funding/arbitrum/rates/BTC/1h_factor.parquet'
df = pl.read_parquet(p)
latest_ts = df['timestamp'].max()
latest_dt = datetime.datetime.utcfromtimestamp(latest_ts)
print(f'BTC 1h_factor latest: {latest_dt.date()} ({latest_ts})')
today = datetime.datetime.utcnow().date()
days_stale = (today - latest_dt.date()).days
print(f'Days stale: {days_stale} (should be 0-1)')
"
```

Expected: latest date within 1 day of today.

---

## Chunk 4: Merge All Sources

### Task 5: Run the unified merge to produce `1h.parquet` per symbol

The merge function (`merge_symbol`) combines:
1. `1h_datastore.parquet` (pre-V2.2, signed, Aug 2023 → Aug 2025)
2. `1h_factor.parquet` (V2.2+, unsigned + direction-corrected, Aug 2025 → today)

And deduplicates by timestamp (HyperSync data wins at overlap).

**Files:**
- Reads: `rates/{SYM}/1h_datastore.parquet` + `rates/{SYM}/1h_factor.parquet` + `direction/{SYM}/1h.parquet`
- Writes: `rates/{SYM}/1h.parquet` (unified canonical file)

- [ ] **Step 1: Run merge**

```bash
make funding-unified-merge DATA="/Volumes/WD Blue 1tb/VMs/data/gmx"
```

Expected: prints merge summary for all ~130 symbols with data, writes `1h.parquet` per symbol.

- [ ] **Step 2: Verify merged output**

```bash
poetry run python -c "
import polars as pl, datetime, os
base = '/Volumes/WD Blue 1tb/VMs/data/gmx/funding/arbitrum/rates'
for sym in ['BTC', 'ETH', 'SOL']:
    p = f'{base}/{sym}/1h.parquet'
    if not os.path.exists(p):
        print(f'{sym}: 1h.parquet MISSING')
        continue
    df = pl.read_parquet(p)
    ts_min = datetime.datetime.utcfromtimestamp(df['timestamp'].min())
    ts_max = datetime.datetime.utcfromtimestamp(df['timestamp'].max())
    sources = df['source'].value_counts()
    print(f'{sym}: {len(df)} rows | {ts_min.date()} -> {ts_max.date()}')
    print(f'  sources: {sources}')
"
```

Expected: BTC/ETH/SOL each ~23,000+ rows, starting 2023-08-26, sources showing both `datastore` and `hypersync`.

- [ ] **Step 3: Spot-check sign correctness at V2.2 boundary**

```bash
poetry run python -c "
import polars as pl, datetime
p = '/Volumes/WD Blue 1tb/VMs/data/gmx/funding/arbitrum/rates/BTC/1h.parquet'
df = pl.read_parquet(p)
# Check rows around the V2.2 cutoff (Aug 2025)
cutoff = int(datetime.datetime(2025, 8, 18, tzinfo=datetime.timezone.utc).timestamp())
cutoff_end = int(datetime.datetime(2025, 8, 21, tzinfo=datetime.timezone.utc).timestamp())
boundary = df.filter((pl.col('timestamp') >= cutoff) & (pl.col('timestamp') <= cutoff_end))
print('Rows around V2.2 boundary (Aug 18-21 2025):')
print(boundary.select(['timestamp', 'funding_rate', 'source', 'longs_pay_shorts']).head(10))
"
```

Expected: `source` transitions from `datastore` to `hypersync` around this period. Both should have non-null `funding_rate` values with matching sign direction.

---

## Chunk 5: Update 1h_factor.parquet (Canonical Rate File)

The existing pipeline reads `1h_factor.parquet` (not `1h.parquet`) for the FreqTrade export. After the merge, `1h.parquet` contains the full history. We need to either point the exporter at `1h.parquet` or copy it over.

Check the exporter to see which file it reads:

- [ ] **Step 1: Check which file the FreqTrade exporter reads**

```bash
grep -n "1h_factor\|1h\.parquet\|rates.*parquet" src/gmx_historical_data/freqtrade_exporter.py | head -20
```

- [ ] **Step 2a: If exporter reads `1h_factor.parquet` — replace it with `1h.parquet` content**

```bash
poetry run python -c "
import polars as pl, os, shutil
from pathlib import Path

base = Path('/Volumes/WD Blue 1tb/VMs/data/gmx/funding/arbitrum/rates')
for sym_dir in sorted(base.iterdir()):
    src = sym_dir / '1h.parquet'
    dst = sym_dir / '1h_factor.parquet'
    if src.exists():
        # Copy unified 1h.parquet over 1h_factor.parquet
        shutil.copy2(src, dst)
        df = pl.read_parquet(dst)
        print(f'{sym_dir.name}: {len(df)} rows written to 1h_factor.parquet')
"
```

- [ ] **Step 2b: If exporter already reads `1h.parquet` — skip this task**

---

## Chunk 6: Export to FreqTrade Feather Format

### Task 7: Run funding feather export

**Files:**
- Reads: `rates/{SYM}/1h_factor.parquet` (or `1h.parquet` depending on exporter)
- Writes: `futures/{SYM}_USDC_USDC-1h-funding_rate.feather`

- [ ] **Step 1: Run export**

```bash
make export-funding DATA="/Volumes/WD Blue 1tb/VMs/data/gmx"
```

- [ ] **Step 2: Verify feather output covers full history**

```bash
poetry run python -c "
import pandas as pd
import pyarrow.feather as feather
import os

feather_dir = '/Volumes/WD Blue 1tb/VMs/data/gmx/futures'
for sym in ['BTC', 'ETH', 'SOL']:
    path = f'{feather_dir}/{sym}_USDC_USDC-1h-funding_rate.feather'
    if not os.path.exists(path):
        print(f'{sym}: MISSING')
        continue
    df = feather.read_feather(path)
    print(f'{sym}: {len(df)} rows | {df[\"date\"].min().date()} -> {df[\"date\"].max().date()}')
"
```

Expected: BTC/ETH/SOL starting from ~2023-08-26, ending at today. Row count ~23,000+.

- [ ] **Step 3: Sanity check funding rate magnitudes**

```bash
poetry run python -c "
import pyarrow.feather as feather
import pandas as pd

path = '/Volumes/WD Blue 1tb/VMs/data/gmx/futures/BTC_USDC_USDC-1h-funding_rate.feather'
df = feather.read_feather(path)
df = df.sort_values('date')
print('BTC funding rate stats:')
print(df['open'].describe())
print()
# Check for unreasonable values (> 1% per hour = annual 8760%)
extreme = df[df['open'].abs() > 0.01]
print(f'Extreme rows (>1%/hr): {len(extreme)}')
if len(extreme) > 0:
    print(extreme[['date', 'open']].head(5))
"
```

Expected: `mean` close to 0, `std` in the 0.00001-0.001 range, very few or no extreme rows.

---

## Summary of Commands (Quick Reference)

After confirming archive RPC works (Task 1 Step 3):

```bash
# 1. DataStore backfill — ~35-90 min, resumable
export JSON_RPC_ARBITRUM=<your-archive-rpc-url>
poetry run python scripts/extract_funding_datastore.py \
    --from-block 120000000 --to-block 370000000 \
    --output-dir "/Volumes/WD Blue 1tb/VMs/data/gmx/funding" \
    --output parquet --resume

# 2. HyperSync tail catch-up — ~5 min
make funding-unified-resume DATA="/Volumes/WD Blue 1tb/VMs/data/gmx"

# 3. Merge all sources
make funding-unified-merge DATA="/Volumes/WD Blue 1tb/VMs/data/gmx"

# 4. Replace 1h_factor.parquet with merged 1h.parquet (if needed per Task 6)
# (run the python snippet from Task 6 Step 2a)

# 5. Export to FreqTrade feathers
make export-funding DATA="/Volumes/WD Blue 1tb/VMs/data/gmx"
```

---

## Risk & Rollback

- **No existing data is deleted.** DataStore writes to new `1h_datastore.parquet` files.
- **Merge writes to new `1h.parquet`**, not overwriting `1h_factor.parquet`.
- **Step 5 (overwrite `1h_factor.parquet`)** is the only destructive step — take a backup first if concerned:
  ```bash
  cp "/Volumes/WD Blue 1tb/VMs/data/gmx/funding/arbitrum/rates/BTC/1h_factor.parquet" \
     "/Volumes/WD Blue 1tb/VMs/data/gmx/funding/arbitrum/rates/BTC/1h_factor.parquet.bak"
  ```
- If DataStore RPC returns zeros for a symbol, those rows will have `funding_rate=0.0`. Inspect with the verify script from Task 5 Step 3.
- **Resume is safe**: all scripts use checkpoint files — interrupted runs can be re-run without duplication.
