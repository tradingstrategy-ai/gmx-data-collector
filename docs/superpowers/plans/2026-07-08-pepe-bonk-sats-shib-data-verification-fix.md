# PEPE / BONK / SATS / SHIB Data Verification & Fix Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Repair corrupted/incomplete GMX historical candle data for PEPE, BONK, SATS, SHIB, after verifying against fresh tmp-dir collections from both the Chainlink path and the historical oracle-event (HyperSync log) path.

**Architecture:** Three phases. (1) Merge the existing-but-unmerged Chainlink decimals fix (`b6c43ff` on `feat/chainlink-decimals-fix`) and add a reusable price-continuity validator. (2) Fresh collection into a tmp dir — Chainlink RPC backfill for PEPE/SHIB, oracle-event re-walk for BONK/SATS — plus a fully staged repair (CEX gap-fill for BONK/SATS) built and validated entirely in tmp, cross-verified against CEX (1000x-scaled pairs), `.bak` pre-corruption files, and `data_fix_test2/`. (3) Copy validated artifacts into the live store and drive behind on-drive backups; nothing is deleted.

**Tech Stack:** Python 3.12 (poetry env), typer CLI (`gmx_historical_data.cli`), pandas/pyarrow/polars, HyperSync, Arbitrum JSON-RPC (Multicall3 Chainlink backfill), freqtrade CEX download (`fill-gaps-cex`).

---

## Diagnosis (established by forensics — do not re-derive)

| Token | Path | Defect |
|-------|------|--------|
| PEPE | Chainlink | Exported feathers (1m/5m/15m/1h/4h, futures+index+mark) are **decade-folded**: a prior repair divided rows by 1e9/1e10/1e11 per-chunk instead of uniform 1e10 (~21% of 1h rows 10x off; 29 decade flips). 1d feather clean. Exported parquets in `futures/` clean but stale (end 2026-06-16). **Live source parquets** (`user_data/data/gmx/candles/arbitrum/PEPE/`) mixed-unit corrupt: close max 2.75e5, full depth 2023-07-21 → 2026-06-29. |
| SHIB | Chainlink | 1h feathers good. `…-1m-index.feather` 97% still ×1e10 (unit cliff 2026-06-09 11:25). `…-1m-mark.feather` **truncated, unreadable**. Most exported parquets raw ×1e10. `…-15m-mark.parquet` missing. Live source parquets mixed-unit corrupt (close max 3.31e5). |
| BONK | Oracle (no Chainlink feed on Arbitrum) | Prices correct everywhere. **Sparsity** in feathers: 1m coverage 10.6% (largest hole 35d), 1h missing 190 bars. Live source parquets clean but shallow (start 2025-12-18). |
| SATS | Oracle (no Chainlink feed) | Prices correct. **Sparsity**: 1m coverage 7.7% (two ~35d holes), 1h missing 1,048 bars. Live source parquets clean but shallow (start 2025-12-18). |

**Root cause (PEPE/SHIB):** `src/gmx_historical_data/cli.py:146` hard-codes `OHLCVResampler(decimals=8)`; PEPE/SHIB Chainlink feeds are 18-decimal → backfill inflated ×1e10. Fix exists in commit `b6c43ff` (branch `feat/chainlink-decimals-fix`, sits directly on master HEAD `b298583`, fast-forwardable). NOT merged; any collect/export on current master re-poisons data.

**Store map (verified — three OHLCV stores + checkpoints):**

| Store | Location | State |
|-------|----------|-------|
| Live source parquets (collector writes, exporter+gap-fill read) | `user_data/data/gmx/candles/arbitrum/<SYM>/{1m,5m,15m,1h,4h,1d}.parquet` — local dir, NOT on drive. Schema: `timestamp,open,high,low,close,symbol` (col is **`timestamp`**, not `date`) | PEPE/SHIB corrupt; BONK/SATS shallow |
| Exported freqtrade files (what strategies consume) | `user_data/data/gmx/futures/` → symlink → `/Volumes/WD Blue 1tb/VMs/data/gmx/futures/`. Schema: `date,open,high,low,close,volume` | per-token defects above |
| Drive snapshot of source parquets | `/Volumes/WD Blue 1tb/VMs/data/gmx/candles/arbitrum/` (checkpoint copy dated 2026-06-12) | PEPE/SHIB corrupt |
| Checkpoints (live) | `user_data/data/gmx/checkpoints/<sym>_checkpoint.json` (PEPE last_updated 2026-06-29) | must match repaired data |

**Reference norms** (healthy ETH/AAVE): complete forward-filled timestamp grid, zero gaps; `volume == 0.0` everywhere; index/mark feathers byte-identical to futures. Repaired data must match.

**Constraints (user rules — binding):**
- No git commands without explicit user approval at the marked ⛔ steps.
- Every live-store/drive mutation preceded by a verified backup. Never delete `.bak` files, `candles_schemafix_backup_2026-06-12/`, `checkpoints_backup_2026-06-12/`, or the new backups.
- NEVER run the exporter with `--unsafe-overwrite` on these tokens (`README.md:63-69`).
- `export JSON_RPC_ARBITRUM=$ARBITRUM_CHAIN_JSON_RPC` before pytest.
- Manual review gates at end of Chunk 2 and Chunk 3.

**Workspace:** `TMP=/tmp/gmx_verify_2026-07-08`. Drive: `DRIVE="/Volumes/WD Blue 1tb/VMs/data/gmx"`. Repo: `/Users/avik/Work/tradingstrategy/gmx_historical_data_new` (all commands run from here).

---

## Chunk 1: Code fix + validation tooling

### Task 0: Preflight

**Files:** `$TMP/manifest_before.txt` created; repo untouched.

- [ ] **Step 0.1: Verify environment + drive**

```bash
cd /Users/avik/Work/tradingstrategy/gmx_historical_data_new
export JSON_RPC_ARBITRUM=$ARBITRUM_CHAIN_JSON_RPC
test -n "$JSON_RPC_ARBITRUM" && echo RPC_OK || echo RPC_MISSING
test -n "$HYPERSYNC_API_TOKEN" && echo HYPERSYNC_OK || echo HYPERSYNC_MISSING
ls "/Volumes/WD Blue 1tb/VMs/data/gmx/futures" > /dev/null && echo DRIVE_OK
df -h /tmp "/Volumes/WD Blue 1tb" | awk 'NR==1 || /tmp|WD/'
```

Expected: `RPC_OK`, `HYPERSYNC_OK`, `DRIVE_OK`, ≥20 GB free on both. If `HYPERSYNC_MISSING`, stop and ask the user (needed for BONK/SATS oracle re-walk; PEPE/SHIB Chainlink work can proceed without it).

- [ ] **Step 0.2: Create tmp workspace + inventory manifest (all three stores)**

```bash
TMP=/tmp/gmx_verify_2026-07-08
mkdir -p $TMP/{collect,export,staging,cex,reports,scripts}
DRIVE="/Volumes/WD Blue 1tb/VMs/data/gmx"
{ ls -la "$DRIVE/futures" | grep -iE 'pepe|bonk|sats|shib'
  for t in PEPE SHIB BONK SATS; do
    ls -la "user_data/data/gmx/candles/arbitrum/$t" "$DRIVE/candles/arbitrum/$t" 2>&1
  done
  ls -la user_data/data/gmx/checkpoints/{pepe,shib,bonk,sats}_checkpoint.json
  md5 "$DRIVE/futures/"{PEPE,SHIB,BONK,SATS}_USDC_USDC-1h-futures.feather
} > $TMP/manifest_before.txt
wc -l $TMP/manifest_before.txt
```

Expected: manifest with ~90+ lines — the before-state record for the final report.

### Task 1: Review the fix branch (read-only — no merge yet)

**Files:** none modified.

- [ ] **Step 1.1: Inspect the fix commit**

```bash
git log --oneline -3 feat/chainlink-decimals-fix
git show --stat b6c43ff
git diff master..feat/chainlink-decimals-fix -- src/gmx_historical_data/cli.py | head -80
```

Expected: commit `b6c43ff fix(chainlink): use on-chain feed decimals`; diff replaces the hard-coded `OHLCVResampler(decimals=8)` at `cli.py:146` with per-feed on-chain `decimals()` lookup; `chainlink_rpc_collector.py` gains `get_feed_decimals()` and `_filter_contaminated_phases`; new test `tests/test_chainlink_phase_contamination.py`.

- [ ] **Step 1.2: ⛔ USER APPROVAL (git worktree), then run the fix branch's tests from a worktree**

```bash
git worktree add /tmp/gmx-fix-branch-review feat/chainlink-decimals-fix
cd /tmp/gmx-fix-branch-review
export JSON_RPC_ARBITRUM=$ARBITRUM_CHAIN_JSON_RPC
poetry run pytest tests/test_chainlink_phase_contamination.py tests/test_chainlink_symbol_mapper.py -v
cd /Users/avik/Work/tradingstrategy/gmx_historical_data_new
```

Expected: all PASS.

### Task 2: Merge the fix — ⛔ USER APPROVAL REQUIRED

**Files:** `src/gmx_historical_data/cli.py`, `src/gmx_historical_data/chainlink_rpc_collector.py`, `tests/test_chainlink_phase_contamination.py` (via merge).

- [ ] **Step 2.1: ⛔ STOP — ask the user:** "Merge `feat/chainlink-decimals-fix` (b6c43ff) into master (fast-forward)? Without it, every collect/export re-corrupts PEPE/SHIB." Do not proceed without a yes.

- [ ] **Step 2.2: Merge (fast-forward) and clean up the review worktree**

```bash
git merge --ff-only feat/chainlink-decimals-fix
git worktree remove /tmp/gmx-fix-branch-review
git log --oneline -2
```

Expected: `Fast-forward`; HEAD is now `b6c43ff`.

- [ ] **Step 2.3: Verify the bug is gone at source + run test suite**

```bash
grep -n "OHLCVResampler(" src/gmx_historical_data/cli.py
export JSON_RPC_ARBITRUM=$ARBITRUM_CHAIN_JSON_RPC
poetry run pytest tests/ -x -q 2>&1 | tail -15
```

Expected: no hard-coded `decimals=8` in the resampler construction; suite passes (compare any failures against a pre-merge master run before blaming the merge).

### Task 3: Price-continuity validator (TDD)

Durable tooling: detects decade-folds (PEPE defect), unit cliffs (SHIB defect), grid gaps (BONK/SATS defect). Acceptance gate for Chunks 2–3. Handles both schemas: freqtrade files (`date` col) and source parquets (`timestamp` col, filenames like `1h.parquet`).

**Files:**
- Create: `scripts/validate_price_continuity.py`
- Test: `tests/test_validate_price_continuity.py`

- [ ] **Step 3.1: Write the failing test**

```python
"""Tests for scripts/validate_price_continuity.py."""

import importlib.util
from pathlib import Path

import pandas as pd

_SPEC = importlib.util.spec_from_file_location(
    "validate_price_continuity",
    Path(__file__).parent.parent / "scripts" / "validate_price_continuity.py",
)
vpc = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(vpc)


def _frame(closes, start="2024-01-01", freq="1h", ts_col="date"):
    idx = pd.date_range(start, periods=len(closes), freq=freq, tz="UTC")
    return pd.DataFrame(
        {ts_col: idx, "open": closes, "high": closes, "low": closes,
         "close": closes, "volume": 0.0}
    )


def test_clean_series_passes():
    report = vpc.validate_frame(_frame([1.0e-5, 1.1e-5, 1.05e-5, 0.95e-5]), timeframe="1h")
    assert report.decade_jumps == 0
    assert report.missing_bars == 0
    assert report.ok


def test_timestamp_column_accepted():
    df = _frame([1.0e-5, 1.1e-5, 1.05e-5], ts_col="timestamp")
    report = vpc.validate_frame(df, timeframe="1h")
    assert report.ok


def test_decade_fold_detected():
    df = _frame([2.1e-5, 2.2e-6, 2.15e-5, 2.3e-6])  # PEPE-style 10x flips
    report = vpc.validate_frame(df, timeframe="1h")
    assert report.decade_jumps >= 2
    assert not report.ok


def test_unit_cliff_detected():
    df = _frame([1.9e5, 1.9e5, 1.9e-5, 2.0e-5])  # SHIB-style 1e10 cliff
    report = vpc.validate_frame(df, timeframe="1h")
    assert report.decade_jumps >= 1
    assert not report.ok


def test_grid_gap_detected():
    df = _frame([1.0e-5, 1.0e-5, 1.0e-5, 1.0e-5])
    df = df.drop(index=[1, 2]).reset_index(drop=True)  # BONK-style holes
    report = vpc.validate_frame(df, timeframe="1h")
    assert report.missing_bars == 2
    assert not report.ok


def test_zero_and_nan_flagged():
    df = _frame([1.0e-5, 0.0, float("nan"), 1.0e-5])
    report = vpc.validate_frame(df, timeframe="1h")
    assert report.zero_or_nan == 2
    assert not report.ok


def test_timeframe_from_name():
    assert vpc._timeframe_from_name(Path("PEPE_USDC_USDC-1h-futures.feather")) == "1h"
    assert vpc._timeframe_from_name(Path("1h.parquet")) == "1h"
    assert vpc._timeframe_from_name(Path("1m.parquet")) == "1m"
```

- [ ] **Step 3.2: Run test to verify it fails**

```bash
export JSON_RPC_ARBITRUM=$ARBITRUM_CHAIN_JSON_RPC
poetry run pytest tests/test_validate_price_continuity.py -v
```

Expected: FAIL — module load error (script doesn't exist yet).

- [ ] **Step 3.3: Implement the validator**

```python
"""Validate OHLCV candle files for unit-scaling corruption and grid gaps.

Detects the three defect classes found in the PEPE/BONK/SATS/SHIB forensics:

* decade folds — adjacent closes jumping by ~10x/100x (piecewise-wrong divisor)
* unit cliffs — a single 1e10-style regime switch mid-series
* grid gaps — missing bars on the forward-filled timestamp grid

Accepts freqtrade files (``date`` column, ``PAIR-1h-futures.feather``) and
source candle parquets (``timestamp`` column, ``1h.parquet``).

Usage::

    poetry run python scripts/validate_price_continuity.py FILE [FILE ...] \
        [--jump-ratio 5.0] [--timeframe 1h] [--json]

Exit code 0 if every file passes, 1 otherwise.

:author: gmx_historical_data maintainers
"""

import argparse
import dataclasses
import json
import sys
from pathlib import Path

import pandas as pd

TIMEFRAME_DELTAS = {
    "1m": pd.Timedelta(minutes=1),
    "5m": pd.Timedelta(minutes=5),
    "15m": pd.Timedelta(minutes=15),
    "1h": pd.Timedelta(hours=1),
    "4h": pd.Timedelta(hours=4),
    "1d": pd.Timedelta(days=1),
}


@dataclasses.dataclass
class ValidationReport:
    """Result of validating one candle DataFrame.

    :param path: source file path (empty for in-memory frames)
    :param rows: row count
    :param decade_jumps: adjacent-close ratios beyond the jump threshold
    :param missing_bars: bars absent from the regular timeframe grid
    :param zero_or_nan: closes that are 0, negative, or NaN
    :param duplicate_ts: duplicated timestamps
    :param first: first timestamp (ISO) or ""
    :param last: last timestamp (ISO) or ""
    :param ok: True when all defect counters are zero
    """

    path: str
    rows: int
    decade_jumps: int
    missing_bars: int
    zero_or_nan: int
    duplicate_ts: int
    first: str
    last: str
    ok: bool


def validate_frame(
    df: pd.DataFrame, timeframe: str, jump_ratio: float = 5.0, path: str = ""
) -> ValidationReport:
    """Validate one OHLCV frame.

    :param df: candle frame with a UTC ``date`` or ``timestamp`` column
    :param timeframe: one of 1m/5m/15m/1h/4h/1d
    :param jump_ratio: adjacent-close ratio treated as a decade jump
    :param path: label for the report
    :return: populated :class:`ValidationReport`
    """
    ts_col = "date" if "date" in df.columns else "timestamp"
    df = df.sort_values(ts_col).reset_index(drop=True)
    close = df["close"]
    zero_or_nan = int(((close <= 0) | close.isna()).sum())
    duplicate_ts = int(df[ts_col].duplicated().sum())

    valid = close[close > 0]
    ratio = valid / valid.shift(1)
    decade_jumps = int(((ratio > jump_ratio) | (ratio < 1 / jump_ratio)).sum())

    delta = TIMEFRAME_DELTAS[timeframe]
    if len(df) > 1:
        span = df[ts_col].iloc[-1] - df[ts_col].iloc[0]
        expected = int(span / delta) + 1
        missing_bars = max(0, expected - df[ts_col].nunique())
    else:
        missing_bars = 0

    ok = decade_jumps == 0 and missing_bars == 0 and zero_or_nan == 0 and duplicate_ts == 0
    return ValidationReport(
        path=path,
        rows=len(df),
        decade_jumps=decade_jumps,
        missing_bars=missing_bars,
        zero_or_nan=zero_or_nan,
        duplicate_ts=duplicate_ts,
        first=str(df[ts_col].iloc[0]) if len(df) else "",
        last=str(df[ts_col].iloc[-1]) if len(df) else "",
        ok=ok,
    )


def _timeframe_from_name(path: Path) -> str:
    """Infer timeframe from a candle filename.

    :param path: e.g. ``PEPE_USDC_USDC-1h-futures.feather`` or ``1h.parquet``
    :return: timeframe token
    :raises ValueError: when no known timeframe token is present
    """
    stem = path.stem
    for tf in TIMEFRAME_DELTAS:
        if stem == tf or f"-{tf}-" in stem or stem.endswith(f"-{tf}"):
            return tf
    raise ValueError(f"cannot infer timeframe from {path.name}")


def main() -> int:
    """CLI entry point.

    :return: process exit code (0 = all files ok)
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("files", nargs="+", type=Path)
    parser.add_argument("--jump-ratio", type=float, default=5.0)
    parser.add_argument("--timeframe", default=None, help="override inferred timeframe")
    parser.add_argument("--json", action="store_true", dest="as_json")
    args = parser.parse_args()

    reports = []
    for f in args.files:
        df = pd.read_feather(f) if f.suffix == ".feather" else pd.read_parquet(f)
        tf = args.timeframe or _timeframe_from_name(f)
        reports.append(validate_frame(df, tf, args.jump_ratio, path=str(f)))

    if args.as_json:
        print(json.dumps([dataclasses.asdict(r) for r in reports], indent=2))
    else:
        for r in reports:
            flag = "OK  " if r.ok else "FAIL"
            print(
                f"{flag} {Path(r.path).name}: rows={r.rows} jumps={r.decade_jumps} "
                f"missing={r.missing_bars} zero/nan={r.zero_or_nan} dupes={r.duplicate_ts} "
                f"[{r.first} .. {r.last}]"
            )
    return 0 if all(r.ok for r in reports) else 1


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 3.4: Run tests to verify they pass**

```bash
poetry run pytest tests/test_validate_price_continuity.py -v
```

Expected: 7 passed.

- [ ] **Step 3.5: Smoke-test against known-bad and known-good files (read-only)**

```bash
DRIVE="/Volumes/WD Blue 1tb/VMs/data/gmx"
poetry run python scripts/validate_price_continuity.py \
  "$DRIVE/futures/PEPE_USDC_USDC-1h-futures.feather" \
  "$DRIVE/futures/SHIB_USDC_USDC-1h-futures.feather" \
  "$DRIVE/futures/BONK_USDC_USDC-1h-futures.feather" \
  "$DRIVE/futures/SATS_USDC_USDC-1h-futures.feather" \
  "$DRIVE/futures/ETH_USDC_USDC-1h-futures.feather" \
  user_data/data/gmx/candles/arbitrum/PEPE/1h.parquet \
  user_data/data/gmx/candles/arbitrum/BONK/1h.parquet
```

Expected (must reproduce forensics): PEPE feather FAIL (jumps≈29), SHIB feather OK, BONK feather FAIL (missing≈190), SATS feather FAIL (missing≈1048), ETH OK, PEPE source parquet FAIL (unit cliff), BONK source parquet OK-or-gaps-only. Exit 1.

- [ ] **Step 3.6: ⛔ USER APPROVAL, then commit**

```bash
git add scripts/validate_price_continuity.py tests/test_validate_price_continuity.py
git commit -m "feat(validation): price-continuity validator (decade folds, unit cliffs, grid gaps)"
```

## Chunk 2: Fresh tmp collection + staged repair + cross-verification

Everything in this chunk writes ONLY under `$TMP`. The drive and `user_data/` are read-only inputs.

### Task 4: Chainlink re-collect PEPE + SHIB into tmp

- [ ] **Step 4.1: Full re-collect with the fixed code**

```bash
export JSON_RPC_ARBITRUM=$ARBITRUM_CHAIN_JSON_RPC
TMP=/tmp/gmx_verify_2026-07-08
poetry run python -m gmx_historical_data.cli collect --full --force \
  --symbol PEPE,SHIB --output-dir $TMP/collect --concurrency 4 \
  --log-file $TMP/reports/collect_pepe_shib.log
```

Expected: exit 0; `$TMP/collect/candles/arbitrum/{PEPE,SHIB}/{1m,5m,15m,1h,4h,1d}.parquet` created; checkpoints in `$TMP/collect/checkpoints/`. Runtime: tens of minutes (full multi-phase Chainlink backfill).

- [ ] **Step 4.2: Validate continuity + magnitude of the fresh collection**

```bash
poetry run python scripts/validate_price_continuity.py \
  $TMP/collect/candles/arbitrum/PEPE/1h.parquet \
  $TMP/collect/candles/arbitrum/SHIB/1h.parquet \
  $TMP/collect/candles/arbitrum/PEPE/1m.parquet \
  $TMP/collect/candles/arbitrum/SHIB/1m.parquet
```

Expected: all OK, zero jumps. Sanity: PEPE close ∈ [6e-7, 3e-5], SHIB ∈ [4e-6, 4e-5] (print with pandas if in doubt). Any value > 1e-3 → decimals fix not active; STOP, re-check Task 2.

- [ ] **Step 4.3: Depth check vs existing drive feathers**

```bash
poetry run python - <<'EOF'
import pandas as pd

targets = {"PEPE": "2023-07-21 16:00", "SHIB": "2024-09-26 17:00"}
for sym, want in targets.items():
    df = pd.read_parquet(f"/tmp/gmx_verify_2026-07-08/collect/candles/arbitrum/{sym}/1h.parquet")
    print(sym, "fresh_start:", df["timestamp"].min(), "target:<=", want,
          "fresh_end:", df["timestamp"].max())
EOF
```

Expected: fresh start ≤ target. **If fresh history is shallower** (Chainlink phases don't reach that far back), record it — Task 8 then uses the fallback repair (uniform-rescale of `.bak`, Step 8.4) for the head segment.

### Task 5: Oracle-event re-walk BONK + SATS into tmp

- [ ] **Step 5.1: Full oracle re-walk (HyperSync)**

```bash
poetry run python -m gmx_historical_data.cli collect --full --force \
  --symbol BONK,SATS --all-markets --output-dir $TMP/collect --concurrency 2 \
  --log-file $TMP/reports/collect_bonk_sats.log
```

Expected: exit 0; `$TMP/collect/candles/arbitrum/{BONK,SATS}/*.parquet` created.

- [ ] **Step 5.2: Coverage + price-agreement audit vs drive feathers**

```bash
poetry run python - <<'EOF'
import pandas as pd

DRIVE = "/Volumes/WD Blue 1tb/VMs/data/gmx/futures"
TMP = "/tmp/gmx_verify_2026-07-08"
for sym in ("BONK", "SATS"):
    fresh = pd.read_parquet(f"{TMP}/collect/candles/arbitrum/{sym}/1h.parquet")
    fresh = fresh.rename(columns={"timestamp": "date"})
    drive = pd.read_feather(f"{DRIVE}/{sym}_USDC_USDC-1h-futures.feather")
    m = fresh.merge(drive, on="date", suffixes=("_f", "_d"))
    rel = ((m["close_f"] - m["close_d"]).abs() / m["close_d"]).max()
    print(sym, "fresh_rows:", len(fresh), "drive_rows:", len(drive),
          "common:", len(m), "max_rel_diff:", rel,
          "fresh_range:", fresh["date"].min(), "->", fresh["date"].max())
EOF
```

Expected: `max_rel_diff` < 0.01 on common timestamps (same oracle source). Record fresh coverage — how much of the BONK/SATS holes the oracle re-walk actually fills. If fresh coverage ≈ the thin GMX-API window only (per `README.md:88-93`), the deep holes must be CEX-filled (Task 6).

### Task 6: Build + verify the staged repair in tmp

The gap-filler reads/writes **source parquets** at `{data_dir}/candles/arbitrum/<SYM>/<tf>.parquet` (`cex_gap_fill/orchestrator.py:86`); the exporter then propagates to feathers. BONK/SATS live source parquets only start 2025-12-18, so deep staging parquets must be derived from the (price-correct) drive feathers first.

- [ ] **Step 6.1: Stage PEPE/SHIB (fresh collection) + BONK/SATS (feather-derived, oracle-merged) source parquets**

```bash
mkdir -p $TMP/staging/candles/arbitrum
cp -R $TMP/collect/candles/arbitrum/PEPE $TMP/collect/candles/arbitrum/SHIB \
      $TMP/staging/candles/arbitrum/
poetry run python - <<'EOF'
import pathlib

import pandas as pd

DRIVE = pathlib.Path("/Volumes/WD Blue 1tb/VMs/data/gmx/futures")
TMP = pathlib.Path("/tmp/gmx_verify_2026-07-08")

for sym in ("BONK", "SATS"):
    out_dir = TMP / "staging/candles/arbitrum" / sym
    out_dir.mkdir(parents=True, exist_ok=True)
    for tf in ("1m", "5m", "15m", "1h", "4h", "1d"):
        # deep, price-correct history (with holes) from the drive feather
        deep = pd.read_feather(DRIVE / f"{sym}_USDC_USDC-{tf}-futures.feather")
        deep = deep.rename(columns={"date": "timestamp"})[
            ["timestamp", "open", "high", "low", "close"]
        ]
        # overlay fresh oracle re-walk rows where available (better provenance)
        fresh_path = TMP / "collect/candles/arbitrum" / sym / f"{tf}.parquet"
        if fresh_path.exists():
            fresh = pd.read_parquet(fresh_path)[
                ["timestamp", "open", "high", "low", "close"]
            ]
            deep = pd.concat([deep, fresh]).drop_duplicates(
                subset="timestamp", keep="last"
            )
        deep = deep.sort_values("timestamp").reset_index(drop=True)
        deep["symbol"] = sym
        deep["volume"] = 0.0
        deep.to_parquet(out_dir / f"{tf}.parquet", index=False)
        print(sym, tf, len(deep), deep["timestamp"].min(), "->", deep["timestamp"].max())
EOF
```

Expected: staged parquets for all 4 tokens × 6 timeframes; BONK/SATS spans match the drive feathers' full range.

- [ ] **Step 6.2: CEX gap-fill dry run (verification of routing + gap detection)**

```bash
poetry run python -m gmx_historical_data.cli fill-gaps-cex \
  --data-dir $TMP/staging --symbol PEPE,SHIB,BONK,SATS \
  --routing-file configs/cex_routing.json --cex-datadir $TMP/cex \
  --dry-run --log-dir $TMP/reports
```

Expected: routing resolves to 1000PEPE/1000SHIB/1000BONK/1000SATS (scale 1000, `cex_gap_fill/symbols.py:11-23`); gap counts for BONK/SATS in the same order as forensics (~130 windows on 1h, thousands on 1m); NO large gap/mismatch count for fresh PEPE/SHIB. Note: the CEX download itself runs even in dry-run (only parquet writes are skipped) and needs `./freqtrade-gmx` + network; it may take a while.

- [ ] **Step 6.3: Real CEX gap-fill into staging (reuses Step 6.2's downloads)**

```bash
poetry run python -m gmx_historical_data.cli fill-gaps-cex \
  --data-dir $TMP/staging --symbol BONK,SATS \
  --routing-file configs/cex_routing.json --cex-datadir $TMP/cex \
  --skip-download --log-dir $TMP/reports
poetry run python scripts/validate_price_continuity.py \
  $TMP/staging/candles/arbitrum/BONK/1h.parquet \
  $TMP/staging/candles/arbitrum/BONK/1m.parquet \
  $TMP/staging/candles/arbitrum/SATS/1h.parquet \
  $TMP/staging/candles/arbitrum/SATS/1m.parquet
```

Expected: missing_bars 0 or dramatically reduced (CEX pairs list later than GMX; residual head-gaps acceptable — record numbers), decade_jumps = 0 (proves the 1000x scale applied; a missed scale = 1000x cliff at every fill boundary).

- [ ] **Step 6.4: Cross-check PEPE/SHIB vs `.bak`-derived expectation**

The `.bak` files are internally consistent raw data: pre-2025-10-16 rows = human price ×1e10, post = human. So `bak_close / 1e10` (pre-cliff) must match the fresh Chainlink series within cross-source tolerance.

```bash
poetry run python - <<'EOF'
import pandas as pd

DRIVE = "/Volumes/WD Blue 1tb/VMs/data/gmx/futures"
TMP = "/tmp/gmx_verify_2026-07-08"
CLIFFS = {"PEPE": "2025-10-16 17:00", "SHIB": "2025-10-16 15:00"}  # from forensics
for sym, cliff in CLIFFS.items():
    bak = pd.read_feather(f"{DRIVE}/{sym}_USDC_USDC-1h-futures.feather.bak")
    pre = bak["date"] < pd.Timestamp(cliff, tz="UTC")
    bak.loc[pre, ["open", "high", "low", "close"]] /= 1e10
    fresh = pd.read_parquet(f"{TMP}/staging/candles/arbitrum/{sym}/1h.parquet")
    fresh = fresh.rename(columns={"timestamp": "date"})
    m = bak.merge(fresh, on="date", suffixes=("_b", "_f"))
    rel = (m["close_b"] - m["close_f"]).abs() / m["close_f"]
    print(sym, "common:", len(m), "median_rel:", rel.median(),
          "p99_rel:", rel.quantile(0.99))
EOF
```

Expected: median_rel < 0.01, p99 < 0.05 (Chainlink vs GMX oracle — small divergence normal; 10x disagreement is not).

- [ ] **Step 6.5: Cross-check vs `data_fix_test2/` (the Jul-2 fixed re-collect)**

```bash
poetry run python - <<'EOF'
import pandas as pd

for sym in ("PEPE", "SHIB"):
    old = pd.read_parquet(f"data_fix_test2/candles/arbitrum/{sym}/1h.parquet")
    new = pd.read_parquet(
        f"/tmp/gmx_verify_2026-07-08/collect/candles/arbitrum/{sym}/1h.parquet"
    )
    m = old.merge(new, on="timestamp", suffixes=("_o", "_n"))
    rel = ((m["close_o"] - m["close_n"]).abs() / m["close_n"]).max()
    print(sym, "common:", len(m), "max_rel:", rel)
EOF
```

Expected: max_rel ≈ 0 (same source, same fixed code). Material disagreement → investigate before proceeding. (If `data_fix_test2` columns differ, adapt the merge key — inspect with `df.columns` first.)

### Task 7: Export staged data to freqtrade files in tmp + verification report — ⛔ MANUAL REVIEW GATE

- [ ] **Step 7.1: Export staged candles to isolated freqtrade output (feather AND parquet — `--format` takes one value per run)**

```bash
for FMT in feather parquet; do
  poetry run python -m gmx_historical_data.cli export-candles \
    --data-dir $TMP/staging --output-dir $TMP/export \
    --symbol PEPE --symbol SHIB --symbol BONK --symbol SATS --format $FMT
done
ls $TMP/export/gmx/futures/ | wc -l
```

Expected: `{PEPE,SHIB,BONK,SATS}_USDC_USDC-<tf>-{futures,index,mark}.{feather,parquet}` — 4 tokens × 6 tf × 3 kinds × 2 formats = 144 files under `$TMP/export/gmx/futures/` (exporter writes to `{output-dir}/gmx/futures/`).

- [ ] **Step 7.2: Validate every exported 1m/1h file + invariants**

```bash
poetry run python scripts/validate_price_continuity.py \
  $TMP/export/gmx/futures/{PEPE,SHIB,BONK,SATS}_USDC_USDC-1h-futures.feather \
  $TMP/export/gmx/futures/{PEPE,SHIB,BONK,SATS}_USDC_USDC-1m-futures.feather
poetry run python - <<'EOF'
import pandas as pd

ROOT = "/tmp/gmx_verify_2026-07-08/export/gmx/futures"
for sym in ("PEPE", "SHIB", "BONK", "SATS"):
    fut = pd.read_feather(f"{ROOT}/{sym}_USDC_USDC-1h-futures.feather")
    idx = pd.read_feather(f"{ROOT}/{sym}_USDC_USDC-1h-index.feather")
    mrk = pd.read_feather(f"{ROOT}/{sym}_USDC_USDC-1h-mark.feather")
    assert list(fut.columns) == ["date", "open", "high", "low", "close", "volume"]
    assert fut["close"].equals(idx["close"]) and fut["close"].equals(mrk["close"])
    pq = pd.read_parquet(f"{ROOT}/{sym}_USDC_USDC-1h-futures.parquet")
    assert len(pq) == len(fut)
    print(sym, "OK", len(fut), fut["date"].min(), "->", fut["date"].max())
EOF
```

Expected: validator OK for all (BONK/SATS may report recorded residual head-gaps — compare against Step 6.3 numbers); invariants hold (index==mark==futures, feather==parquet).

- [ ] **Step 7.3: Write `$TMP/reports/verification_report.md`** — per token: fresh-collection depth vs drive depth; validator before (drive/live) vs after (staged/exported) tables; `.bak` reconciliation stats; `data_fix_test2` agreement; CEX fill stats (bars filled, residual gaps); chosen repair route for Chunk 3 (fresh-replace vs `.bak`-rescale fallback for PEPE/SHIB; staged gap-filled files for BONK/SATS).

- [ ] **Step 7.4: ⛔ STOP — present the report to the user for manual review.** Do not start Chunk 3 without explicit approval. Nothing outside `$TMP` has been modified so far.

## Chunk 3: Guarded live-store + drive repair, final verification

### Task 8: Backups, then replace PEPE/SHIB artifacts

- [ ] **Step 8.1: Create backups (copy, never move) — drive files AND live local store**

```bash
DRIVE="/Volumes/WD Blue 1tb/VMs/data/gmx"
BK="$DRIVE/futures_backup_2026-07-08"; BKC="$DRIVE/candles_decimalsfix_backup_2026-07-08"
mkdir -p "$BK" "$BKC/live_candles" "$BKC/drive_candles" "$BKC/checkpoints"
for t in PEPE SHIB BONK SATS; do
  cp -p "$DRIVE/futures/"${t}_USDC_USDC-* "$BK/"                       # feathers+parquets+.bak
  cp -pR "user_data/data/gmx/candles/arbitrum/$t" "$BKC/live_candles/"  # live source parquets
  cp -pR "$DRIVE/candles/arbitrum/$t" "$BKC/drive_candles/"             # drive snapshot parquets
done
cp -p user_data/data/gmx/checkpoints/{pepe,shib,bonk,sats}_checkpoint.json "$BKC/checkpoints/"
ls "$BK" | wc -l; du -sh "$BK" "$BKC"
```

Expected: `$BK` file count matches the Step 0.2 manifest for `futures/` (glob catches `.bak` files too — intended). Note `PEPE_ETH-USDC_USDC_USDC-*` funding files are NOT matched — they are never mutated by this plan.

- [ ] **Step 8.2: Verify backup integrity before any mutation**

```bash
for t in PEPE SHIB BONK SATS; do
  md5 "$DRIVE/futures/${t}_USDC_USDC-1h-futures.feather" \
      "$BK/${t}_USDC_USDC-1h-futures.feather"
done
```

Expected: hash pairs identical for all four tokens. Any mismatch → STOP.

- [ ] **Step 8.3: Replace live + drive source parquets and checkpoints (kills the re-poison source)**

```bash
TMP=/tmp/gmx_verify_2026-07-08
for t in PEPE SHIB; do
  cp -p $TMP/staging/candles/arbitrum/$t/*.parquet user_data/data/gmx/candles/arbitrum/$t/
  cp -p $TMP/staging/candles/arbitrum/$t/*.parquet "$DRIVE/candles/arbitrum/$t/"
done
for s in pepe shib; do
  cp -p $TMP/collect/checkpoints/${s}_checkpoint.json user_data/data/gmx/checkpoints/
done
poetry run python scripts/validate_price_continuity.py \
  user_data/data/gmx/candles/arbitrum/PEPE/1h.parquet \
  user_data/data/gmx/candles/arbitrum/SHIB/1h.parquet
```

Expected: OK / exit 0.

- [ ] **Step 8.4: Replace PEPE/SHIB exported feathers + parquets on the drive**

Primary route (fresh history ≥ existing depth, confirmed at the Task 7 gate):

```bash
cp -p $TMP/export/gmx/futures/PEPE_USDC_USDC-* "$DRIVE/futures/"
cp -p $TMP/export/gmx/futures/SHIB_USDC_USDC-* "$DRIVE/futures/"
```

Fallback route (fresh history shallower — decided at the Task 7 gate): rebuild each feather as `concat(uniform_rescaled_bak_head, fresh_tail)` — rescale `.bak` rows before the per-token 2025-10-16 unit cliff (PEPE 17:00, SHIB 15:00 UTC) by exactly 1e10, take fresh rows from cliff onward, dedupe on `date` keeping fresh, write index/mark as copies of futures, feather with zstd compression (match `freqtrade_exporter.py:483`). Implement as `$TMP/scripts/splice_bak_fresh.py`, validate outputs with the validator BEFORE copying to the drive, then copy as above.

- [ ] **Step 8.5: Validate the drive state for PEPE/SHIB**

```bash
poetry run python scripts/validate_price_continuity.py \
  "$DRIVE/futures/PEPE_USDC_USDC-1h-futures.feather" \
  "$DRIVE/futures/PEPE_USDC_USDC-1m-futures.feather" \
  "$DRIVE/futures/SHIB_USDC_USDC-1h-futures.feather" \
  "$DRIVE/futures/SHIB_USDC_USDC-1m-futures.feather" \
  "$DRIVE/futures/SHIB_USDC_USDC-1m-index.feather" \
  "$DRIVE/futures/SHIB_USDC_USDC-1m-mark.feather"
ls "$DRIVE/futures/SHIB_USDC_USDC-15m-mark.parquet"
```

Expected: all OK — this replaces the previously unreadable SHIB 1m-mark and mixed-unit 1m-index; the previously missing SHIB 15m-mark.parquet now exists.

### Task 9: Apply BONK/SATS repair

- [ ] **Step 9.1: Replace BONK/SATS artifacts from validated staging/export**

```bash
for t in BONK SATS; do
  cp -p $TMP/staging/candles/arbitrum/$t/*.parquet user_data/data/gmx/candles/arbitrum/$t/
  cp -p $TMP/staging/candles/arbitrum/$t/*.parquet "$DRIVE/candles/arbitrum/$t/"
  cp -p $TMP/export/gmx/futures/${t}_USDC_USDC-* "$DRIVE/futures/"
done
```

(Direct copy of the already-validated exports — same artifacts a guarded merge-export would produce, but deterministic. BONK/SATS checkpoints are left untouched: the oracle re-walk used `--force` in tmp and live incremental collection state remains valid.)

- [ ] **Step 9.2: Validate BONK/SATS drive state**

```bash
poetry run python scripts/validate_price_continuity.py \
  "$DRIVE/futures/BONK_USDC_USDC-1h-futures.feather" \
  "$DRIVE/futures/BONK_USDC_USDC-1m-futures.feather" \
  "$DRIVE/futures/SATS_USDC_USDC-1h-futures.feather" \
  "$DRIVE/futures/SATS_USDC_USDC-1m-futures.feather"
```

Expected: matches the Step 6.3/7.2 approved numbers (0 or recorded-residual missing bars; 0 jumps).

### Task 10: Final verification + report — ⛔ MANUAL REVIEW GATE

- [ ] **Step 10.1: Full validator sweep — all four tokens, all timeframes, feather + parquet**

```bash
cd "$DRIVE/futures"
poetry run python /Users/avik/Work/tradingstrategy/gmx_historical_data_new/scripts/validate_price_continuity.py \
  {PEPE,SHIB,BONK,SATS}_USDC_USDC-{1m,5m,15m,1h,4h,1d}-futures.feather \
  {PEPE,SHIB,BONK,SATS}_USDC_USDC-1h-{index,mark}.feather
cd /Users/avik/Work/tradingstrategy/gmx_historical_data_new
```

Expected: exit 0 apart from the recorded BONK/SATS residual head-gaps.

- [ ] **Step 10.2: Consistency invariants + depth**

Short script asserting: (a) index/mark equal futures per token/timeframe; (b) feather vs parquet agree on common timestamps; (c) ranges: PEPE starts ≤ 2023-07-21 16:00, SHIB ≤ 2024-09-26 17:00, all four end ≥ 2026-06-29 (previous feather end).

- [ ] **Step 10.3: Freqtrade load smoke test**

```bash
poetry run python - <<'EOF'
import pandas as pd

for sym in ("PEPE", "SHIB", "BONK", "SATS"):
    df = pd.read_feather(
        f"/Volumes/WD Blue 1tb/VMs/data/gmx/futures/{sym}_USDC_USDC-1h-futures.feather"
    )
    assert list(df.columns) == ["date", "open", "high", "low", "close", "volume"], sym
    print(sym, "OK", len(df), df["date"].min(), "->", df["date"].max())
EOF
```

- [ ] **Step 10.4: Write `$TMP/reports/final_report.md`** — before/after manifest diff (`$TMP/manifest_before.txt` vs fresh listing), validator before/after tables, repair routes taken, residual known issues, backup locations (`$BK`, `$BKC`).

- [ ] **Step 10.5: ⛔ STOP — present final report for manual review.** Offer optional cleanup (delete `$TMP` only; NEVER the drive backups or `.bak` files) and ask whether to commit remaining repo changes (git only with approval).

---

## Out of scope (documented follow-ups)

1. Funding files: 1–2 months stale, bogus orphan row at 2026-06-21 12:06 in every 8h file, datastore→factor method-transition hole (2025-08-19 → 2025-09-09), "8h" files actually hourly-sampled.
2. Exported parquets for all OTHER tokens are stale (end 2026-06-16) — a full `make refresh-data` is a separate operation (safe only AFTER the decimals fix is merged).
3. Audit other Chainlink tokens' feed decimals (`chainlink_feeds_complete.py` vs on-chain `decimals()`) — any other non-8-decimal feed has the same historical corruption.
4. Drive `candles/` snapshot vs live `user_data/data/gmx/candles/` dual-store: consider making one canonical (symlink like `futures/`) to prevent future divergence.

---

## Plan review notes (inline review, 2026-07-08)

Subagent reviewers unavailable (weekly limit); an inline verification pass against the repo corrected: source-parquet column is `timestamp` not `date` (validator + all snippets), `--format both` doesn't exist (loop feather/parquet, mirroring `export-candles-both`), `fill-gaps-cex` operates on source parquets at `{data-dir}/candles/arbitrum/` not feathers (staging layout reworked), exporter writes to `{output-dir}/gmx/futures/`, live source store is repo-local `user_data/data/gmx/candles/` (NOT the drive copy) and PEPE/SHIB are corrupt there too, BONK/SATS source parquets are shallow (2025-12-18+) so staging derives deep history from drive feathers, live checkpoints live at `user_data/data/gmx/checkpoints/`.
