# Volume and trade ticks

What the `volume` column in a GMX candle actually means, and what is in the
tick tape beside it. Read this before using either number: several of the
values are correct only under a stated definition, and one of them
double-counts if you sum it the obvious way.

## Where volume comes from

GMX candles are reconstructed from oracle price prints, which carry no size.
That is why every bar shipped `volume=0.0` before this existed, and why the
GMX candle API still returns only `[timestamp, open, high, low, close]`.

Size lives on chain instead. The collector reads three events from the GMX v2
`EventEmitter` (`0xC8ee91A54287DB53897056e12D9819156D3822Fb`) via HyperSync:

| Event | Meaning | Fields used |
|---|---|---|
| `PositionIncrease` | A perp position opened or added to | `sizeDeltaUsd`, `sizeDeltaInTokens`, `executionPrice` |
| `PositionDecrease` | A perp position closed or reduced | same |
| `SwapInfo` | A GM-pool swap | `amountIn`/`amountOut`, `tokenInPrice`/`tokenOutPrice` |

## What `volume` means in the feather

**`volume` is perp base-token volume.** For `BTC_USDC_USDC-1h-futures.feather`
it is BTC traded on BTC perp markets in that hour, summing
`PositionIncrease` + `PositionDecrease` across every collateral variant of the
market (BTC has several).

Three consequences worth being explicit about:

- **Perp only.** GM-pool swaps are *not* included. A candle for BTC describes
  the BTC perpetual, and swaps are collateral movement rather than perp
  trading. This matches what GMX itself reports as `marginVolumeUsd`.
  Swap flow is still collected — it is in the tick tape and broken out in the
  sidecar, just not folded into this column. The policy is one constant,
  `CANDLE_VOLUME_KINDS` in `gmx_historical_data.candle_volume`.
- **Every fill counts once.** A position opened and later closed contributes
  twice, because that is two fills. This is the convention every centralised
  venue uses and therefore the one Freqtrade and NautilusTrader assume;
  making it one-sided would break cross-venue comparison silently.
- **Liquidations count.** They are real fills that moved real size, and
  excluding them would understate volume exactly when volatility makes it
  matter most.

The feather schema stays locked to the six `EXPORT_COLUMNS`
(`date, open, high, low, close, volume`) because Freqtrade reads them
positionally. USD notional therefore never enters the feather.

### Bars that will always read zero

Candles run back to 2021 on Chainlink oracle prices, but GMX v2 did not exist
until August 2023. No fill can be attributed to a bar before roughly block
120,000,000, so those bars keep `volume=0.0` permanently. An empty field is
honest; a fabricated one would not be.

Bars after that read zero until the backfill has covered them — see below.

## Files

```
user_data/data/gmx/
├── futures/{SYM}_USDC_USDC-{tf}-futures.feather   # volume column, base tokens
├── ticks/{date}.parquet                           # per-fill tape
└── tick_volume/{date}.parquet                     # per-symbol volume + USD
```

### `ticks/{date}.parquet` — the tape

One row per fill: `date`, `block_number`, `transaction_hash`, `log_index`,
`event_name`, `kind` (`perp`/`swap`), `symbol`, `market`, `price_usd`,
`size_tokens`, `size_usd`, `is_long`, `price_impact_usd`, `side`.

A row is uniquely identified by
`(transaction_hash, log_index, symbol, kind)` — **not** by
`(transaction_hash, log_index)`, because one `SwapInfo` log produces two rows.

`{date}` is the UTC date the fills **happened**, not the date the run happened.
A scan window straddles midnight — the daily cron runs at 02:00 UTC — so one
run routinely writes into two day files, and the same day is filled by two
different runs. Day files are therefore merged and deduplicated, never
overwritten, and candle volume is always recomputed from a day's *whole* file.
That is what makes a bar split across two runs come out complete: the runs do
not add to each other's numbers, they each re-derive the day from the tape.

### `tick_volume/{date}.parquet` — the sidecar

`symbol`, `date`, `volume`, `volume_usd`, `trades`, `timeframe`, `kind`.

Broken out per `kind` deliberately, so the perp-only-vs-perp-plus-swap
definition can be changed by a consumer, or by us, without re-collecting
anything.

## The one trap: swap USD double-counts

A `SwapInfo` log becomes **two** rows, one per token, each carrying the full
notional — a USDC→WETH swap is genuine flow in USDC *and* in WETH, and each
side is priced in its own token, so the two USD figures differ by the swap fee
and neither can be derived from the other.

That makes **per-symbol** sums correct and **protocol-wide** sums wrong by 2x.
Measured against GMX's own `swapVolumeUsd` for 2026-09-01, summing every swap
row gives a ratio of 1.999.

Filter to one `side` for a protocol total:

```python
protocol_swap_usd = ticks[(ticks.kind == "swap") & (ticks.side == "in")].size_usd.sum()
```

Perp rows have `side = None` and never double-count.

## Verification

Against GMX's own Subsquid `volumeInfos` for **2026-09-01**, the one fully
covered day in the sample:

| Metric | Ours | GMX | Ratio |
|---|---:|---:|---:|
| Perp (margin) USD | 114,276,121 | 114,276,121 | **1.000** |
| Swap USD (both sides) | 2,704,069 | 1,352,560 | 1.999 (by design, above) |

Both figures are reproduced by the **daily** path, driven as two runs split at
the 02:00 UTC cron seam: the day's `1d` sidecar summed and compared against
`marginVolumeUsd`. Each run re-derives the day from its whole tape rather than
adding to the other's numbers, so the seam costs nothing; re-running the apply
changes no bar.

Scaling is checked independently by comparing each fill's implied price to the
oracle's price for the same symbol: across 47 symbols in one sample, zero
mismatched by more than 1%.

## Running the backfill

The daily snapshot only fills volume for bars it collects. Everything older
needs the one-off backfill, which is resumable and safe to re-run:

```bash
# Resume from checkpoint (or genesis) and scan 20M blocks, then stop
PYTHONPATH=src poetry run python scripts/backfill_trade_ticks.py --max-blocks 20000000

# Recompute candle volume from tapes already on disk, without rescanning
PYTHONPATH=src poetry run python scripts/backfill_trade_ticks.py --apply-only
```

The checkpoint only advances after a chunk's tape is durably written, so an
interrupted run re-scans that chunk rather than skipping it. Full range from
genesis is roughly 380M blocks, on the order of a few hours.

Both paths file a fill under its UTC date and compute a bar from that one file,
and applying replaces the bar. A day file left behind by an older run that
named files after the *run* date therefore overwrites the correct value for
every day it overlaps, so delete such files before running `--apply-only`
rather than trusting the sort order.
