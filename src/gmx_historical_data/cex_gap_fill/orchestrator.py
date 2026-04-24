"""Top-level orchestrator: iterates symbols/timeframes, invokes sub-modules, writes parquet."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from gmx_historical_data.config import TIMEFRAME_TO_FILENAME

from .detector import DetectorConfig, detect_gaps
from .freqtrade_runner import CEXDownloadError, resolve_feather_path, run_download
from .logging_utils import RunSummary, configure_run_logger, make_run_id, write_summary_json
from .reconciler import (
    assert_history_preserved,
    reconcile,
    warn_seam_discontinuities,
)
from .router import RoutingTable, load_routing, save_routing

log = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[4]
DEFAULT_TIMEFRAMES = ["1min", "5min", "15min", "1h", "4h", "1d"]


def fill_gaps_from_cex(
    data_dir: Path,
    symbols: list[str] | None,
    timeframes: list[str] | None,
    routing_file: Path,
    cex_datadir: Path | None,
    exchanges: list[str],
    gap_threshold: float,
    merge_gap_bars: int,
    log_dir: Path,
    dry_run: bool,
    skip_download: bool = False,
    download_timeout: int = 1800,
    download_start: str = "20230801",
    network: str = "arbitrum",
) -> RunSummary:
    """Run the full CEX gap-fill stage.

    Reads GMX parquets, detects gaps, downloads CEX data via ``./freqtrade-gmx``,
    reconciles bars, writes corrected parquets.  The feather exporter downstream
    will pick up the corrections automatically.

    :param data_dir: Root data dir (same as ``collect --output-dir``). Parquets
        live at ``{data_dir}/candles/{network}/{SYMBOL}/{tf}.parquet``.
    :param symbols: Symbol whitelist; ``None`` = all symbols found on disk.
    :param timeframes: Timeframe whitelist; ``None`` = all six defaults.
    :param routing_file: Path to ``configs/cex_routing.json``.
    :param cex_datadir: Override freqtrade data dir; ``None`` = freqtrade default.
    :param exchanges: CEX venues to use, in preference order.
    :param gap_threshold: Fractional price-jump threshold for gap detection.
    :param merge_gap_bars: Max gap between flagged bars to merge into one range.
    :param log_dir: Directory for run log and summary JSON.
    :param dry_run: If ``True``, detect and log but do not write corrected parquets.
    :param skip_download: If ``True``, skip the freqtrade subprocess and reuse
        already-downloaded CEX feathers.
    :param download_timeout: Seconds before freqtrade subprocess is killed.
    :param download_start: ``YYYYMMDD`` start for freqtrade ``--timerange``.
    :param network: On-chain network identifier used in the parquet path.
    :returns: :class:`~logging_utils.RunSummary` with per-run statistics.
    """
    run_id = make_run_id()
    log_path = log_dir / f"cex_gap_fill_{run_id}.log"
    configure_run_logger(log_path)
    summary = RunSummary(run_id=run_id, started_at=datetime.now(UTC).isoformat())

    config = DetectorConfig(gap_pct_threshold=gap_threshold, merge_gap_bars=merge_gap_bars)
    tfs = timeframes or DEFAULT_TIMEFRAMES
    routing = load_routing(routing_file)

    candles_root = data_dir / "candles" / network
    if symbols is None:
        if candles_root.exists():
            symbols = sorted(p.name for p in candles_root.iterdir() if p.is_dir())
        else:
            symbols = []

    plan = _build_download_plan(symbols, routing, exchanges)

    if not skip_download:
        for exch, pairs in plan.items():
            if not pairs:
                continue
            try:
                run_download(
                    exchange=exch,
                    pairs=sorted(pairs),
                    timeframes=tfs,
                    timerange_start=download_start,
                    datadir=cex_datadir,
                    cwd=REPO_ROOT,
                    timeout=download_timeout,
                )
            except CEXDownloadError as err:
                log.error("download failed for %s: %s", exch, err)
                summary.errors.append(f"{exch}: {err}")

    for sym in symbols:
        route = routing.resolve(sym)
        if route is None or route.is_skip:
            summary.symbols_skipped_no_cex.append(sym)
            continue

        for tf in tfs:
            tf_filename = TIMEFRAME_TO_FILENAME.get(tf, tf)
            parquet_path = candles_root / sym / f"{tf_filename}.parquet"
            if not parquet_path.exists():
                continue

            gmx_df = pl.read_parquet(parquet_path)
            cex_path = _resolve_cex_feather(cex_datadir, route.exchange, route.pair, tf)
            cex_df = _load_cex_feather(cex_path, gmx_df)

            det = detect_gaps(gmx_df, tf=tf, config=config)
            corrected, stats = reconcile(gmx_df, cex_df, det, gmx_symbol=sym, config=config)
            assert_history_preserved(original=gmx_df, corrected=corrected)
            warn_seam_discontinuities(corrected, threshold=gap_threshold, gmx_symbol=sym)

            summary.totals["full_replaced"] += stats.full_replaced
            summary.totals["volume_replaced"] += stats.volume_replaced
            summary.totals["kept"] += stats.kept

            log.info(
                "SYMBOL=%s TF=%s exchange=%s full_replaced=%d vol_replaced=%d kept=%d",
                sym, tf, route.exchange, stats.full_replaced, stats.volume_replaced, stats.kept,
            )

            if not dry_run:
                corrected.write_parquet(parquet_path)

        summary.symbols_processed += 1

    save_routing(routing, routing_file)
    summary.finished_at = datetime.now(UTC).isoformat()
    write_summary_json(summary, log_dir / f"cex_gap_fill_{run_id}.summary.json")
    return summary


def _build_download_plan(
    symbols: list[str], routing: RoutingTable, exchanges: list[str]
) -> dict[str, set[str]]:
    plan: dict[str, set[str]] = {e: set() for e in exchanges}
    for sym in symbols:
        route = routing.resolve(sym)
        if route and not route.is_skip and route.exchange in plan:
            plan[route.exchange].add(route.pair)
    return plan


def _resolve_cex_feather(
    datadir: Path | None, exchange: str, pair: str, tf: str
) -> Path:
    if datadir is None:
        datadir = REPO_ROOT / "user_data" / "data"
    return resolve_feather_path(datadir=datadir, exchange=exchange, pair=pair, timeframe=tf)


def _load_cex_feather(path: Path, gmx_df: pl.DataFrame) -> pl.DataFrame:
    """Load a CEX feather file; return empty frame on missing/error."""
    if not path.exists():
        return pl.DataFrame(schema=gmx_df.schema)
    try:
        df = pl.read_ipc(path)
        if "date" in df.columns and "timestamp" not in df.columns:
            df = df.rename({"date": "timestamp"})
        return df
    except Exception as exc:
        log.warning("failed to load CEX feather %s: %s", path, exc)
        return pl.DataFrame(schema=gmx_df.schema)
