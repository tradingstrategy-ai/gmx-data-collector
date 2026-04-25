"""CEX gap-fill pipeline stage.

Additive post-processing that fills GMX OHLCV price gaps and zero-volume bars
using Binance/Bybit data downloaded via the ./freqtrade-gmx wrapper.

Entry point: :func:`fill_gaps_from_cex`. Never imported by legacy code paths.
"""

from gmx_historical_data.cex_gap_fill.orchestrator import fill_gaps_from_cex  # noqa: F401

__all__ = ["fill_gaps_from_cex"]
