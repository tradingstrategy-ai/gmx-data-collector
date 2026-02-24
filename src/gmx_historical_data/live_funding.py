"""Live GMX V2 funding rate fetcher.

Fetches the current per-market funding rate from the GMX infra API and
upserts it as a single hourly row into FreqTrade feather files.
"""

from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import pyarrow.feather as feather
from eth_defi.gmx.api import GMXAPI

# GMX API returns annualized rates in 1e30 fixed-point.
# Divide by 1e30 to get fractional APR, then by hours-per-year to get per-hour.
_HOURS_PER_YEAR = 365.25 * 24
_GMX_PRECISION = 1e30


def fetch_live_funding_rates(network: str = "arbitrum") -> dict[str, float]:
    """Fetch current per-hour funding rates for all listed GMX markets.

    Calls :class:`eth_defi.gmx.api.GMXAPI` ``get_markets_info()`` and converts
    ``fundingRateLong`` (annualised, 1e30 fixed-point, signed) to a signed
    per-hour float. When multiple markets share a symbol (e.g. three ETH
    markets), the one with the highest combined open interest is used.

    :param network: GMX network slug — ``"arbitrum"`` or ``"avalanche"``.
    :returns: Mapping of ``{symbol: signed_hourly_rate}``, e.g.
        ``{"ETH": 1.055e-5, "BTC": -3.2e-6}``.
    """
    api = GMXAPI(chain=network)
    data = api.get_markets_info()
    markets = data.get("markets", [])

    # symbol -> (total_oi, hourly_rate)
    best: dict[str, tuple[int, float]] = {}

    for market in markets:
        if not market.get("isListed", True):
            continue

        name = market.get("name", "")
        # Skip swap-only markets — their names lack a "/" (e.g. "SWAP-ONLY [USDC-USDT]")
        if "/" not in name:
            continue
        symbol = name.split("/")[0].strip()
        if not symbol:
            continue

        try:
            hourly = int(market["fundingRateLong"]) / _GMX_PRECISION / _HOURS_PER_YEAR
            oi = int(market.get("openInterestLong", 0)) + int(market.get("openInterestShort", 0))
        except (KeyError, ValueError, TypeError):
            # fundingRateLong missing, non-numeric, or OI field is null — skip market
            continue

        if symbol not in best or oi > best[symbol][0]:
            best[symbol] = (oi, hourly)

    return {sym: rate for sym, (_, rate) in best.items()}


def _now_utc() -> datetime:
    """Return current UTC datetime. Extracted for testability.

    :returns: Current datetime in UTC.
    """
    return datetime.now(UTC)


def upsert_live_rates_to_feather(
    feather_dir: Path,
    rates: dict[str, float],
    market_filter: str | None = None,
) -> int:
    """Append or replace the current-hour funding rate in each feather file.

    Rounds the current UTC time down to the hour boundary and writes a
    one-row OHLCV-format row (``open`` = signed hourly rate, all other
    columns zero) into each
    ``{feather_dir}/data/gmx/futures/{SYM}_USDC_USDC-1h-funding_rate.feather``.
    An existing row at the same timestamp is replaced (idempotent on re-run).

    :param feather_dir: Root directory for feather files (e.g. FreqTrade
        ``user_data/``); the function looks under
        ``{feather_dir}/data/gmx/futures/``.
    :param rates: Mapping of ``{symbol: signed_hourly_rate}`` as returned by
        :func:`fetch_live_funding_rates`.
    :param market_filter: Optional market symbol filter, e.g. ``"ETH/USD"``.
        When set, only the base symbol is updated (``"ETH"``).
    :returns: Number of feather files successfully updated.
    """
    now = _now_utc()
    # Floor to hour boundary
    hour_ts = pd.Timestamp(now.year, now.month, now.day, now.hour, 0, 0, tz="UTC")

    # Apply optional market filter
    if market_filter:
        filter_sym = market_filter.split("/")[0].strip()
        rates = {s: r for s, r in rates.items() if s == filter_sym}

    gmx_dir = feather_dir / "data" / "gmx" / "futures"
    updated = 0

    for symbol, hourly_rate in rates.items():
        filepath = gmx_dir / f"{symbol}_USDC_USDC-1h-funding_rate.feather"
        if not filepath.exists():
            continue

        try:
            df = pd.read_feather(filepath)
        except Exception:
            continue

        # Build new row
        new_row = pd.DataFrame(
            [
                {
                    "date": hour_ts,
                    "open": float(hourly_rate),
                    "high": 0.0,
                    "low": 0.0,
                    "close": 0.0,
                    "volume": 0.0,
                }
            ]
        )

        # Normalise timezone to UTC so comparison works for both tz-aware and tz-naive files
        if df["date"].dt.tz is None:
            df["date"] = df["date"].dt.tz_localize("UTC")

        # Drop any existing row at the same timestamp, append, sort
        df = df[df["date"] != hour_ts]
        df = pd.concat([df, new_row], ignore_index=True)
        df = df.sort_values("date").reset_index(drop=True)
        df["date"] = df["date"].dt.as_unit("ns")

        feather.write_feather(df, filepath)
        updated += 1

    return updated
