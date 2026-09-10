"""Decode GMX v2 trade events into ticks, and aggregate ticks into candle volume.

Candles in this project are reconstructed from oracle price prints, which
carry no size -- so every published bar shipped ``volume=0.0``. The size does
exist on chain: ``PositionIncrease``/``PositionDecrease`` carry the perp fill
(``sizeDeltaUsd``, ``sizeDeltaInTokens``) and ``SwapInfo`` carries GM-pool
swap flow. This module turns those decoded events into a flat tick tape and
sums that tape into per-symbol, per-bar volume.

Two encoding rules govern every number here, both verified against events
pulled live from Arbitrum on 2026-09-10:

- USD values are 30-decimal fixed point: ``usd = raw / 10**30``.
- Prices are 30-decimal fixed point *already divided by the token's own
  decimals*, so recovering a human price needs the token back:
  ``price = raw * 10**decimals / 10**30``. A USDC (6dp) price of
  ``999869800000000000000000`` is $0.99987; a WETH (18dp) price of
  ``2468955797890000`` is $2468.96.

Getting the decimals wrong silently scales volume by powers of ten, so an
event whose token or market is not in the supplied mappings is dropped rather
than guessed -- see :func:`ticks_from_event_data`.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Sequence
from dataclasses import dataclass

import pandas as pd
from eth_defi.gmx.events import GMXEventData

logger = logging.getLogger(__name__)

#: GMX encodes USD values as 30-decimal fixed point.
GMX_USD_PRECISION = 10**30

#: Position events -- one perp fill each.
PERP_EVENTS = ("PositionIncrease", "PositionDecrease")

#: GM-pool swap event.
SWAP_EVENT = "SwapInfo"

#: Tick kinds, usable as an ``aggregate_tick_volume`` filter.
PERP_KIND = "perp"
SWAP_KIND = "swap"

#: Candle timeframe -> pandas resample alias, matching the candle filenames.
_TIMEFRAME_TO_PANDAS: dict[str, str] = {
    "1m": "1min",
    "5m": "5min",
    "15m": "15min",
    "1h": "1h",
    "4h": "4h",
    "1d": "1D",
}

_VOLUME_COLUMNS = ["symbol", "date", "volume", "volume_usd", "trades"]

_TICK_COLUMNS = [
    "date",
    "block_number",
    "transaction_hash",
    "log_index",
    "event_name",
    "kind",
    "symbol",
    "market",
    "price_usd",
    "size_tokens",
    "size_usd",
    "is_long",
    "price_impact_usd",
    "side",
]


@dataclass(frozen=True, slots=True)
class TokenMeta:
    """Identity and scale of a token.

    :param symbol: Candle symbol, e.g. ``BTC`` -- the stem of
        ``BTC_USDC_USDC-1m-futures.feather``.
    :param decimals: Token decimals, needed to scale both amounts and prices.
    """

    symbol: str
    decimals: int


@dataclass(frozen=True, slots=True)
class TradeTick:
    """One fill, denominated in both tokens and USD.

    :param timestamp: Block timestamp, Unix seconds.
    :param block_number: Block the event was emitted in.
    :param transaction_hash: Transaction hash.
    :param log_index: Log index within the transaction.
    :param event_name: Source event, e.g. ``PositionIncrease``.
    :param kind: ``perp`` for position events, ``swap`` for GM-pool swaps.
    :param symbol: Candle symbol this fill counts toward.
    :param market: Market (or pool) address the fill happened on.
    :param price_usd: Fill price in USD.
    :param size_tokens: Fill size in base tokens.
    :param size_usd: Fill size in USD.
    :param is_long: Position side; ``None`` for swaps.
    :param price_impact_usd: Signed price impact in USD.
    :param side: ``in``/``out`` for the two halves of a swap, ``None`` for a
        perp fill. Both halves carry the full notional, so summing swap
        ``size_usd`` protocol-wide double-counts by 2x; filter to one side
        for a protocol total. Per-symbol sums are correct as-is.
    """

    timestamp: int
    block_number: int
    transaction_hash: str
    log_index: int
    event_name: str
    kind: str
    symbol: str
    market: str
    price_usd: float
    size_tokens: float
    size_usd: float
    is_long: bool | None
    price_impact_usd: float
    side: str | None = None


def _scale_price(raw: int, decimals: int) -> float:
    """Convert a GMX fixed-point price to USD.

    :param raw: Raw 30-decimal price as emitted.
    :param decimals: Decimals of the token the price refers to.
    :returns: Price in USD.
    """
    return raw * (10**decimals) / GMX_USD_PRECISION


def _scale_usd(raw: int) -> float:
    """Convert a GMX 30-decimal USD value to USD.

    :param raw: Raw 30-decimal USD value.
    :returns: Value in USD.
    """
    return raw / GMX_USD_PRECISION


def _scale_amount(raw: int, decimals: int) -> float:
    """Convert a raw token amount to human units.

    :param raw: Raw integer amount in the token's own decimals.
    :param decimals: Token decimals.
    :returns: Amount in whole tokens.
    """
    return raw / (10**decimals)


def _lookup(address: str | None, table: dict[str, TokenMeta]) -> TokenMeta | None:
    """Case-insensitively resolve an address in a metadata table.

    Addresses arrive checksummed from some sources and lowercased from
    others, so both spellings must hit.

    :param address: Address to resolve, or ``None``.
    :param table: Mapping of address to :class:`TokenMeta`.
    :returns: The entry, or ``None`` when absent.
    """
    if not address:
        return None
    hit = table.get(address)
    if hit is not None:
        return hit
    lowered = address.lower()
    for key, value in table.items():
        if key.lower() == lowered:
            return value
    return None


def ticks_from_event_data(
    event: GMXEventData,
    *,
    timestamp: int,
    block_number: int,
    tx_hash: str,
    log_index: int,
    markets: dict[str, TokenMeta],
    tokens: dict[str, TokenMeta],
) -> list[TradeTick]:
    """Turn one decoded GMX event into the ticks it represents.

    A position event yields a single tick. A swap yields one tick per side --
    a USDC->WETH swap is genuine flow in both tokens, and each side is valued
    at its own token's price, so the two notionals differ by the swap fee and
    neither may be derived from the other.

    Events whose market or token is missing from the mappings are dropped:
    the decimals would be unknown, and assuming 18 would inflate an
    8-decimal token's volume by a factor of 10^10.

    :param event: Decoded event from ``eth_defi.gmx.events.decode_gmx_event``.
    :param timestamp: Block timestamp, Unix seconds.
    :param block_number: Block number.
    :param tx_hash: Transaction hash.
    :param log_index: Log index within the transaction.
    :param markets: Market token address -> index :class:`TokenMeta`.
    :param tokens: Token address -> :class:`TokenMeta`.
    :returns: Zero or more ticks; empty for events that are not fills.
    """
    name = event.event_name
    uints = event.uint_items or {}
    ints = event.int_items or {}
    addresses = event.address_items or {}

    if name in PERP_EVENTS:
        market_address = addresses.get("market")
        meta = _lookup(market_address, markets)
        if meta is None:
            logger.debug("Skipping %s on unmapped market %s", name, market_address)
            return []

        size_usd = _scale_usd(uints.get("sizeDeltaUsd", 0))
        size_tokens = _scale_amount(uints.get("sizeDeltaInTokens", 0), meta.decimals)
        if size_usd <= 0 and size_tokens <= 0:
            return []

        return [
            TradeTick(
                timestamp=timestamp,
                block_number=block_number,
                transaction_hash=tx_hash,
                log_index=log_index,
                event_name=name,
                kind=PERP_KIND,
                symbol=meta.symbol,
                market=market_address or "",
                price_usd=_scale_price(uints.get("executionPrice", 0), meta.decimals),
                size_tokens=size_tokens,
                size_usd=size_usd,
                is_long=(event.bool_items or {}).get("isLong"),
                price_impact_usd=_scale_usd(ints.get("priceImpactUsd", 0)),
                side=None,
            )
        ]

    if name == SWAP_EVENT:
        market_address = addresses.get("market") or ""
        impact = _scale_usd(ints.get("priceImpactUsd", 0))
        ticks: list[TradeTick] = []

        for token_key, amount_key, price_key, side in (
            ("tokenIn", "amountIn", "tokenInPrice", "in"),
            ("tokenOut", "amountOut", "tokenOutPrice", "out"),
        ):
            meta = _lookup(addresses.get(token_key), tokens)
            if meta is None:
                logger.debug("Skipping swap side %s: token not mapped", token_key)
                continue

            size_tokens = _scale_amount(uints.get(amount_key, 0), meta.decimals)
            if size_tokens <= 0:
                continue
            price_usd = _scale_price(uints.get(price_key, 0), meta.decimals)

            ticks.append(
                TradeTick(
                    timestamp=timestamp,
                    block_number=block_number,
                    transaction_hash=tx_hash,
                    log_index=log_index,
                    event_name=name,
                    kind=SWAP_KIND,
                    symbol=meta.symbol,
                    market=market_address,
                    price_usd=price_usd,
                    size_tokens=size_tokens,
                    size_usd=size_tokens * price_usd,
                    is_long=None,
                    price_impact_usd=impact,
                    side=side,
                )
            )
        return ticks

    return []


def ticks_to_frame(ticks: Sequence[TradeTick]) -> pd.DataFrame:
    """Flatten ticks into a DataFrame ready for parquet storage.

    :param ticks: Ticks from :func:`ticks_from_event_data`.
    :returns: One row per tick, ``date`` as UTC timestamps; an empty frame
        with the full column set when ``ticks`` is empty.
    """
    if not ticks:
        empty = pd.DataFrame({column: [] for column in _TICK_COLUMNS})
        empty["date"] = pd.to_datetime(empty["date"], utc=True)
        empty["block_number"] = empty["block_number"].astype("int64")
        return empty

    frame = pd.DataFrame(
        {
            "date": pd.to_datetime([t.timestamp for t in ticks], unit="s", utc=True),
            "block_number": [t.block_number for t in ticks],
            "transaction_hash": [t.transaction_hash for t in ticks],
            "log_index": [t.log_index for t in ticks],
            "event_name": [t.event_name for t in ticks],
            "kind": [t.kind for t in ticks],
            "symbol": [t.symbol for t in ticks],
            "market": [t.market for t in ticks],
            "price_usd": [t.price_usd for t in ticks],
            "size_tokens": [t.size_tokens for t in ticks],
            "size_usd": [t.size_usd for t in ticks],
            "is_long": [t.is_long for t in ticks],
            "price_impact_usd": [t.price_impact_usd for t in ticks],
            "side": [t.side for t in ticks],
        }
    )
    frame["block_number"] = frame["block_number"].astype("int64")
    return frame.sort_values(["date", "block_number", "log_index"]).reset_index(drop=True)


def aggregate_tick_volume(
    ticks: Iterable[TradeTick],
    timeframe: str,
    kinds: Sequence[str] | None = None,
) -> pd.DataFrame:
    """Sum ticks into per-symbol volume on a candle grid.

    Every fill counts once, so a position opened and later closed contributes
    twice -- the convention every CEX feed uses, and therefore the one
    Freqtrade and NautilusTrader assume.

    :param ticks: Ticks to aggregate.
    :param timeframe: Candle timeframe, one of ``1m``, ``5m``, ``15m``,
        ``1h``, ``4h``, ``1d``.
    :param kinds: Which tick kinds count, e.g. ``("perp",)``. ``None``
        counts every kind supplied.
    :returns: Columns ``symbol``, ``date``, ``volume`` (base tokens),
        ``volume_usd``, ``trades``; sorted by symbol then date.
    :raises KeyError: If ``timeframe`` is not a known candle timeframe.
    """
    alias = _TIMEFRAME_TO_PANDAS[timeframe]

    selected = [t for t in ticks if kinds is None or t.kind in kinds]
    if not selected:
        empty = pd.DataFrame({column: [] for column in _VOLUME_COLUMNS})
        empty["date"] = pd.to_datetime(empty["date"], utc=True)
        return empty

    frame = pd.DataFrame(
        {
            "symbol": [t.symbol for t in selected],
            "date": pd.to_datetime([t.timestamp for t in selected], unit="s", utc=True),
            "volume": [t.size_tokens for t in selected],
            "volume_usd": [t.size_usd for t in selected],
        }
    )
    frame["date"] = frame["date"].dt.floor(alias)

    grouped = (
        frame.groupby(["symbol", "date"], as_index=False)
        .agg(volume=("volume", "sum"), volume_usd=("volume_usd", "sum"), trades=("volume", "size"))
        .sort_values(["symbol", "date"])
        .reset_index(drop=True)
    )
    grouped["trades"] = grouped["trades"].astype("int64")
    return grouped[_VOLUME_COLUMNS]
