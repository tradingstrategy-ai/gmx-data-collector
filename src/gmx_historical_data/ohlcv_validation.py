"""Shared OHLCV validation helpers.

The storage layer and Freqtrade export layer both need the same boundary
checks. Keeping them here avoids drift between source persistence, export
publication, and the standalone integrity audit script.
"""

from __future__ import annotations

from dataclasses import dataclass

import polars as pl

PRICE_COLUMNS = ("open", "high", "low", "close")
EXPORT_COLUMNS = ("date", "open", "high", "low", "close", "volume")

# GMX derives 4h/1d candles from oracle updates, which yields small, benign
# high/low overshoots relative to the open/close body.  These timeframes get a
# bounded relative ordering tolerance; intraday candles (1m-1h) stay strict.
# The agreed 0.75% ceiling admits the observed benign aggregation artifacts
# while remaining far below scale-jump corruption.
DEFAULT_ORDERING_TOLERANCE = 0.0075
_TOLERANT_ORDERING_TIMEFRAMES = frozenset({"4h", "1d"})


def ordering_tolerance_for_timeframe(timeframe: str | None) -> float:
    """Return the relative OHLC-ordering tolerance for ``timeframe``.

    :param timeframe: Candle timeframe (e.g. ``'1h'``, ``'4h'``, ``'1d'``).
    :returns: :data:`DEFAULT_ORDERING_TOLERANCE` for aggregated 4h/1d
        timeframes, ``0.0`` (strict) otherwise.
    """
    if timeframe is None:
        return 0.0
    return DEFAULT_ORDERING_TOLERANCE if timeframe in _TOLERANT_ORDERING_TIMEFRAMES else 0.0


@dataclass(frozen=True)
class OhlcvValidationResult:
    """Structured summary for a successful OHLCV validation."""

    rows: int
    timestamp_column: str
    location: str


def validate_ohlcv(
    frame: pl.DataFrame,
    *,
    timestamp_column: str,
    location: str,
    allow_nonpositive_prices: bool = False,
    ordering_tolerance: float = 0.0,
) -> pl.DataFrame:
    """Validate OHLCV invariants and return the original frame.

    The validator checks:

    - required columns are present
    - timestamp values are non-null, unique, and strictly increasing
    - OHLC values are finite and strictly positive
    - ``low`` is not above ``min(open, close)``
    - ``high`` is not below ``max(open, close)``

    ``volume`` is validated when present, but it is not required because the
    source candle store does not persist it.

    :param allow_nonpositive_prices: When ``True``, price columns only need to
        be finite; zero and negative values are accepted and the OHLC ordering
        check is skipped.  Used for funding-rate frames, where the rate is
        stored in ``open`` and legitimately goes negative while the other OHLC
        columns are zero sentinels.
    :param ordering_tolerance: Relative tolerance for the OHLC ordering check.
        GMX aggregates 4h/1d candles from oracle updates and produces small,
        benign high/low overshoots relative to the open/close body.  A row is
        only rejected when ``low > min(open, close) * (1 + ordering_tolerance)``
        or ``high < max(open, close) * (1 - ordering_tolerance)``.  Defaults to
        ``0.0`` (strict); use :func:`ordering_tolerance_for_timeframe`.
    """

    required_columns = [timestamp_column, *PRICE_COLUMNS]
    missing = [column for column in required_columns if column not in frame.columns]
    if missing:
        raise ValueError(f"{location}: missing required OHLCV columns: {missing}")

    if frame.is_empty():
        raise ValueError(f"{location}: invalid OHLCV empty frame")

    working = frame.select([column for column in frame.columns if column in {*required_columns, "volume"}])

    timestamp_nulls = working.filter(pl.col(timestamp_column).is_null())
    if not timestamp_nulls.is_empty():
        first_timestamp = _first_timestamp(timestamp_nulls, timestamp_column)
        raise ValueError(
            f"{location}: invalid OHLCV timestamp values count={timestamp_nulls.height} "
            f"first_timestamp={first_timestamp}"
        )

    duplicated_timestamps = working.filter(pl.col(timestamp_column).is_duplicated())
    if not duplicated_timestamps.is_empty():
        first_timestamp = _first_timestamp(duplicated_timestamps, timestamp_column)
        raise ValueError(
            f"{location}: duplicate timestamps count={duplicated_timestamps.height} "
            f"first_timestamp={first_timestamp}"
        )

    non_monotonic = working.with_columns(
        pl.col(timestamp_column).diff().alias("__timestamp_delta")
    ).filter(
        pl.col("__timestamp_delta").is_not_null() & (pl.col("__timestamp_delta") <= pl.duration(microseconds=0))
    )
    if not non_monotonic.is_empty():
        first_timestamp = _first_timestamp(non_monotonic, timestamp_column)
        raise ValueError(
            f"{location}: non-monotonic timestamps count={non_monotonic.height} "
            f"first_timestamp={first_timestamp}"
        )

    for column in PRICE_COLUMNS:
        invalid = working.filter(
            _invalid_price_expr(column, allow_nonpositive_prices=allow_nonpositive_prices)
        )
        if not invalid.is_empty():
            first_timestamp = _first_timestamp(invalid, timestamp_column)
            raise ValueError(
                f"{location}: invalid OHLCV {column} values count={invalid.height} "
                f"first_timestamp={first_timestamp}"
            )

    if not allow_nonpositive_prices:
        ordering_violations = working.filter(_ordering_violation_expr(tolerance=ordering_tolerance))
        if not ordering_violations.is_empty():
            first_timestamp = _first_timestamp(ordering_violations, timestamp_column)
            raise ValueError(
                f"{location}: OHLC ordering violation count={ordering_violations.height} "
                f"first_timestamp={first_timestamp}"
            )

    if "volume" in working.columns:
        invalid_volume = working.filter(_invalid_volume_expr("volume"))
        if not invalid_volume.is_empty():
            first_timestamp = _first_timestamp(invalid_volume, timestamp_column)
            raise ValueError(
                f"{location}: invalid OHLCV volume values count={invalid_volume.height} "
                f"first_timestamp={first_timestamp}"
            )

    return frame


def assert_export_parity(left: pl.DataFrame, right: pl.DataFrame, *, location: str) -> None:
    """Assert that two exported frames are identical after canonical sorting."""

    missing_left = [column for column in EXPORT_COLUMNS if column not in left.columns]
    missing_right = [column for column in EXPORT_COLUMNS if column not in right.columns]
    if missing_left or missing_right:
        raise ValueError(
            f"{location}: Feather/Parquet export parity mismatch: "
            f"missing_left={missing_left}, missing_right={missing_right}"
        )

    canonical_left = _canonical_export_frame(left)
    canonical_right = _canonical_export_frame(right)
    if canonical_left.equals(canonical_right):
        return

    raise ValueError(f"{location}: Feather/Parquet export parity mismatch")


def count_ordering_violations(frame: pl.DataFrame, *, tolerance: float) -> tuple[int, int]:
    """Count OHLC-ordering violations, split by a relative tolerance band.

    :param frame: OHLCV frame with ``open``/``high``/``low``/``close`` columns.
    :param tolerance: Relative ordering tolerance (see :func:`validate_ohlcv`).
    :returns: ``(within_tolerance, over_tolerance)`` row counts.  ``within_tolerance``
        rows breach strict ordering but stay inside the tolerance band;
        ``over_tolerance`` rows breach even the tolerant bound.
    """
    if any(column not in frame.columns for column in PRICE_COLUMNS):
        return (0, 0)
    strict = frame.filter(_ordering_violation_expr(tolerance=0.0)).height
    over = frame.filter(_ordering_violation_expr(tolerance=tolerance)).height
    return (strict - over, over)


def _canonical_export_frame(frame: pl.DataFrame) -> pl.DataFrame:
    return frame.select(EXPORT_COLUMNS).with_columns(
        pl.col("date").cast(pl.Datetime("ns", "UTC"), strict=False)
    ).sort("date")


def _invalid_price_expr(column: str, *, allow_nonpositive_prices: bool) -> pl.Expr:
    expr = pl.col(column).cast(pl.Float64, strict=False)
    nonfinite = expr.is_null() | expr.is_nan() | ~expr.is_finite()
    if allow_nonpositive_prices:
        return nonfinite.fill_null(False)
    return (nonfinite | (expr <= 0)).fill_null(False)


def _ordering_violation_expr(*, tolerance: float) -> pl.Expr:
    open_ = pl.col("open").cast(pl.Float64, strict=False)
    high = pl.col("high").cast(pl.Float64, strict=False)
    low = pl.col("low").cast(pl.Float64, strict=False)
    close = pl.col("close").cast(pl.Float64, strict=False)
    body_low = pl.min_horizontal(open_, close)
    body_high = pl.max_horizontal(open_, close)
    return (
        (low > body_low * (1.0 + tolerance)) | (high < body_high * (1.0 - tolerance))
    ).fill_null(False)


def _invalid_volume_expr(column: str) -> pl.Expr:
    expr = pl.col(column).cast(pl.Float64, strict=False)
    return (expr.is_null() | expr.is_nan() | ~expr.is_finite()).fill_null(False)


def _first_timestamp(frame: pl.DataFrame, timestamp_column: str) -> str:
    value = frame.select(timestamp_column).item(0, 0)
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)
