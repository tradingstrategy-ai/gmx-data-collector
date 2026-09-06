"""Shared OHLCV validation helpers.

The storage layer and Freqtrade export layer both need the same boundary
checks. Keeping them here avoids drift between source persistence, export
publication, and the standalone integrity audit script.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta

import polars as pl

PRICE_COLUMNS = ("open", "high", "low", "close")
EXPORT_COLUMNS = ("date", "open", "high", "low", "close", "volume")

# The exporter carries ``open`` forward from the previous candle's close, while
# ``high``/``low`` are derived from the oracle prints inside the candle window.
# When price gaps between two candles, that carried-forward open legitimately
# falls outside its own window's high/low range, by an amount equal to the
# inter-candle gap -- which is unbounded.  Asserting ``low <= open <= high``
# therefore fails on ordinary volatility (a 0.84% gap on NEAR 2024-03-06, 2.34%
# on wstETH) and no fixed tolerance can separate that from corruption.  The
# envelope is checked against ``close`` only, which is a genuine in-window
# print.  Scale corruption is caught by the close-to-close jump check instead.
#
# Excluding ``open`` outright would leave a decimal-shift defect confined to
# ``open`` invisible, so it keeps a loose scale bound: an open this far outside
# the candle's own range is a unit error, not a gap.  The factor matches the
# jump detector's default ratio, so the bound can only fire where a genuine
# inter-candle move would already have tripped the close-to-close check --
# it adds detection without adding a new false-positive class.
OPEN_SCALE_RATIO = 5.0


class ExportValidationError(ValueError):
    """An OHLCV/export invariant failed — classified as a data defect.

    Subclasses ``ValueError`` so every existing ``except ValueError`` call
    site keeps working unchanged; new code (the export guards) can catch
    this type specifically via
    :data:`~gmx_historical_data.atomic_parquet.DATA_DEFECT_ERRORS`.

    :param location: Human-readable location string, matching the
        ``location`` this validator was called with.
    :param reason: Short greppable slug identifying the failure kind (e.g.
        ``"non_monotonic"``, ``"parity_mismatch"``, ``"history_shrink"``).
    :param message: Full human-readable message; becomes ``str(exc)``.
    """

    def __init__(self, location: str, reason: str, message: str) -> None:
        super().__init__(message)
        self.location = location
        self.reason = reason

    def __reduce__(self):
        return (self.__class__, (self.location, self.reason, str(self)))


#: Multiplier from a timeframe token's unit suffix to minutes.  ``m`` and
#: ``min`` are both accepted because this repo carries two key conventions:
#: parquet file stems (and therefore ``storage.list_timeframes()`` and the
#: exporter's ``tf`` variable) use ``1m``/``5m``/``15m``, while the pandas
#: resampling path uses ``1min``/``5min``/``15min``.  Parsing rather than
#: table lookup also resolves funding-style tokens such as ``8h``, which
#: ``_FUNDING_TIMEFRAME_PATTERN`` admits but no fixed six-key table contains.
_TIMEFRAME_UNIT_MINUTES: dict[str, int] = {"min": 1, "m": 1, "h": 60, "d": 1440}

_TIMEFRAME_PATTERN = re.compile(r"^(\d+)(min|m|h|d)$")


@dataclass(frozen=True, slots=True)
class CadenceBreak:
    """One interior discontinuity in an otherwise fixed-interval series.

    A break is a step between two consecutive present bars that is larger
    than the timeframe's expected interval -- i.e. one or more interior bars
    are absent.  The series is still strictly monotonic, which is why
    :func:`validate_ohlcv`'s ``non_monotonic`` guard never sees it.

    :param before: Timestamp of the last bar present before the hole.
    :param after: Timestamp of the next bar present after the hole.
    :param actual: The observed step, ``after - before``.
    :param missing_bars: Number of absent bars, ``actual // expected - 1``.
    """

    before: datetime
    after: datetime
    actual: timedelta
    missing_bars: int


def parse_timeframe_interval(timeframe: str) -> timedelta:
    """Resolve a timeframe token to its fixed bar interval.

    Accepts both key conventions used in this repo -- filename format
    (``'1m'``, ``'15m'``, ``'4h'``, ``'1d'``) and pandas format (``'1min'``,
    ``'15min'``) -- plus any other ``<count><unit>`` token, so funding-style
    ``'8h'`` resolves without a table entry.

    Note that :func:`gmx_historical_data.cex_gap_fill.detector.minutes_for_timeframe`
    accepts *only* the pandas format and raises on ``'5m'``; this function is
    the one to use anywhere the exporter's ``tf`` variable is in hand.

    :param timeframe: Timeframe token, e.g. ``'4h'``.
    :returns: The bar interval as a :class:`~datetime.timedelta`.
    :raises ValueError: If ``timeframe`` is not a recognised token or its
        count is zero.
    """
    match = _TIMEFRAME_PATTERN.match(timeframe)
    if match is None:
        raise ValueError(f"unrecognised timeframe token: {timeframe!r}")
    count = int(match.group(1))
    if count <= 0:
        raise ValueError(f"timeframe count must be positive: {timeframe!r}")
    return timedelta(minutes=count * _TIMEFRAME_UNIT_MINUTES[match.group(2)])


def find_cadence_breaks(
    frame: pl.DataFrame,
    *,
    timestamp_column: str,
    expected_interval: timedelta,
) -> list[CadenceBreak]:
    """Find every interior step that is not exactly ``expected_interval``.

    This is the check :func:`validate_ohlcv` has always lacked: its
    ``non_monotonic`` guard rejects ``diff <= 0`` but accepts any positive
    step, so a series missing an interior bar is monotonic-but-irregular and
    passes silently (issue #29).

    Pure by design -- it reports and never raises, because severity is a
    policy decision for the caller.  82% of currently-shipped files carry at
    least one break, most of them permanently unrecoverable, so the export
    path records rather than rejects.

    Only steps *larger* than expected are reported.  A smaller step means a
    duplicate or off-grid timestamp, which
    :func:`validate_ohlcv`'s ``duplicate_timestamps`` guard already covers.

    :param frame: OHLCV frame; sorted or not.
    :param timestamp_column: Name of the timestamp column.
    :param expected_interval: The timeframe's fixed bar interval, from
        :func:`parse_timeframe_interval`.
    :returns: Breaks in ascending timestamp order; empty if the series is
        contiguous or has fewer than two rows.
    """
    if frame.height < 2:
        return []

    timestamps = frame.get_column(timestamp_column).sort().to_list()
    breaks: list[CadenceBreak] = []
    for before, after in zip(timestamps, timestamps[1:], strict=False):
        actual = after - before
        if actual <= expected_interval:
            continue
        breaks.append(
            CadenceBreak(
                before=before,
                after=after,
                actual=actual,
                missing_bars=int(actual // expected_interval) - 1,
            )
        )
    return breaks


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
) -> pl.DataFrame:
    """Validate OHLCV invariants and return the original frame.

    The validator checks:

    - required columns are present
    - timestamp values are non-null, unique, and strictly increasing
    - OHLC values are finite and strictly positive
    - ``low`` is not above ``close``
    - ``high`` is not below ``close``
    - ``low`` is not above ``high``

    ``open`` is deliberately excluded from the envelope check: the exporter
    carries it forward from the previous candle's close, so it may sit outside
    the current window's high/low whenever price gaps between candles.  Use
    :func:`count_open_outside_envelope` to report that artifact without
    failing.  It is still held to a loose scale bound
    (:data:`OPEN_SCALE_RATIO`) so a decimal shift confined to ``open`` cannot
    pass unnoticed.

    ``volume`` is validated when present, but it is not required because the
    source candle store does not persist it.

    :param allow_nonpositive_prices: When ``True``, price columns only need to
        be finite; zero and negative values are accepted and the OHLC ordering
        check is skipped.  Used for funding-rate frames, where the rate is
        stored in ``open`` and legitimately goes negative while the other OHLC
        columns are zero sentinels.
    """

    required_columns = [timestamp_column, *PRICE_COLUMNS]
    missing = [column for column in required_columns if column not in frame.columns]
    if missing:
        raise ExportValidationError(
            location, "missing_columns", f"{location}: missing required OHLCV columns: {missing}"
        )

    if frame.is_empty():
        raise ExportValidationError(
            location, "empty_frame", f"{location}: invalid OHLCV empty frame"
        )

    working = frame.select(
        [column for column in frame.columns if column in {*required_columns, "volume"}]
    )

    timestamp_nulls = working.filter(pl.col(timestamp_column).is_null())
    if not timestamp_nulls.is_empty():
        first_timestamp = _first_timestamp(timestamp_nulls, timestamp_column)
        raise ExportValidationError(
            location,
            "invalid_timestamps",
            f"{location}: invalid OHLCV timestamp values count={timestamp_nulls.height} "
            f"first_timestamp={first_timestamp}",
        )

    duplicated_timestamps = working.filter(pl.col(timestamp_column).is_duplicated())
    if not duplicated_timestamps.is_empty():
        first_timestamp = _first_timestamp(duplicated_timestamps, timestamp_column)
        raise ExportValidationError(
            location,
            "duplicate_timestamps",
            f"{location}: duplicate timestamps count={duplicated_timestamps.height} "
            f"first_timestamp={first_timestamp}",
        )

    non_monotonic = working.with_columns(
        pl.col(timestamp_column).diff().alias("__timestamp_delta")
    ).filter(
        pl.col("__timestamp_delta").is_not_null()
        & (pl.col("__timestamp_delta") <= pl.duration(microseconds=0))
    )
    if not non_monotonic.is_empty():
        first_timestamp = _first_timestamp(non_monotonic, timestamp_column)
        raise ExportValidationError(
            location,
            "non_monotonic",
            f"{location}: non-monotonic timestamps count={non_monotonic.height} "
            f"first_timestamp={first_timestamp}",
        )

    for column in PRICE_COLUMNS:
        invalid = working.filter(
            _invalid_price_expr(column, allow_nonpositive_prices=allow_nonpositive_prices)
        )
        if not invalid.is_empty():
            first_timestamp = _first_timestamp(invalid, timestamp_column)
            raise ExportValidationError(
                location,
                "invalid_price",
                f"{location}: invalid OHLCV {column} values count={invalid.height} "
                f"first_timestamp={first_timestamp}",
            )

    if not allow_nonpositive_prices:
        ordering_violations = working.filter(_ordering_violation_expr())
        if not ordering_violations.is_empty():
            first_timestamp = _first_timestamp(ordering_violations, timestamp_column)
            raise ExportValidationError(
                location,
                "ohlc_ordering",
                f"{location}: OHLC ordering violation count={ordering_violations.height} "
                f"first_timestamp={first_timestamp}",
            )

        open_scale_violations = working.filter(_open_scale_violation_expr())
        if not open_scale_violations.is_empty():
            first_timestamp = _first_timestamp(open_scale_violations, timestamp_column)
            raise ExportValidationError(
                location,
                "open_scale",
                f"{location}: OHLC open scale violation count={open_scale_violations.height} "
                f"first_timestamp={first_timestamp}",
            )

    if "volume" in working.columns:
        invalid_volume = working.filter(_invalid_volume_expr("volume"))
        if not invalid_volume.is_empty():
            first_timestamp = _first_timestamp(invalid_volume, timestamp_column)
            raise ExportValidationError(
                location,
                "invalid_volume",
                f"{location}: invalid OHLCV volume values count={invalid_volume.height} "
                f"first_timestamp={first_timestamp}",
            )

    return frame


def assert_export_parity(left: pl.DataFrame, right: pl.DataFrame, *, location: str) -> None:
    """Assert that two exported frames are identical after canonical sorting."""

    missing_left = [column for column in EXPORT_COLUMNS if column not in left.columns]
    missing_right = [column for column in EXPORT_COLUMNS if column not in right.columns]
    if missing_left or missing_right:
        raise ExportValidationError(
            location,
            "parity_missing_columns",
            f"{location}: Feather/Parquet export parity mismatch: "
            f"missing_left={missing_left}, missing_right={missing_right}",
        )

    canonical_left = _canonical_export_frame(left)
    canonical_right = _canonical_export_frame(right)
    if canonical_left.equals(canonical_right):
        return

    raise ExportValidationError(
        location, "parity_mismatch", f"{location}: Feather/Parquet export parity mismatch"
    )


def count_open_outside_envelope(frame: pl.DataFrame) -> int:
    """Count rows whose ``open`` sits outside the candle's own high/low range.

    This is the carried-forward-open artifact described in the module header,
    not a defect: it is reported for visibility but never fails validation.

    :param frame: OHLCV frame with ``open``/``high``/``low``/``close`` columns.
    :returns: number of rows where ``open < low`` or ``open > high``.
    """
    if any(column not in frame.columns for column in PRICE_COLUMNS):
        return 0
    open_ = pl.col("open").cast(pl.Float64, strict=False)
    high = pl.col("high").cast(pl.Float64, strict=False)
    low = pl.col("low").cast(pl.Float64, strict=False)
    return frame.filter(((open_ < low) | (open_ > high)).fill_null(False)).height


def _canonical_export_frame(frame: pl.DataFrame) -> pl.DataFrame:
    return (
        frame.select(EXPORT_COLUMNS)
        .with_columns(pl.col("date").cast(pl.Datetime("ns", "UTC"), strict=False))
        .sort("date")
    )


def _invalid_price_expr(column: str, *, allow_nonpositive_prices: bool) -> pl.Expr:
    expr = pl.col(column).cast(pl.Float64, strict=False)
    nonfinite = expr.is_null() | expr.is_nan() | ~expr.is_finite()
    if allow_nonpositive_prices:
        return nonfinite.fill_null(False)
    return (nonfinite | (expr <= 0)).fill_null(False)


def _ordering_violation_expr() -> pl.Expr:
    high = pl.col("high").cast(pl.Float64, strict=False)
    low = pl.col("low").cast(pl.Float64, strict=False)
    close = pl.col("close").cast(pl.Float64, strict=False)
    return ((low > close) | (high < close) | (low > high)).fill_null(False)


def _open_scale_violation_expr() -> pl.Expr:
    open_ = pl.col("open").cast(pl.Float64, strict=False)
    high = pl.col("high").cast(pl.Float64, strict=False)
    low = pl.col("low").cast(pl.Float64, strict=False)
    return ((open_ > high * OPEN_SCALE_RATIO) | (open_ < low / OPEN_SCALE_RATIO)).fill_null(False)


def _invalid_volume_expr(column: str) -> pl.Expr:
    expr = pl.col(column).cast(pl.Float64, strict=False)
    return (expr.is_null() | expr.is_nan() | ~expr.is_finite()).fill_null(False)


def _first_timestamp(frame: pl.DataFrame, timestamp_column: str) -> str:
    value = frame.select(timestamp_column).item(0, 0)
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)
