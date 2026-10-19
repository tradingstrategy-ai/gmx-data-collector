"""Window computation, block resolution and status records for the DataStore tail-fill (#64)."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import polars as pl
import pytest

from gmx_historical_data import datastore_tail as dt

HOUR = timedelta(hours=1)
NOW = datetime(2026, 10, 19, 12, 30, tzinfo=UTC)
CUTOFF = datetime(2026, 10, 19, 11, 0, tzinfo=UTC)  # floor(now) - 1h


def _frame(
    last: datetime,
    *,
    count: int = 10,
    gap_filled: tuple[int, ...] = (),
    market: str | None = "0xmarket",
) -> pl.DataFrame:
    """``count`` hourly rows ending at ``last``; ``gap_filled`` are offsets from the start."""
    start = last - (count - 1) * HOUR
    stamps = [start + i * HOUR for i in range(count)]
    return pl.DataFrame(
        {
            "timestamp": stamps,
            "market": [market] * count,
            "is_gap_filled": [i in gap_filled for i in range(count)],
        },
        schema_overrides={"timestamp": pl.Datetime("ns", "UTC")},
    )


# ---------------------------------------------------------------------------
# Window computation
# ---------------------------------------------------------------------------


def test_cutoff_is_the_floored_hour_minus_one() -> None:
    assert dt.tail_cutoff(NOW) == CUTOFF


def test_tail_runs_from_last_bar_plus_one_hour_to_the_cutoff() -> None:
    last = CUTOFF - 5 * HOUR
    windows = dt.compute_tail_windows({"MOODENG": _frame(last)}, cutoff=CUTOFF, delisted=set())

    window = windows["MOODENG"]
    assert window.tail_hours == tuple(last + i * HOUR for i in range(1, 6))
    assert window.tail_hours[-1] == CUTOFF
    assert window.gap_hours == ()
    assert window.market == "0xmarket"


def test_a_current_market_has_no_tail_window() -> None:
    windows = dt.compute_tail_windows({"BTC": _frame(CUTOFF)}, cutoff=CUTOFF, delisted=set())
    assert windows == {}


def test_tail_is_capped_to_the_trailing_thirty_days() -> None:
    last = CUTOFF - timedelta(days=90)
    windows = dt.compute_tail_windows({"OLD": _frame(last)}, cutoff=CUTOFF, delisted=set())

    hours = windows["OLD"].tail_hours
    assert hours[0] == CUTOFF - timedelta(days=30)
    assert hours[-1] == CUTOFF
    assert len(hours) == 30 * 24 + 1


def test_every_gap_filled_hour_in_the_trailing_window_is_corrected() -> None:
    frame = _frame(CUTOFF, count=40 * 24, gap_filled=(10, 40 * 24 - 50, 40 * 24 - 49))
    windows = dt.compute_tail_windows({"JTO": frame}, cutoff=CUTOFF, delisted=set())

    window = windows["JTO"]
    # offset 10 is ~39 days old: outside the window, left alone
    assert window.tail_hours == ()
    assert window.gap_hours == (CUTOFF - 49 * HOUR, CUTOFF - 48 * HOUR)
    assert window.hours == window.gap_hours


def test_tail_and_gap_hours_are_one_sorted_union() -> None:
    last = CUTOFF - 3 * HOUR
    frame = _frame(last, count=20, gap_filled=(15,))
    window = dt.compute_tail_windows({"X": frame}, cutoff=CUTOFF, delisted=set())["X"]

    assert window.gap_hours == (last - 4 * HOUR,)
    assert list(window.hours) == sorted(window.hours)
    assert len(window.hours) == 4


def test_hours_already_read_from_the_datastore_are_not_read_again() -> None:
    last = CUTOFF - 4 * HOUR
    done = {last + HOUR, last + 2 * HOUR}
    windows = dt.compute_tail_windows(
        {"X": _frame(last)}, cutoff=CUTOFF, delisted=set(), already_read={"X": done}
    )
    assert windows["X"].tail_hours == (last + 3 * HOUR, last + 4 * HOUR)


def test_delisted_symbols_are_frozen() -> None:
    frame = _frame(CUTOFF - 10 * HOUR, gap_filled=(2,))
    windows = dt.compute_tail_windows({"Luna": frame}, cutoff=CUTOFF, delisted={"LUNA"})
    assert windows == {}


def test_a_market_without_an_address_is_skipped_not_guessed() -> None:
    frame = _frame(CUTOFF - 10 * HOUR, market=None)
    assert dt.compute_tail_windows({"X": frame}, cutoff=CUTOFF, delisted=set()) == {}


def test_frames_without_gap_flags_still_get_a_tail() -> None:
    frame = _frame(CUTOFF - 2 * HOUR).drop("is_gap_filled")
    window = dt.compute_tail_windows({"X": frame}, cutoff=CUTOFF, delisted=set())["X"]
    assert len(window.tail_hours) == 2


# ---------------------------------------------------------------------------
# Hour -> first block >= boundary
# ---------------------------------------------------------------------------


class FakeChain:
    """Blocks with an irregular, monotone clock (4 blocks/s, then 2 blocks/s)."""

    B0 = 1_000
    T0 = int(datetime(2026, 9, 1, tzinfo=UTC).timestamp())
    SPLIT = 5_000_000

    def __init__(self, latest: int = 8_000_000) -> None:
        self.latest = latest
        self.calls = 0
        self.fetched = 0

    def ts(self, block: int) -> int:
        if block <= self.SPLIT:
            return self.T0 + (block - self.B0) // 4
        return self.T0 + (self.SPLIT - self.B0) // 4 + (block - self.SPLIT) // 2

    def fetch(self, blocks: list[int]) -> dict[int, int]:
        self.calls += 1
        self.fetched += len(blocks)
        assert all(0 <= b <= self.latest for b in blocks), blocks
        return {b: self.ts(b) for b in blocks}

    def first_block_at_or_after(self, ts: int) -> int:
        lo, hi = 0, self.latest
        while lo < hi:
            mid = (lo + hi) // 2
            if self.ts(mid) >= ts:
                hi = mid
            else:
                lo = mid + 1
        return lo


def _epoch(value: datetime) -> int:
    return int(value.timestamp())


def test_resolves_the_first_block_at_or_after_each_hour_boundary() -> None:
    chain = FakeChain()
    start = datetime(2026, 9, 5, tzinfo=UTC)
    hours = [_epoch(start + i * HOUR) for i in range(0, 600, 7)]
    hours += [_epoch(datetime(2026, 9, 25, 3, tzinfo=UTC))]  # across the rate change

    got = dt.resolve_hour_blocks(hours, latest_block=chain.latest, fetch_timestamps=chain.fetch)

    assert set(got) == set(hours)
    for hour, block in got.items():
        assert block == chain.first_block_at_or_after(hour)
        assert chain.ts(block) >= hour > chain.ts(block - 1)


def test_resolution_is_batched_not_one_call_per_probe() -> None:
    chain = FakeChain()
    hours = [_epoch(datetime(2026, 9, 5, tzinfo=UTC) + i * HOUR) for i in range(48)]
    dt.resolve_hour_blocks(hours, latest_block=chain.latest, fetch_timestamps=chain.fetch)
    assert chain.calls < 40  # a handful of rounds, each one batched across all hours


def test_a_shared_cache_makes_the_next_chunk_cheaper() -> None:
    chain = FakeChain()
    cache: dict[int, int] = {}
    first = [_epoch(datetime(2026, 9, 5, tzinfo=UTC) + i * HOUR) for i in range(24)]
    second = [h + 24 * 3600 for h in first]
    dt.resolve_hour_blocks(
        first, latest_block=chain.latest, fetch_timestamps=chain.fetch, cache=cache
    )
    cold = chain.fetched
    dt.resolve_hour_blocks(
        second, latest_block=chain.latest, fetch_timestamps=chain.fetch, cache=cache
    )
    assert chain.fetched - cold <= cold


def test_an_hour_after_the_chain_head_is_an_error_not_a_guess() -> None:
    chain = FakeChain(latest=100_000)
    beyond = chain.ts(chain.latest) + 3600
    with pytest.raises(dt.BlockResolutionError):
        dt.resolve_hour_blocks([beyond], latest_block=chain.latest, fetch_timestamps=chain.fetch)


# ---------------------------------------------------------------------------
# Historical-state errors
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "message",
    [
        "historical state 0x1b3... is not available",
        "Historical state 12345 not available",
        "missing trie node abcdef (path )",
        '{"code":-32000,"message":"missing trie node 0x00 (path )"}',
    ],
)
def test_non_archive_errors_are_recognised(message: str) -> None:
    assert dt.is_historical_state_error(message)


@pytest.mark.parametrize("message", ["execution reverted", "rate limited", "timeout", ""])
def test_other_errors_are_not_historical_state_errors(message: str) -> None:
    assert not dt.is_historical_state_error(message)


# ---------------------------------------------------------------------------
# Status + checkpoint records
# ---------------------------------------------------------------------------


def test_status_roundtrip_and_wording(tmp_path: Path) -> None:
    dt.write_tail_status(
        tmp_path, status="unavailable", reason="missing trie node", hours_requested=5, now=NOW
    )
    status = dt.read_tail_status(tmp_path)
    assert status["status"] == "unavailable"
    assert dt.describe_tail_status(status, now=NOW) == "unavailable (missing trie node)"


def test_status_wording_for_each_outcome() -> None:
    def describe(**kw):
        status = {"updated_at": NOW.isoformat(), **kw}
        return dt.describe_tail_status(status, now=NOW)

    assert describe(status="ok", hours_requested=12, hours_read=12) == "ok (12 hour(s) read)"
    assert (
        describe(status="partial", hours_requested=12, hours_read=5, reason="time budget")
        == "partial (5/12 hour(s) read: time budget)"
    )
    assert describe(status="failed", reason="boom") == "failed (boom)"
    assert dt.describe_tail_status(None, now=NOW) == "not run"


def test_an_old_status_is_not_presented_as_this_runs() -> None:
    old = {"status": "ok", "hours_read": 1, "updated_at": (NOW - timedelta(days=3)).isoformat()}
    assert dt.describe_tail_status(old, now=NOW).startswith("not run")


def test_unreadable_status_is_not_run(tmp_path: Path) -> None:
    (tmp_path / dt.TAIL_STATUS_FILE).write_text("{not json")
    assert dt.read_tail_status(tmp_path) is None


def test_backfill_checkpoint_is_versioned_and_shaped_like_the_others(tmp_path: Path) -> None:
    dt.write_backfill_checkpoint(
        tmp_path,
        first_hour=100 * 3600,
        last_hour=200 * 3600,
        last_block=777,
        target_hour=900 * 3600,
    )
    data = json.loads((tmp_path / dt.BACKFILL_CHECKPOINT).read_text())
    assert data["version"] == 2
    assert data["last_block"] == 777
    assert data["last_timestamp"] == 200 * 3600
    assert data["complete"] is False
    assert dt.read_backfill_checkpoint(tmp_path)["last_timestamp"] == 200 * 3600

    dt.write_backfill_checkpoint(
        tmp_path,
        first_hour=100 * 3600,
        last_hour=900 * 3600,
        last_block=999,
        target_hour=900 * 3600,
    )
    assert dt.read_backfill_checkpoint(tmp_path)["complete"] is True


def test_unversioned_backfill_checkpoint_is_refused(tmp_path: Path) -> None:
    (tmp_path / dt.BACKFILL_CHECKPOINT).write_text(json.dumps({"last_block": 5}))
    assert dt.read_backfill_checkpoint(tmp_path) is None


def test_ranges_merge_overlapping_and_touching_hours() -> None:
    h = 3600
    assert dt.merge_ranges([[10 * h, 20 * h], [0, 5 * h], [6 * h, 9 * h], [15 * h, 30 * h]]) == [
        [0, 30 * h]
    ]
    assert dt.merge_ranges([[0, 2 * h], [4 * h, 5 * h]]) == [[0, 2 * h], [4 * h, 5 * h]]


def test_uncovered_hours_are_the_prefix_gaps_and_suffix() -> None:
    h = 3600
    covered = [[2 * h, 4 * h], [7 * h, 8 * h]]
    assert dt.uncovered_hours(covered, 0, 10 * h) == [t * h for t in (0, 1, 5, 6, 9, 10)]
    assert dt.uncovered_hours(covered, 2 * h, 4 * h) == []
    assert dt.covered_hour_count(covered) == 5


def test_checkpoint_covers_ranges_and_completeness_follows_the_requested_start(
    tmp_path: Path,
) -> None:
    h = 3600
    dt.write_backfill_checkpoint(
        tmp_path,
        first_hour=0,
        last_hour=100 * h,
        last_block=9,
        target_hour=100 * h,
        covered=[[90 * h, 100 * h]],
    )
    data = dt.read_backfill_checkpoint(tmp_path)
    assert data["covered"] == [[90 * h, 100 * h]]
    assert data["complete"] is False  # hours 0..89 of the requested range are not covered
    assert data["covered_hours"] == 11


def test_a_checkpoint_without_ranges_is_invalid_and_coverage_restarts(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """The first-commit format (first_hour/last_timestamp, no ranges) never shipped: not migrated."""
    (tmp_path / dt.BACKFILL_CHECKPOINT).write_text(
        json.dumps(
            {
                "version": 2,
                "last_block": 5,
                "last_timestamp": 7200,
                "first_hour": 0,
                "complete": True,
            }
        )
    )
    with caplog.at_level("WARNING"):
        assert dt.read_backfill_checkpoint(tmp_path) is None
    assert "no valid covered ranges" in caplog.text


@pytest.mark.parametrize("covered", [[], [[1]], [[1, "x"]], "nope", [[1, 2, 3]]])
def test_malformed_ranges_are_invalid(tmp_path: Path, covered) -> None:
    (tmp_path / dt.BACKFILL_CHECKPOINT).write_text(
        json.dumps({"version": 2, "last_block": 5, "last_timestamp": 7200, "covered": covered})
    )
    assert dt.read_backfill_checkpoint(tmp_path) is None
