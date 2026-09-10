"""Block-bound semantics for HyperSync queries.

HyperSync's ``Query.to_block`` is exclusive, but every caller in this repo
thinks in inclusive ranges -- a CLI ``--to-block``, a checkpoint's
``last_block``, a chunk's ``ce`` computed as ``start + size - 1``. Passing the
inclusive end straight through dropped the last block of every range.

Each query builder is tested for the failure itself (the last block of the
range must be covered) rather than for the arithmetic, and the last two tests
guard the class: no ``Query`` in the repo may pass a raw inclusive end.
"""

import ast
import asyncio
from pathlib import Path

from gmx_historical_data.hypersync_range import exclusive_end


class TestExclusiveEnd:
    def test_inclusive_end_is_moved_past_the_last_block(self):
        assert exclusive_end(200) == 201

    def test_none_means_open_ended_not_block_zero(self):
        """HyperSync reads an unset bound as 'to the end of data'. Coercing it
        to 0 or to anything else would silently cap the query."""
        assert exclusive_end(None) is None

    def test_adjacent_ranges_neither_share_nor_drop_a_block(self):
        """Two runs claiming to cover [a, b] and [b+1, c] must together cover
        every block of [a, c] exactly once."""
        first_end = exclusive_end(199)
        second_end = exclusive_end(249)
        assert first_end is not None and second_end is not None

        first = set(range(100, first_end))
        second = set(range(200, second_end))

        assert first | second == set(range(100, 250))
        assert not first & second


class TestPositionEventQuery:
    """``GMXEventCollector.build_query`` -- a user's --end-block is inclusive."""

    def _build(self, start, end):
        from gmx_historical_data.gmx_event_collector import GMXEventCollector

        # Bypass __init__: building a query needs no client, RPC or token.
        return object.__new__(GMXEventCollector).build_query(start, end)

    def test_last_block_of_the_range_is_covered(self):
        assert self._build(100, 200).to_block == 201

    def test_open_ended_query_leaves_the_bound_unset(self):
        assert self._build(100, None).to_block is None

    def test_from_block_is_untouched(self):
        assert self._build(100, 200).from_block == 100


class TestAnswerUpdatedQuery:
    """``HyperSyncCollector.build_query`` -- same class, separate builder."""

    def _build(self, start, end):
        from gmx_historical_data.hypersync_collector import HyperSyncCollector

        return object.__new__(HyperSyncCollector).build_query(["0xabc"], start, end)

    def test_last_block_of_the_range_is_covered(self):
        assert self._build(100, 200).to_block == 201

    def test_open_ended_query_leaves_the_bound_unset(self):
        assert self._build(100, None).to_block is None


class TestOraclePriceQuery:
    """``OraclePriceCollector.build_query``, and the chunking that feeds it."""

    def _build(self, start, end, tokens=None):
        from gmx_historical_data.oracle_price_collector import OraclePriceCollector

        return object.__new__(OraclePriceCollector).build_query(start, end, tokens)

    def test_last_block_of_the_range_is_covered(self):
        assert self._build(100, 200).to_block == 201

    def test_open_ended_query_leaves_the_bound_unset(self):
        assert self._build(100, None).to_block is None

    def test_chunked_collection_covers_every_block_exactly_once(self):
        """A multi-chunk scan used to lose each chunk's own last block, and no
        other chunk covered it, so the loss was permanent. Assert the union of
        the queries the chunks produce tiles the range with no gap."""
        from gmx_historical_data.oracle_price_collector import OraclePriceCollector

        collector = object.__new__(OraclePriceCollector)
        chunks: list[tuple[int, int]] = []

        async def fake_chunk(chunk_start, chunk_end, *args, **kwargs):
            chunks.append((chunk_start, chunk_end))
            return [], {}

        collector._collect_chunk_with_retry = fake_chunk

        start, end = 0, 4_000_000
        asyncio.run(
            collector.collect_oracle_events(start_block=start, end_block=end, concurrency=4)
        )
        assert len(chunks) == 4

        # Convert each chunk back to the blocks its query actually covers.
        covered: set[int] = set()
        for chunk_start, chunk_end in chunks:
            query = OraclePriceCollector.build_query(collector, chunk_start, chunk_end)
            assert query.to_block is not None
            blocks = set(range(query.from_block, query.to_block))
            assert not covered & blocks, f"chunk {chunk_start}-{chunk_end} re-covers blocks"
            covered |= blocks

        assert covered == set(range(start, end + 1)), "a block of the range went unscanned"


def _files_constructing_a_query() -> list[Path]:
    root = Path(__file__).resolve().parents[1]
    candidates = [*root.joinpath("src").rglob("*.py"), *root.joinpath("scripts").rglob("*.py")]
    return [p for p in sorted(candidates) if "Query(" in p.read_text(encoding="utf-8")]


def _raw_to_block_kwargs(path: Path) -> list[tuple[int, str]]:
    """Find ``Query(..., to_block=<x>)`` calls whose bound skips ``exclusive_end``.

    Parsed rather than pattern-matched: a bare ``to_block = ...`` local is not
    a query bound and must not be flagged.
    """
    tree = ast.parse(path.read_text(encoding="utf-8"))
    offenders = []

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        name = getattr(node.func, "id", None) or getattr(node.func, "attr", None)
        if name != "Query":
            continue
        for keyword in node.keywords:
            if keyword.arg != "to_block":
                continue
            value = keyword.value
            routed = isinstance(value, ast.Call) and (
                getattr(value.func, "id", None) == "exclusive_end"
                or getattr(value.func, "attr", None) == "exclusive_end"
            )
            if not routed:
                offenders.append((node.lineno, ast.unparse(keyword)))

    return offenders


def test_no_query_builder_passes_a_raw_inclusive_end():
    """The bug class, guarded at the source.

    ``end_block + 1`` at each call site is how this spread across eleven
    builders; the conversion belongs to ``exclusive_end``, and this test fails
    the moment a new ``Query`` is built without it.
    """
    root = Path(__file__).resolve().parents[1]
    offenders = [
        f"{path.relative_to(root)}:{lineno}: {snippet}"
        for path in _files_constructing_a_query()
        for lineno, snippet in _raw_to_block_kwargs(path)
    ]

    assert not offenders, "Query.to_block must route through exclusive_end:\n" + "\n".join(
        offenders
    )


def test_the_guard_above_has_something_to_guard():
    """If it finds no Query construction, the guard proves nothing -- a rename
    would disable it silently."""
    assert len(_files_constructing_a_query()) >= 11
