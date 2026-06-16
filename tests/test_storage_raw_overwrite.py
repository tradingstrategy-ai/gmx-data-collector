"""Regression tests for raw-event overwrite semantics.

Guards the bug where ``--force`` re-collection rewrote only ``partition=0`` while
``read_raw_events`` reads every ``partition=*``: stale appended partitions from
prior incremental runs survived and got resampled into candles.
"""

from gmx_historical_data.event_decoder import AnswerUpdatedEvent
from gmx_historical_data.storage import ParquetStorage


def _event(round_id: int) -> AnswerUpdatedEvent:
    """Build a minimal AnswerUpdatedEvent identified by its round id.

    :param round_id: Unique round id used to tell events apart in assertions.
    :return: A populated AnswerUpdatedEvent.
    """
    return AnswerUpdatedEvent(
        block_number=round_id,
        block_timestamp=1_700_000_000 + round_id,
        transaction_hash=f"0x{round_id:064x}",
        log_index=0,
        aggregator_address="0x0000000000000000000000000000000000000001",
        price=round_id * 100,
        round_id=round_id,
        timestamp=1_700_000_000 + round_id,
    )


def test_overwrite_drops_stale_partitions(tmp_path):
    """save_raw_events(overwrite=True) must remove pre-existing partitions."""
    storage = ParquetStorage(tmp_path)
    sym = "ETH"

    # Prior runs: partition=0 then an appended partition=1.
    storage.save_raw_events([_event(1), _event(2)], sym, partition_id=0)
    storage.append_raw_events([_event(3), _event(4)], sym)  # -> partition=1

    before = storage.read_raw_events(sym)
    assert sorted(before["round_id"].astype(int)) == [1, 2, 3, 4]

    # Forced re-collection writes a fresh partition=0 with overwrite=True.
    storage.save_raw_events([_event(100)], sym, partition_id=0, overwrite=True)

    # The stale partition=1 must be gone, and only the fresh event remains.
    partitions = sorted(p.name for p in (storage.raw_dir / sym).glob("partition=*"))
    assert partitions == ["partition=0"]
    after = storage.read_raw_events(sym)
    assert sorted(after["round_id"].astype(int)) == [100]


def test_no_overwrite_keeps_other_partitions(tmp_path):
    """Without overwrite, appended partitions are preserved (append default)."""
    storage = ParquetStorage(tmp_path)
    sym = "ETH"

    storage.save_raw_events([_event(1)], sym, partition_id=0)
    storage.append_raw_events([_event(2)], sym)  # -> partition=1

    # Overwrite just partition=0; partition=1 must survive.
    storage.save_raw_events([_event(9)], sym, partition_id=0, overwrite=False)

    partitions = sorted(p.name for p in (storage.raw_dir / sym).glob("partition=*"))
    assert partitions == ["partition=0", "partition=1"]
    after = storage.read_raw_events(sym)
    assert sorted(after["round_id"].astype(int)) == [2, 9]
