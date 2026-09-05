"""Tests that the standalone extract scripts' Parquet writes are atomic.

Follow-up to test_atomic_parquet_writes.py: a production dry run found that
the initial C1 fix only covered ``storage.py`` (762 of 1,949 raw-store
Parquet files, under ``candles/``). The remaining 1,187 files -- funding
(650), open_interest (256), pool_liquidity (256), raw (24) -- were written
by ``scripts/extract_*.py`` helpers that called ``DataFrame.write_parquet()``
directly, with none of the atomic-write protection. Per the dry run, the
HyperSync ``429`` storm that caused the original incident hit during the
*extract* phase, making these the most ``429``-exposed writers in the repo.

This reuses test_atomic_parquet_writes.py's crash-mid-write technique
against two representative ``append_parquet()`` helpers (open_interest,
pool_liquidity) now routed through ``atomic_write_parquet()``.
"""

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import polars as pl
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "scripts"


def _load_script_module(name: str, filename: str) -> ModuleType:
    """Import a ``scripts/*.py`` file as a module by path.

    Some extract scripts import sibling scripts with a bare
    ``from extract_open_interest import ...`` (relying on the interpreter
    having put the script's own directory on ``sys.path`` when run directly,
    e.g. ``python scripts/extract_pool_liquidity.py``) -- so ``scripts/`` is
    added to ``sys.path`` here to match that runtime contract.

    :param name: Module name to register in :data:`sys.modules`.
    :param filename: Script filename under ``scripts/``.
    :returns: The imported module.
    """
    if str(SCRIPTS_DIR) not in sys.path:
        sys.path.insert(0, str(SCRIPTS_DIR))

    path = SCRIPTS_DIR / filename
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _simulate_interrupted_write(monkeypatch: pytest.MonkeyPatch) -> None:
    """Monkeypatch ``pl.DataFrame.write_parquet`` to fail mid-write.

    Identical technique to ``test_atomic_parquet_writes.py``: writes garbage
    bytes to the target path and then raises, before ``atomic_write_parquet``
    ever reaches its fsync/``os.replace`` commit step. Patching the shared
    ``polars.DataFrame`` class affects every caller regardless of which
    module imported ``polars`` -- there is only one such class per process.

    :param monkeypatch: pytest's monkeypatch fixture.
    """

    def _crash_mid_write(self: pl.DataFrame, path, *args, **kwargs) -> None:
        Path(path).write_bytes(b"TRUNCATED-NOT-A-REAL-PARQUET-FILE")
        raise OSError("simulated interruption mid-write")

    monkeypatch.setattr(pl.DataFrame, "write_parquet", _crash_mid_write)


@pytest.fixture(scope="module")
def open_interest_module() -> ModuleType:
    return _load_script_module("extract_open_interest", "extract_open_interest.py")


@pytest.fixture(scope="module")
def pool_liquidity_module() -> ModuleType:
    return _load_script_module("extract_pool_liquidity", "extract_pool_liquidity.py")


def test_extract_open_interest_append_parquet_survives_interrupted_write(
    open_interest_module: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """``append_parquet()`` (OI raw + snapshot write path) is now atomic."""
    append_parquet = open_interest_module.append_parquet

    filepath = tmp_path / "raw" / "ETH_USD" / "data.parquet"
    append_parquet(
        pl.DataFrame({"blockNumber": [1, 2], "logIndex": [0, 1], "eventType": ["a", "b"]}),
        filepath,
    )
    assert filepath.exists()
    before = pl.read_parquet(filepath)

    _simulate_interrupted_write(monkeypatch)

    with pytest.raises(OSError, match="simulated interruption"):
        append_parquet(
            pl.DataFrame({"blockNumber": [3], "logIndex": [0], "eventType": ["c"]}),
            filepath,
        )

    monkeypatch.undo()

    # The previous target is untouched and still fully readable.
    assert filepath.exists()
    assert pl.read_parquet(filepath).equals(before)

    # Only a stray .tmp file was left, not a corrupt target.
    tmp_files = list(filepath.parent.glob("*.parquet.tmp"))
    assert len(tmp_files) == 1


def test_extract_pool_liquidity_append_parquet_survives_interrupted_write(
    pool_liquidity_module: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """``append_parquet()`` (pool-liquidity raw write path) is now atomic."""
    append_parquet = pool_liquidity_module.append_parquet

    filepath = tmp_path / "raw" / "ETH_USD" / "data.parquet"
    append_parquet(pl.DataFrame({"block_number": [1, 2], "log_index": [0, 1]}), filepath)
    assert filepath.exists()
    before = pl.read_parquet(filepath)

    _simulate_interrupted_write(monkeypatch)

    with pytest.raises(OSError, match="simulated interruption"):
        append_parquet(pl.DataFrame({"block_number": [3], "log_index": [0]}), filepath)

    monkeypatch.undo()

    assert filepath.exists()
    assert pl.read_parquet(filepath).equals(before)
    tmp_files = list(filepath.parent.glob("*.parquet.tmp"))
    assert len(tmp_files) == 1


def test_block_timestamp_cache_survives_interrupted_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """The pandas-engine block-timestamp cache write is also atomic now.

    Uses :func:`atomic_write_parquet_pandas` directly (rather than driving
    the full ``BlockTimestampCache`` build path, which needs a live RPC
    provider) -- this is the same helper ``block_timestamp_cache.py`` calls.
    """
    import pandas as pd

    from gmx_historical_data.atomic_parquet import atomic_write_parquet_pandas

    cache_path = tmp_path / "block_timestamps.parquet"
    atomic_write_parquet_pandas(pd.DataFrame({"block": [1, 2], "timestamp": [100, 200]}), cache_path)
    assert cache_path.exists()
    before = pd.read_parquet(cache_path)

    original_to_parquet = pd.DataFrame.to_parquet

    def _crash_mid_write(self: pd.DataFrame, path, *args, **kwargs) -> None:
        Path(path).write_bytes(b"TRUNCATED-NOT-A-REAL-PARQUET-FILE")
        raise OSError("simulated interruption mid-write")

    monkeypatch.setattr(pd.DataFrame, "to_parquet", _crash_mid_write)

    with pytest.raises(OSError, match="simulated interruption"):
        atomic_write_parquet_pandas(pd.DataFrame({"block": [3], "timestamp": [300]}), cache_path)

    monkeypatch.setattr(pd.DataFrame, "to_parquet", original_to_parquet)

    assert cache_path.exists()
    pd.testing.assert_frame_equal(pd.read_parquet(cache_path), before)
    tmp_files = list(cache_path.parent.glob("*.parquet.tmp"))
    assert len(tmp_files) == 1
