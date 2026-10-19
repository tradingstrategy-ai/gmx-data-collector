"""DataStore tail-fill, gap correction and backfill, with the RPC mocked (#64, #47).

The public arb1 RPC is not an archive node, so nothing here touches a network:
``requests.post`` is replaced by a fake chain that serves block headers and the
batch reader's ``eth_call``.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import polars as pl
import pytest
from eth_abi import decode as abi_decode
from eth_abi import encode as abi_encode

from gmx_historical_data import datastore_tail as dt


def _load(name: str):
    path = Path(__file__).resolve().parents[1] / "scripts" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


ds = _load("extract_funding_datastore")
uni = _load("extract_unified_funding")

HOUR = timedelta(hours=1)
NOW = datetime(2026, 10, 19, 12, 30, tzinfo=UTC)
CUTOFF = datetime(2026, 10, 19, 11, 0, tzinfo=UTC)
MOO = "0x" + "aa" * 20
WIF = "0x" + "bb" * 20
SCALE = 10**30
RPC = "http://rpc.invalid"


def _h(hour: int, day: int = 19) -> datetime:
    return datetime(2026, 10, day, hour, tzinfo=UTC)


def _epoch(value: datetime) -> int:
    return int(value.timestamp())


def raw_value(hour: datetime) -> int:
    """The DataStore value the fake chain holds during ``hour`` (negative: shorts pay)."""
    return -((_epoch(hour) // 3600) % 1000 + 1) * 10**21


def hourly_of(hour: datetime) -> float:
    return raw_value(hour) / SCALE * 3600


class _Resp:
    def __init__(self, body):
        self._body = body

    def raise_for_status(self):
        pass

    def json(self):
        return self._body


class FakeChain:
    """Arbitrum-ish JSON-RPC: 4 blocks/s, headers anywhere, state only where allowed."""

    B0 = 1_000
    T0 = _epoch(datetime(2026, 9, 1, tzinfo=UTC))

    def __init__(self, *, created=None, state_floor=0, state_ceiling=None):
        self.created = created or {}  # market -> first block with code
        self.state_floor = state_floor  # eth_call below this: not an archive node
        self.state_ceiling = state_ceiling  # eth_call at or above this fails (partial window)
        self.call_blocks: list[int] = []
        self.latest = self.B0 + 4 * (_epoch(NOW) - self.T0)

    def ts(self, block: int) -> int:
        return self.T0 + (block - self.B0) // 4

    def _state_error(self, block: int) -> str | None:
        if block < self.state_floor:
            return f"missing trie node 0xabc (path ) state for block {block}"
        if self.state_ceiling is not None and block >= self.state_ceiling:
            return f"historical state for block {block} is not available"
        return None

    def __call__(self, url, json, timeout):
        out = []
        for req in json:
            method, params = req["method"], req["params"]
            if method == "eth_blockNumber":
                out.append({"id": req["id"], "result": hex(self.latest)})
            elif method == "eth_getBlockByNumber":
                bn = int(params[0], 16)
                out.append(
                    {"id": req["id"], "result": {"number": hex(bn), "timestamp": hex(self.ts(bn))}}
                )
            else:
                bn = int(params[1], 16)
                self.call_blocks.append(bn)
                error = self._state_error(bn)
                if error:
                    out.append({"id": req["id"], "error": {"code": -32000, "message": error}})
                    continue
                data = bytes.fromhex(params[0]["data"][2:])
                markets = abi_decode(
                    ["address[]"], data[len(bytes.fromhex(ds.FUNDING_BATCH_BYTECODE[2:])) :]
                )[0]
                hour = datetime.fromtimestamp(self.ts(bn) // 3600 * 3600, tz=UTC)
                values, statuses = [], []
                for market in markets:
                    if bn < self.created.get(market.lower(), 0):
                        values.append(0)
                        statuses.append(ds.READ_NOT_DEPLOYED)
                    else:
                        values.append(raw_value(hour))
                        statuses.append(ds.READ_OK)
                raw = abi_encode(["int256[]", "uint8[]"], [values, statuses]).hex()
                out.append({"id": req["id"], "result": "0x" + raw})
        return _Resp(out)


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    monkeypatch.setattr(ds.time, "sleep", lambda *_: None)


def _install(monkeypatch, chain: FakeChain) -> FakeChain:
    monkeypatch.setattr(ds.requests, "post", chain)
    return chain


# ---------------------------------------------------------------------------
# A lake with one quiet market
# ---------------------------------------------------------------------------


def _write_lake(
    network_dir: Path,
    symbol: str = "MOO",
    market: str = MOO,
    *,
    first_event: datetime = _h(2),
    last_event: datetime = _h(6),
    silent: tuple[datetime, ...] = (_h(4),),
) -> None:
    """Raw + factor + direction state whose last event is ``last_event``."""
    events = []
    hour = first_event
    while hour <= last_event:
        if hour not in silent:
            events.append(hour)
        hour += HOUR
    n = len(events)
    pl.DataFrame(
        {
            "timestamp": events,
            "funding_rate": [2e-9] * n,
            "funding_rate_min": [2e-9] * n,
            "funding_rate_max": [2e-9] * n,
            "funding_rate_hourly": [2e-9 * 3600] * n,
            "funding_rate_annualized": [1.0] * n,
            "update_count": [3] * n,
            "symbol": [symbol] * n,
            "market": [market] * n,
        },
        schema_overrides={"timestamp": pl.Datetime("ns", "UTC"), "update_count": pl.UInt32},
    ).write_parquet(_mkdir(network_dir / "rates" / symbol) / "1h_factor.parquet")
    _mkdir(network_dir / "raw" / "funding" / symbol / "partition=0")
    pl.DataFrame({"symbol": [symbol]}).write_parquet(
        network_dir / "raw" / "funding" / symbol / "partition=0" / "data.parquet"
    )
    _mkdir(network_dir / "direction" / symbol)
    pl.DataFrame(
        {"timestamp": [first_event], "longs_pay_shorts": [True]},
        schema_overrides={"timestamp": pl.Datetime("ns", "UTC")},
    ).write_parquet(network_dir / "direction" / symbol / "1h.parquet")


def _mkdir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def _canonical(network_dir: Path, symbol: str = "MOO") -> pl.DataFrame:
    return pl.read_parquet(network_dir / "rates" / symbol / "1h.parquet").sort("timestamp")


def _by_hour(frame: pl.DataFrame) -> dict[datetime, dict]:
    return {row["timestamp"]: row for row in frame.iter_rows(named=True)}


@pytest.fixture
def lake(tmp_path: Path) -> Path:
    network_dir = tmp_path / "funding" / "arbitrum"
    _write_lake(network_dir)
    uni.merge_unified_rates(network_dir)
    return network_dir


def _tail_fill(network_dir: Path, **kw):
    kw.setdefault("rpc_config", RPC)
    kw.setdefault("now", NOW)
    return uni.tail_fill_datastore(network_dir, **kw)


# ---------------------------------------------------------------------------
# Tail extension, gap correction, precedence
# ---------------------------------------------------------------------------


def test_the_fixture_lake_has_a_stale_tail_and_a_carried_hour(lake: Path) -> None:
    rows = _by_hour(_canonical(lake))
    assert max(rows) == _h(6)
    assert rows[_h(4)]["is_gap_filled"] is True


def test_datastore_reads_extend_the_tail_to_the_cutoff(lake: Path, monkeypatch) -> None:
    _install(monkeypatch, FakeChain())

    result = _tail_fill(lake)

    rows = _by_hour(_canonical(lake))
    assert max(rows) == CUTOFF
    for hour in (_h(7), _h(8), _h(9), _h(10), _h(11)):
        row = rows[hour]
        assert row["source"] == "datastore"
        assert row["is_gap_filled"] is False
        assert row["funding_rate_hourly"] == pytest.approx(hourly_of(hour))
        assert row["funding_rate_hourly"] < 0  # signed as read: shorts pay
    assert result.status["status"] == "ok"
    assert result.touched == {"MOO"}


def test_a_carried_gap_hour_is_replaced_by_the_chain_state(lake: Path, monkeypatch) -> None:
    _install(monkeypatch, FakeChain())
    before = _by_hour(_canonical(lake))[_h(4)]
    assert before["source"] == "hypersync" and before["is_gap_filled"] is True

    _tail_fill(lake)

    after = _by_hour(_canonical(lake))[_h(4)]
    assert after["source"] == "datastore"
    assert after["is_gap_filled"] is False
    assert after["funding_rate_hourly"] == pytest.approx(hourly_of(_h(4)))
    assert after["funding_rate_hourly"] != before["funding_rate_hourly"]


def test_event_hours_are_never_read_and_keep_their_hypersync_row(lake: Path, monkeypatch) -> None:
    chain = _install(monkeypatch, FakeChain())
    before = _by_hour(_canonical(lake))

    _tail_fill(lake)

    after = _by_hour(_canonical(lake))
    for hour in (_h(2), _h(3), _h(5), _h(6)):
        assert after[hour] == before[hour]
        assert after[hour]["source"] == "hypersync"
        assert after[hour]["is_gap_filled"] is False
    read_hours = {chain.ts(b) // 3600 * 3600 for b in chain.call_blocks}
    assert read_hours == {_epoch(h) for h in (_h(4), _h(7), _h(8), _h(9), _h(10), _h(11))}


def test_reads_are_at_the_first_block_of_each_hour(lake: Path, monkeypatch) -> None:
    chain = _install(monkeypatch, FakeChain())
    _tail_fill(lake)
    for block in chain.call_blocks:
        assert chain.ts(block) % 3600 == 0  # 4 blocks/s: the first block of the hour
        assert chain.ts(block - 1) < chain.ts(block)


def test_one_call_per_block_reads_every_market_at_once(tmp_path: Path, monkeypatch) -> None:
    network_dir = tmp_path / "funding" / "arbitrum"
    _write_lake(network_dir, "MOO", MOO)
    _write_lake(network_dir, "WIF", WIF)
    uni.merge_unified_rates(network_dir)
    chain = _install(monkeypatch, FakeChain())

    result = _tail_fill(network_dir)

    # 6 hours (gap + five tail) x 2 markets, but one eth_call per hourly block.
    assert len(chain.call_blocks) == 6
    assert result.touched == {"MOO", "WIF"}
    assert max(_canonical(network_dir, "WIF")["timestamp"]) == CUTOFF


def test_the_datastore_file_is_signed_and_labelled(lake: Path, monkeypatch) -> None:
    _install(monkeypatch, FakeChain())
    _tail_fill(lake)

    stored = pl.read_parquet(lake / "rates" / "MOO" / "1h_datastore.parquet")
    assert set(stored["source"]) == {"datastore"}
    assert stored["is_gap_filled"].to_list() == [False] * len(stored)
    assert (stored["funding_rate_hourly"] < 0).all()
    assert set(stored["market"]) == {MOO}


def test_a_second_run_reads_nothing_it_already_has(lake: Path, monkeypatch) -> None:
    chain = _install(monkeypatch, FakeChain())
    _tail_fill(lake)
    first_calls = len(chain.call_blocks)
    canonical = _canonical(lake)

    result = _tail_fill(lake)

    assert len(chain.call_blocks) == first_calls
    assert result.status["status"] == "ok"
    assert result.touched == set()
    assert _canonical(lake).equals(canonical)


def test_the_window_is_capped_to_thirty_days(tmp_path: Path, monkeypatch) -> None:
    network_dir = tmp_path / "funding" / "arbitrum"
    _write_lake(
        network_dir,
        first_event=datetime(2026, 9, 1, tzinfo=UTC),
        last_event=datetime(2026, 9, 2, tzinfo=UTC),
        silent=(),
    )
    uni.merge_unified_rates(network_dir)
    chain = _install(monkeypatch, FakeChain())

    _tail_fill(network_dir)

    oldest = min(chain.ts(b) for b in chain.call_blocks)
    assert oldest == _epoch(CUTOFF - timedelta(days=dt.TAIL_WINDOW_DAYS))
    assert len(chain.call_blocks) == dt.TAIL_WINDOW_DAYS * 24 + 1


# ---------------------------------------------------------------------------
# Never invent rows
# ---------------------------------------------------------------------------


def test_delisted_markets_are_not_extended(lake: Path, monkeypatch) -> None:
    (lake.parent.parent / "delisted_markets.json").write_text(json.dumps({"symbols": ["MOO"]}))
    chain = _install(monkeypatch, FakeChain())
    before = _canonical(lake)

    result = _tail_fill(lake)

    assert chain.call_blocks == []
    assert result.touched == set()
    assert _canonical(lake).equals(before)
    assert max(before["timestamp"]) == _h(6)


def test_hours_before_the_market_existed_are_not_observations(lake: Path, monkeypatch) -> None:
    chain = FakeChain()
    chain.created[MOO] = next(
        b for b in range(chain.B0, chain.latest, 4) if chain.ts(b) >= _epoch(_h(9))
    )
    _install(monkeypatch, chain)

    result = _tail_fill(lake)

    stored = _by_hour(pl.read_parquet(lake / "rates" / "MOO" / "1h_datastore.parquet"))
    assert set(stored) == {_h(9), _h(10), _h(11)}  # NOT_DEPLOYED hours produce no row
    canonical = _by_hour(_canonical(lake))
    assert _h(7) not in canonical and _h(8) not in canonical  # a hole, not an invented hour
    assert result.status["hours_read"] == 3


# ---------------------------------------------------------------------------
# Fail-soft
# ---------------------------------------------------------------------------


def test_a_non_archive_node_is_recorded_not_raised(lake: Path, monkeypatch) -> None:
    chain = FakeChain(state_floor=10**12)
    _install(monkeypatch, chain)
    before = _canonical(lake)

    result = _tail_fill(lake)

    assert result.status["status"] == "unavailable"
    assert "missing trie node" in result.status["reason"]
    assert _canonical(lake).equals(before)  # stale tails stay exactly as they were
    assert not (lake / "rates" / "MOO" / "1h_datastore.parquet").exists()
    saved = dt.read_tail_status(lake / "checkpoints")
    assert saved["status"] == "unavailable"
    assert dt.describe_tail_status(saved, now=NOW).startswith("unavailable (")


def test_historical_state_errors_are_not_retried(lake: Path, monkeypatch) -> None:
    chain = _install(monkeypatch, FakeChain(state_floor=10**12))
    slept = []
    monkeypatch.setattr(ds.time, "sleep", lambda s: slept.append(s))

    _tail_fill(lake)

    assert slept == []
    assert len(chain.call_blocks) <= 6  # the one chunk, tried once -- not five retries of it


def test_without_an_rpc_the_tail_is_unavailable(lake: Path, monkeypatch) -> None:
    monkeypatch.delenv("JSON_RPC_ARBITRUM", raising=False)
    monkeypatch.delenv("ARBITRUM_CHAIN_JSON_RPC", raising=False)

    result = _tail_fill(lake, rpc_config=None)

    assert result.status["status"] == "unavailable"
    assert "JSON_RPC_ARBITRUM" in result.status["reason"]


def test_a_partial_window_is_persisted_and_survives_the_next_run(lake: Path, monkeypatch) -> None:
    chain = FakeChain()
    chain.state_ceiling = next(
        b for b in range(chain.B0, chain.latest, 4) if chain.ts(b) >= _epoch(_h(10))
    )
    _install(monkeypatch, chain)

    result = _tail_fill(lake, chunk_hours=2)

    assert result.status["status"] == "partial"
    assert result.status["hours_read"] == 4  # gap hour + 07, 08, 09
    stored = _by_hour(pl.read_parquet(lake / "rates" / "MOO" / "1h_datastore.parquet"))
    assert set(stored) == {_h(4), _h(7), _h(8), _h(9)}
    assert max(_canonical(lake)["timestamp"]) == _h(9)  # contiguous prefix, no hole

    chain.state_ceiling = None
    chain.call_blocks.clear()
    again = _tail_fill(lake, chunk_hours=2)

    assert again.status["status"] == "ok"
    assert len(chain.call_blocks) == 2  # only the two hours still missing
    assert max(_canonical(lake)["timestamp"]) == CUTOFF


def test_a_time_budget_stops_between_chunks_and_keeps_what_it_read(lake: Path, monkeypatch) -> None:
    _install(monkeypatch, FakeChain())

    result = _tail_fill(lake, chunk_hours=2, max_minutes=0)

    assert result.status["status"] == "partial"
    assert "time budget" in result.status["reason"]
    assert result.status["hours_read"] == 0


# ---------------------------------------------------------------------------
# Merge precedence: event hour > DataStore > carried gap fill
# ---------------------------------------------------------------------------


def _datastore_rows(network_dir: Path, hours: list[datetime], symbol="MOO", market=MOO):
    n = len(hours)
    path = _mkdir(network_dir / "rates" / symbol) / "1h_datastore.parquet"
    pl.DataFrame(
        {
            "timestamp": hours,
            "funding_rate": [-5e-9] * n,
            "funding_rate_hourly": [-5e-9 * 3600] * n,
            "longs_pay_shorts": [False] * n,
            "update_count": [1] * n,
            "symbol": [symbol] * n,
            "market": [market] * n,
        },
        schema_overrides={"timestamp": pl.Datetime("ns", "UTC"), "update_count": pl.UInt32},
    ).write_parquet(path)


def test_precedence_event_beats_datastore_beats_gap_fill(tmp_path: Path) -> None:
    network_dir = tmp_path / "funding" / "arbitrum"
    _write_lake(network_dir)  # events 02,03,05,06; hour 04 carried
    _datastore_rows(network_dir, [_h(3), _h(4), _h(8)])

    uni.merge_unified_rates(network_dir)

    rows = _by_hour(_canonical(network_dir))
    assert rows[_h(3)]["source"] == "hypersync" and rows[_h(3)]["is_gap_filled"] is False
    assert rows[_h(3)]["funding_rate_hourly"] == pytest.approx(2e-9 * 3600)  # event TWAP wins
    assert rows[_h(4)]["source"] == "datastore"  # DataStore beats the carried value
    assert rows[_h(4)]["is_gap_filled"] is False
    assert rows[_h(4)]["funding_rate_hourly"] == pytest.approx(-5e-9 * 3600)
    assert rows[_h(8)]["source"] == "datastore"  # and extends the tail
    assert max(rows) == _h(8)


def test_a_carried_hour_without_a_datastore_read_stays_carried(tmp_path: Path) -> None:
    network_dir = tmp_path / "funding" / "arbitrum"
    _write_lake(network_dir)
    uni.merge_unified_rates(network_dir)
    row = _by_hour(_canonical(network_dir))[_h(4)]
    assert row["source"] == "hypersync" and row["is_gap_filled"] is True


# ---------------------------------------------------------------------------
# extract_funding_hours: the shared reader
# ---------------------------------------------------------------------------


def _read_hours(chain, hours, **kw):
    persisted: list = []
    result = ds.extract_funding_hours(
        RPC,
        [_epoch(h) for h in hours],
        wanted=lambda hour: [MOO],
        symbol_of=lambda addr: "MOO",
        persist=persisted.extend,
        **kw,
    )
    return result, persisted


def test_reader_returns_one_signed_record_per_wanted_hour(monkeypatch) -> None:
    chain = _install(monkeypatch, FakeChain())
    hours = [_h(h) for h in range(3, 9)]

    result, records = _read_hours(chain, hours, chunk_hours=4)

    assert [r.block_timestamp for r in records] == [_epoch(h) for h in hours]
    assert [int(r.funding_factor_per_second) for r in records] == [raw_value(h) for h in hours]
    assert result.hours_read == 6 and result.stopped is None


def test_reader_stops_at_unavailable_state_and_keeps_earlier_chunks(monkeypatch) -> None:
    chain = FakeChain()
    chain.state_ceiling = next(
        b for b in range(chain.B0, chain.latest, 4) if chain.ts(b) >= _epoch(_h(7))
    )
    _install(monkeypatch, chain)

    result, records = _read_hours(chain, [_h(h) for h in range(3, 11)], chunk_hours=2)

    assert result.stopped == "unavailable"
    assert "not available" in result.reason
    assert [r.block_timestamp for r in records] == [_epoch(_h(h)) for h in range(3, 7)]


# ---------------------------------------------------------------------------
# Backfill: hourly from genesis to the funding epoch, resumable
# ---------------------------------------------------------------------------

EPOCH_HOUR = _epoch(_h(20, 5))
FROM_HOUR = _epoch(_h(0, 3))
MARKETS = {MOO: {"symbol": "MOO/USD", "indexToken": "0x1", "isListed": True}}


def _backfill(tmp_path: Path, **kw):
    kw.setdefault("chunk_hours", 12)
    return ds.run_hourly_backfill(
        RPC,
        MARKETS,
        tmp_path,
        from_hour=FROM_HOUR,
        to_hour=EPOCH_HOUR,
        resume=kw.pop("resume", True),
        **kw,
    )


def _stored_hours(tmp_path: Path) -> list[datetime]:
    frame = pl.read_parquet(tmp_path / "arbitrum" / "rates" / "MOO" / "1h_datastore.parquet")
    return frame.sort("timestamp")["timestamp"].to_list()


def _expected_hours() -> list[datetime]:
    return [datetime.fromtimestamp(t, tz=UTC) for t in range(FROM_HOUR, EPOCH_HOUR + 1, 3600)]


def test_backfill_writes_every_hour_from_genesis_to_the_epoch(tmp_path, monkeypatch) -> None:
    _install(monkeypatch, FakeChain())

    result = _backfill(tmp_path)

    assert result.stopped is None
    assert _stored_hours(tmp_path) == _expected_hours()
    checkpoint = dt.read_backfill_checkpoint(tmp_path / "arbitrum" / "checkpoints")
    assert checkpoint["complete"] is True
    assert checkpoint["last_timestamp"] == EPOCH_HOUR


def test_backfill_resumes_from_the_checkpoint_after_a_timeout(tmp_path, monkeypatch) -> None:
    chain = _install(monkeypatch, FakeChain())
    budget = iter([False, False, True])  # the step timeout hits before the third chunk

    first = _backfill(tmp_path, should_stop=lambda: next(budget, True))

    assert first.stopped == "deadline"
    cp_dir = tmp_path / "arbitrum" / "checkpoints"
    checkpoint = dt.read_backfill_checkpoint(cp_dir)
    assert checkpoint["complete"] is False
    assert checkpoint["last_timestamp"] == FROM_HOUR + (2 * 12 - 1) * 3600
    assert len(_stored_hours(tmp_path)) == 24
    first_calls = len(chain.call_blocks)

    second = _backfill(tmp_path)

    assert second.stopped is None
    assert _stored_hours(tmp_path) == _expected_hours()
    # The 24 hours already persisted were not read again.
    resumed_hours = {chain.ts(b) for b in chain.call_blocks[first_calls:]}
    assert min(resumed_hours) == checkpoint["last_timestamp"] + 3600
    assert dt.read_backfill_checkpoint(cp_dir)["complete"] is True


def test_a_kill_between_persist_and_checkpoint_loses_nothing(tmp_path, monkeypatch) -> None:
    chain = _install(monkeypatch, FakeChain())
    real = ds.save_parquet
    writes = {"n": 0}

    def dying_save(data, output_dir):
        writes["n"] += 1
        if writes["n"] == 3:
            raise KeyboardInterrupt  # killed while writing the third chunk
        return real(data, output_dir)

    monkeypatch.setattr(ds, "save_parquet", dying_save)
    with pytest.raises(KeyboardInterrupt):
        _backfill(tmp_path)

    checkpoint = dt.read_backfill_checkpoint(tmp_path / "arbitrum" / "checkpoints")
    assert checkpoint["last_timestamp"] == FROM_HOUR + (2 * 12 - 1) * 3600  # not the third chunk

    monkeypatch.setattr(ds, "save_parquet", real)
    _backfill(tmp_path)
    assert _stored_hours(tmp_path) == _expected_hours()
    del chain


def test_backfill_hours_are_in_the_canonical_series_after_the_merge(tmp_path, monkeypatch) -> None:
    _install(monkeypatch, FakeChain())
    _backfill(tmp_path)
    network_dir = tmp_path / "arbitrum"

    uni.merge_unified_rates(network_dir)

    canonical = _canonical(network_dir)
    assert set(canonical["source"]) == {"datastore"}
    assert canonical["timestamp"].to_list() == _expected_hours()


def test_backfill_before_a_market_exists_writes_nothing_for_it(tmp_path, monkeypatch) -> None:
    chain = FakeChain()
    chain.created[MOO] = next(
        b for b in range(chain.B0, chain.latest, 4) if chain.ts(b) >= _epoch(_h(0, 4))
    )
    _install(monkeypatch, chain)

    _backfill(tmp_path)

    assert _stored_hours(tmp_path)[0] == _h(0, 4)


def test_backfill_on_a_non_archive_node_stops_without_inventing_history(
    tmp_path, monkeypatch
) -> None:
    _install(monkeypatch, FakeChain(state_floor=10**12))

    result = _backfill(tmp_path)

    assert result.stopped == "unavailable"
    assert not (tmp_path / "arbitrum" / "rates" / "MOO" / "1h_datastore.parquet").exists()
