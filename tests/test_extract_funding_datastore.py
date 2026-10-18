"""Integrity tests for ``scripts/extract_funding_datastore.py``.

The RPC layer (``requests.post``) is mocked; no network is used.

- A successful zero reading is kept; failed/empty/undecodable responses are
  retried (and fail the run) instead of becoming zero.
- Timestamps are validated, never defaulted to epoch zero.
- Checkpoints only advance after the data they cover has been persisted.
- ``funding_fee_long``/``funding_fee_short`` are derived from the signed rate.
"""

from __future__ import annotations

import importlib.util
import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest
from eth_abi import encode as abi_encode

ETH = "0x" + "11" * 20
BTC = "0x" + "22" * 20
MARKETS = {
    ETH: {"symbol": "ETH/USD", "indexToken": "0xaaa", "isListed": True},
    BTC: {"symbol": "BTC/USD", "indexToken": "0xbbb", "isListed": True},
}
SCALE = 10**30
T0 = 1_700_000_000 - 1_700_000_000 % 3600


def _load_module():
    repo = Path(__file__).resolve().parents[1]
    path = repo / "scripts" / "extract_funding_datastore.py"
    spec = importlib.util.spec_from_file_location("extract_funding_datastore", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules["extract_funding_datastore"] = module
    spec.loader.exec_module(module)
    return module


mod = _load_module()


class _Resp:
    def __init__(self, body):
        self._body = body

    def raise_for_status(self):
        pass

    def json(self):
        return self._body


class FakeRpc:
    """Mock JSON-RPC endpoint; one block per hour starting at ``T0``.

    ``values`` maps block -> [eth, btc] raw int256.  ``fail_once`` blocks return
    a JSON-RPC error on their first funding call; ``fail_always`` never succeed.
    """

    def __init__(
        self, values, fail_once=(), fail_always=(), bad_ts=(), created=None, read_failed=()
    ):
        self.values = values
        self.created = created or {}  # market -> first block where it has code
        self.read_failed = set(read_failed)  # (block, market) whose getInt reverts
        self.fail_once = set(fail_once)
        self.fail_always = set(fail_always)
        self.bad_ts = set(bad_ts)

    def ts(self, bn):
        return T0 + (bn - 1000) * 3600

    def __call__(self, url, json, timeout):
        out = []
        for req in json:
            bn = (
                int(req["params"][1], 16)
                if req["method"] == "eth_call"
                else int(req["params"][0], 16)
            )
            if req["method"] == "eth_getBlockByNumber":
                ts = 0 if bn in self.bad_ts else self.ts(bn)
                out.append({"id": req["id"], "result": {"number": hex(bn), "timestamp": hex(ts)}})
            elif bn in self.fail_always or bn in self.fail_once:
                self.fail_once.discard(bn)
                out.append({"id": req["id"], "error": {"code": -32000, "message": "boom"}})
            else:
                statuses = []
                for market, _ in zip((ETH, BTC), self.values[bn]):
                    if bn < self.created.get(market, 0):
                        statuses.append(mod.READ_NOT_DEPLOYED)
                    elif (bn, market) in self.read_failed:
                        statuses.append(mod.READ_FAILED)
                    else:
                        statuses.append(mod.READ_OK)
                raw = abi_encode(["int256[]", "uint8[]"], [self.values[bn], statuses]).hex()
                out.append({"id": req["id"], "result": "0x" + raw})
        return _Resp(out)


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    monkeypatch.setattr(mod.time, "sleep", lambda *_: None)


def _extract(rpc, monkeypatch, blocks, **kw):
    monkeypatch.setattr(mod.requests, "post", rpc)
    return mod.extract_funding_rates(
        w3=None,
        rpc_config="http://rpc.invalid",
        from_block=blocks[0],
        to_block=blocks[-1],
        markets=MARKETS,
        interval_blocks=1,
        **kw,
    )


def test_mixed_zero_and_error_batch(monkeypatch):
    """Valid zeros are kept; an errored block is retried, not turned into zero."""
    values = {
        1000: [0, 5 * SCALE // 10**6],  # ETH is a genuine zero
        1001: [-3 * SCALE // 10**6, 0],  # error on first try, then valid
        1002: [2 * SCALE // 10**6, 4 * SCALE // 10**6],
    }
    rpc = FakeRpc(values, fail_once=[1001])
    records = _extract(rpc, monkeypatch, [1000, 1001, 1002])

    got = {(r.block_number, r.market): int(r.funding_factor_per_second) for r in records}
    assert len(records) == 6  # 3 blocks x 2 listed markets, zeros included
    assert got[(1000, ETH)] == 0
    assert got[(1001, ETH)] == -3 * SCALE // 10**6  # real value, not the error default
    assert got[(1001, BTC)] == 0


def test_persistent_rpc_failure_fails_batch(monkeypatch):
    values = {1000: [1, 2], 1001: [3, 4]}
    with pytest.raises(mod.FundingExtractionError):
        _extract(FakeRpc(values, fail_always=[1001]), monkeypatch, [1000, 1001])


def test_empty_and_undecodable_results_are_failures(monkeypatch):
    for bad in ("0x", "0xdeadbeef", None):

        def post(url, json, timeout, bad=bad):
            return _Resp([{"id": r["id"], "result": bad} for r in json])

        monkeypatch.setattr(mod.requests, "post", post)
        with pytest.raises(mod.FundingExtractionError):
            mod.batch_fetch_funding_rates("http://rpc.invalid", [ETH, BTC], [1000])


def test_invalid_timestamp_is_not_epoch_zero(monkeypatch):
    values = {1000: [1, 2], 1001: [3, 4]}
    with pytest.raises(mod.FundingExtractionError):
        _extract(FakeRpc(values, bad_ts=[1001]), monkeypatch, [1000, 1001])


def test_interruption_before_persistence_then_resume(monkeypatch, tmp_path):
    blocks = list(range(1000, 1006))
    values = {bn: [(bn - 999) * SCALE // 10**6, 0] for bn in blocks}
    rpc = FakeRpc(values)
    persisted: list = []
    calls = {"n": 0}

    def flaky_persist(batch):
        calls["n"] += 1
        if calls["n"] == 2:
            raise KeyboardInterrupt  # interrupted before the 2nd chunk is written
        persisted.extend(batch)

    with pytest.raises(KeyboardInterrupt):
        _extract(
            rpc,
            monkeypatch,
            blocks,
            checkpoint_dir=tmp_path,
            checkpoint_interval=2,
            persist=flaky_persist,
        )

    # Checkpoint covers only what was persisted
    last = mod.load_checkpoint(tmp_path)
    assert last is not None
    assert last <= max(r.block_number for r in persisted)

    resume_from = mod.resume_block(tmp_path, 1, blocks[0])
    monkeypatch.setattr(mod.requests, "post", rpc)
    mod.extract_funding_rates(
        w3=None,
        rpc_config="http://rpc.invalid",
        from_block=resume_from,
        to_block=blocks[-1],
        markets=MARKETS,
        interval_blocks=1,
        checkpoint_dir=tmp_path,
        checkpoint_interval=2,
        persist=persisted.extend,
    )

    keys = {(r.block_number, r.market) for r in persisted}
    assert keys == {(bn, m) for bn in blocks for m in (ETH, BTC)}


def test_pre_creation_blocks_are_not_observations(monkeypatch):
    """A market with no code at a block yields no record there; later zeros stay."""
    values = {bn: [SCALE // 10**6, 0] for bn in (1000, 1001, 1002)}
    rpc = FakeRpc(values, created={BTC: 1001})
    records = _extract(rpc, monkeypatch, [1000, 1001, 1002])

    btc = sorted(r.block_number for r in records if r.market == BTC)
    eth = sorted(r.block_number for r in records if r.market == ETH)
    assert btc == [1001, 1002]  # nothing invented for 1000; genuine zeros after creation kept
    assert eth == [1000, 1001, 1002]  # sampling grid unchanged for existing markets
    assert all(int(r.funding_factor_per_second) == 0 for r in records if r.market == BTC)


def test_reverted_datastore_read_is_a_failure_not_zero(monkeypatch):
    """A reverted getInt (reader status READ_FAILED) fails the batch instead of reading 0."""
    values = {1000: [1, 2], 1001: [3, 4]}
    rpc = FakeRpc(values, read_failed=[(1001, ETH)])
    with pytest.raises(mod.FundingExtractionError):
        _extract(rpc, monkeypatch, [1000, 1001])


def test_reader_source_has_no_catch_to_zero():
    src = (
        Path(__file__).resolve().parents[1] / "contracts" / "GMXFundingRateBatch.sol"
    ).read_text()
    assert "results[i] = 0" not in src
    assert "statuses[i] = 2" in src


def test_legacy_checkpoint_refuses_resume(tmp_path):
    """An unversioned checkpoint may point past unwritten data: refuse, don't skip."""
    (tmp_path / "funding_datastore_checkpoint.json").write_text(
        '{"last_block": 1001, "total_records": 4}'
    )
    with pytest.raises(mod.LegacyCheckpointError, match="legacy"):
        mod.resume_block(tmp_path, 1, 1000)


def test_saved_checkpoint_is_versioned_and_resumable(tmp_path):
    mod.save_checkpoint(tmp_path, 1001, 4)
    assert mod.resume_block(tmp_path, 1, 1000) == 1002


@pytest.mark.parametrize(
    ("hourly", "long_fee", "short_fee", "longs_pay"),
    [(-0.036, -0.036, 0.036, False), (0.036, 0.036, -0.036, True), (0.0, 0.0, 0.0, False)],
)
def test_fee_signs_follow_signed_rate(hourly, long_fee, short_fee, longs_pay):
    value = round(hourly / 3600 * SCALE)
    ts = int(datetime(2024, 1, 1, tzinfo=UTC).timestamp())
    rec = mod.FundingDatastoreRecord(
        symbol="ETH",
        market=ETH,
        funding_factor_per_second=str(value),
        funding_rate_per_second=value / SCALE,
        longs_pay_shorts=value > 0,
        block_number=1,
        block_timestamp=ts,
        block_datetime="x",
    )
    row = mod.aggregate_hourly_rates([rec])["ETH"].row(0, named=True)
    assert row["funding_fee_long"] == pytest.approx(long_fee)
    assert row["funding_fee_short"] == pytest.approx(short_fee)
    assert row["longs_pay_shorts"] is longs_pay
    assert row["funding_rate_hourly"] == pytest.approx(hourly)
