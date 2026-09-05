"""Tests for the shared HyperSync client-pool factory (C3).

No real network calls: :class:`hypersync.HypersyncClient` construction is
local (it just stores config), and :class:`HyperSyncKeyRotator` is pure
Python -- neither talks to the network until a query is actually issued.
"""

import pytest
from hypersync import HypersyncClient

from gmx_historical_data.hypersync_client_factory import (
    RotatingHypersyncClient,
    build_hypersync_client_or_rotator,
)
from gmx_historical_data.hypersync_key_rotator import HyperSyncKeyRotator

ENDPOINT = "https://arbitrum.hypersync.xyz"


def test_extract_helpers_rotate_key_pool():
    """The exact contract the seven extract scripts now share: a 3-key
    pooled value yields a rotator with 3 keys; a single-key value yields a
    single-key client; an empty/unset value yields None without raising."""
    pooled = build_hypersync_client_or_rotator("key1,key2,key3", ENDPOINT)
    assert isinstance(pooled, HyperSyncKeyRotator)
    assert pooled.total_keys == 3

    single = build_hypersync_client_or_rotator("only_key", ENDPOINT)
    assert isinstance(single, HypersyncClient)
    assert not isinstance(single, HyperSyncKeyRotator)

    assert build_hypersync_client_or_rotator(None, ENDPOINT) is None
    assert build_hypersync_client_or_rotator("", ENDPOINT) is None


def test_pooled_token_is_space_separated_too():
    """Matches HyperSyncKeyRotator's own comma-or-space contract."""
    pooled = build_hypersync_client_or_rotator("key1 key2 key3", ENDPOINT)
    assert isinstance(pooled, HyperSyncKeyRotator)
    assert pooled.total_keys == 3


@pytest.mark.parametrize("raw_token", [None, "", "   ", ",", " , "])
def test_empty_or_whitespace_only_token_yields_none_without_raising(raw_token):
    assert build_hypersync_client_or_rotator(raw_token, ENDPOINT) is None


def test_rotating_client_pool_has_no_rotator_for_single_key():
    pool = RotatingHypersyncClient("only_key", ENDPOINT)
    assert pool.total_keys == 1
    assert pool.key_rotator is None


def test_rotating_client_pool_has_no_rotator_when_unset():
    pool = RotatingHypersyncClient(None, ENDPOINT)
    assert pool.total_keys == 1
    assert pool.key_rotator is None
    assert isinstance(pool.client, HypersyncClient)


def test_rotating_client_pool_builds_one_client_per_key():
    pool = RotatingHypersyncClient("key1,key2,key3", ENDPOINT)
    assert pool.total_keys == 3
    assert pool.key_rotator is not None
    assert pool.key_rotator.total_keys == 3
    assert isinstance(pool.client, HypersyncClient)


def test_rotate_on_error_rotates_on_rate_limit_shaped_message():
    pool = RotatingHypersyncClient("key1,key2,key3", ENDPOINT)
    assert pool.rotate_on_error(Exception("429 Too Many Requests")) is True
    assert pool.key_rotator.current_index == 1


def test_rotate_on_error_ignores_unrelated_errors():
    pool = RotatingHypersyncClient("key1,key2,key3", ENDPOINT)
    assert pool.rotate_on_error(Exception("connection reset by peer")) is False
    assert pool.key_rotator.current_index == 0


def test_rotate_on_error_is_a_noop_without_a_pool():
    """A single-key (or unset) pool has nothing to rotate to."""
    pool = RotatingHypersyncClient("only_key", ENDPOINT)
    assert pool.rotate_on_error(Exception("429 too many requests")) is False


def test_rotate_on_error_returns_false_once_every_key_has_failed():
    pool = RotatingHypersyncClient("key1,key2", ENDPOINT)
    for key in pool.key_rotator.keys:
        pool.key_rotator.mark_failed(key)

    assert pool.rotate_on_error(Exception("429")) is False
