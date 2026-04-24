"""Tests for cex_gap_fill.symbols module."""

from gmx_historical_data.cex_gap_fill.symbols import normalize_k_prefix


def test_normalize_k_prefix_lowercase_k_prepended_uppercase_base():
    assert normalize_k_prefix("kPEPE") == "KPEPE"


def test_normalize_k_prefix_already_uppercase_unchanged():
    assert normalize_k_prefix("KPEPE") == "KPEPE"


def test_normalize_k_prefix_non_k_prefix_unchanged():
    assert normalize_k_prefix("BTC") == "BTC"


def test_normalize_k_prefix_lowercase_k_lowercase_base_unchanged():
    assert normalize_k_prefix("kitty") == "kitty"


def test_normalize_k_prefix_empty_string_unchanged():
    assert normalize_k_prefix("") == ""
