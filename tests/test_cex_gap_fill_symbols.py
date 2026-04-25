"""Tests for cex_gap_fill.symbols module."""

from gmx_historical_data.cex_gap_fill.symbols import (
    gmx_symbol_to_cex_base,
    gmx_symbol_to_cex_pair,
    normalize_k_prefix,
    price_scale_for,
)


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


def test_gmx_symbol_to_cex_base_plain_symbol_unchanged():
    assert gmx_symbol_to_cex_base("BTC") == "BTC"


def test_gmx_symbol_to_cex_base_bonk_becomes_1000bonk():
    assert gmx_symbol_to_cex_base("BONK") == "1000BONK"


def test_gmx_symbol_to_cex_base_applies_k_prefix_then_1000_remap():
    # kPEPE → normalize → KPEPE → strip K → PEPE → in GMX_TO_BINANCE_NAME → 1000PEPE
    assert gmx_symbol_to_cex_base("kPEPE") == "1000PEPE"


def test_gmx_symbol_to_cex_pair_format():
    assert gmx_symbol_to_cex_pair("BTC") == "BTC/USDT:USDT"


def test_gmx_symbol_to_cex_pair_for_1000_prefixed_symbol():
    assert gmx_symbol_to_cex_pair("BONK") == "1000BONK/USDT:USDT"


def test_price_scale_for_plain_symbol_is_one():
    assert price_scale_for("BTC") == 1.0


def test_price_scale_for_bonk_is_thousandth():
    assert price_scale_for("BONK") == 1 / 1000
