"""Pure tests for the per-symbol cross-source zero-volume classifier."""

from gmx_historical_data.zero_volume_check import (
    ZERO_VOLUME_SHARE_THRESHOLD,
    ZeroVolumeVerdict,
    classify_zero_volume,
    suspicious_findings,
)


class TestClassifyZeroVolume:
    def test_below_zero_share_threshold_is_not_flagged(self):
        findings = classify_zero_volume(
            zero_share_by_pair={"AAA_USDC_USDC": (0.5, 720, 5.0)},
            open_interest_by_pair={"AAA_USDC_USDC": 1_000_000.0},
            tick_volume_by_pair={"AAA_USDC_USDC": 5.0},
        )

        assert findings == []

    def test_zero_candle_volume_with_nonzero_ticks_is_suspicious(self):
        findings = classify_zero_volume(
            zero_share_by_pair={"AAA_USDC_USDC": (1.0, 720, 0.0)},
            open_interest_by_pair={"AAA_USDC_USDC": 0.0},
            tick_volume_by_pair={"AAA_USDC_USDC": 12.0},
        )

        assert findings[0].verdict is ZeroVolumeVerdict.SUSPICIOUS

    def test_okb_like_sparse_nonzero_candle_volume_is_expected(self):
        findings = classify_zero_volume(
            zero_share_by_pair={"OKB_USDC_USDC": (696 / 721, 721, 25.0)},
            open_interest_by_pair={"OKB_USDC_USDC": 15_000.0},
            tick_volume_by_pair={"OKB_USDC_USDC": 25.0},
        )

        assert findings[0].zero_share >= ZERO_VOLUME_SHARE_THRESHOLD
        assert findings[0].verdict is ZeroVolumeVerdict.EXPECTED

    def test_both_volume_sources_zero_is_expected_regardless_of_oi(self):
        findings = classify_zero_volume(
            zero_share_by_pair={"AAA_USDC_USDC": (1.0, 720, 0.0)},
            open_interest_by_pair={"AAA_USDC_USDC": 5_000_000.0},
            tick_volume_by_pair={"AAA_USDC_USDC": 0.0},
        )

        assert findings[0].verdict is ZeroVolumeVerdict.EXPECTED
        assert findings[0].open_interest_usd == 5_000_000.0

    def test_missing_tick_sidecar_is_unknown(self):
        findings = classify_zero_volume(
            zero_share_by_pair={"AAA_USDC_USDC": (1.0, 720, 0.0)},
            open_interest_by_pair={},
            tick_volume_by_pair=None,
        )

        assert findings[0].verdict is ZeroVolumeVerdict.UNKNOWN

    def test_delisted_pairs_are_excluded_and_findings_are_sorted(self):
        findings = classify_zero_volume(
            zero_share_by_pair={
                "LOW_USDC_USDC": (0.96, 720, 0.0),
                "HIGH_USDC_USDC": (1.0, 720, 0.0),
                "DELISTED_USDC_USDC": (1.0, 720, 0.0),
            },
            open_interest_by_pair={},
            live_pairs={"LOW_USDC_USDC", "HIGH_USDC_USDC"},
            tick_volume_by_pair={},
        )

        assert [f.pair for f in findings] == ["HIGH_USDC_USDC", "LOW_USDC_USDC"]


class TestSuspiciousFindings:
    def test_filters_to_suspicious_only(self):
        findings = classify_zero_volume(
            zero_share_by_pair={
                "QUIET_USDC_USDC": (1.0, 720, 0.0),
                "BUG_USDC_USDC": (1.0, 720, 0.0),
            },
            open_interest_by_pair={},
            tick_volume_by_pair={"BUG_USDC_USDC": 1.0},
        )

        assert [f.pair for f in suspicious_findings(findings)] == ["BUG_USDC_USDC"]
