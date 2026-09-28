"""Per-symbol zero-volume check -- collector bug or genuinely quiet market?

Some GMX markets are listed but barely traded (BOME, BRETT, MEME, MEW, SATS
today), so a pair whose recent candles are almost entirely zero-volume is not
by itself evidence of a bug. What distinguishes "nobody is trading this" from
"the collector stopped writing volume for this pair" is open interest: a
pair with ~zero OI alongside its zero volume is genuinely dormant, while a
pair carrying real OI but reporting zero volume is suspicious -- positions
are open, so *something* should be trading.
"""

from gmx_historical_data.zero_volume_check import (
    NEGLIGIBLE_OI_USD,
    ZERO_VOLUME_SHARE_THRESHOLD,
    ZeroVolumeVerdict,
    classify_zero_volume,
    suspicious_findings,
)


class TestClassifyZeroVolume:
    def test_below_threshold_is_not_flagged(self):
        findings = classify_zero_volume(
            zero_share_by_pair={"AAA_USDC_USDC": (0.5, 720)},
            open_interest_by_pair={"AAA_USDC_USDC": 1_000_000.0},
        )

        assert findings == []

    def test_at_or_above_threshold_is_flagged(self):
        findings = classify_zero_volume(
            zero_share_by_pair={"AAA_USDC_USDC": (ZERO_VOLUME_SHARE_THRESHOLD, 720)},
            open_interest_by_pair={"AAA_USDC_USDC": 1_000_000.0},
        )

        assert len(findings) == 1
        assert findings[0].pair == "AAA_USDC_USDC"

    def test_zero_volume_with_negligible_oi_is_expected(self):
        """BOME/BRETT/MEME/MEW/SATS today: 100% zero volume, ~$0 OI."""
        findings = classify_zero_volume(
            zero_share_by_pair={"BOME_USDC_USDC": (1.0, 720)},
            open_interest_by_pair={"BOME_USDC_USDC": 0.0},
        )

        assert findings[0].verdict is ZeroVolumeVerdict.EXPECTED

    def test_oi_exactly_at_the_negligible_floor_is_expected(self):
        findings = classify_zero_volume(
            zero_share_by_pair={"AAA_USDC_USDC": (1.0, 720)},
            open_interest_by_pair={"AAA_USDC_USDC": NEGLIGIBLE_OI_USD},
        )

        assert findings[0].verdict is ZeroVolumeVerdict.EXPECTED

    def test_zero_volume_with_real_oi_is_suspicious(self):
        findings = classify_zero_volume(
            zero_share_by_pair={"AAA_USDC_USDC": (1.0, 720)},
            open_interest_by_pair={"AAA_USDC_USDC": 5_000_000.0},
        )

        assert findings[0].verdict is ZeroVolumeVerdict.SUSPICIOUS

    def test_missing_oi_data_is_unknown_not_a_crash(self):
        findings = classify_zero_volume(
            zero_share_by_pair={"AAA_USDC_USDC": (1.0, 720)},
            open_interest_by_pair={},
        )

        assert findings[0].verdict is ZeroVolumeVerdict.UNKNOWN
        assert findings[0].open_interest_usd is None

    def test_delisted_pairs_are_excluded_via_live_pairs(self):
        findings = classify_zero_volume(
            zero_share_by_pair={
                "AAA_USDC_USDC": (1.0, 720),
                "DELISTED_USDC_USDC": (1.0, 720),
            },
            open_interest_by_pair={},
            live_pairs={"AAA_USDC_USDC"},
        )

        pairs = {f.pair for f in findings}
        assert pairs == {"AAA_USDC_USDC"}

    def test_no_live_pairs_filter_considers_everything(self):
        findings = classify_zero_volume(
            zero_share_by_pair={"AAA_USDC_USDC": (1.0, 720)},
            open_interest_by_pair={},
            live_pairs=None,
        )

        assert len(findings) == 1

    def test_findings_sorted_by_descending_zero_share(self):
        findings = classify_zero_volume(
            zero_share_by_pair={
                "LOW_USDC_USDC": (0.96, 720),
                "HIGH_USDC_USDC": (1.0, 720),
            },
            open_interest_by_pair={},
        )

        assert [f.pair for f in findings] == ["HIGH_USDC_USDC", "LOW_USDC_USDC"]

    def test_custom_thresholds_are_honoured(self):
        findings = classify_zero_volume(
            zero_share_by_pair={"AAA_USDC_USDC": (0.5, 720)},
            open_interest_by_pair={"AAA_USDC_USDC": 50.0},
            share_threshold=0.4,
            negligible_oi_usd=100.0,
        )

        assert len(findings) == 1
        assert findings[0].verdict is ZeroVolumeVerdict.EXPECTED


class TestSuspiciousFindings:
    def test_filters_to_suspicious_only(self):
        findings = classify_zero_volume(
            zero_share_by_pair={
                "QUIET_USDC_USDC": (1.0, 720),
                "BUG_USDC_USDC": (1.0, 720),
                "UNKNOWN_USDC_USDC": (1.0, 720),
            },
            open_interest_by_pair={
                "QUIET_USDC_USDC": 0.0,
                "BUG_USDC_USDC": 2_000_000.0,
            },
        )

        suspicious = suspicious_findings(findings)

        assert [f.pair for f in suspicious] == ["BUG_USDC_USDC"]


def test_default_thresholds_are_documented_constants():
    assert ZERO_VOLUME_SHARE_THRESHOLD == 0.95
    assert NEGLIGIBLE_OI_USD == 1_000.0
