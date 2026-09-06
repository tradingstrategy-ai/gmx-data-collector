"""Unit tests for storage._assert_history_preserved's exception taxonomy."""

from datetime import UTC, datetime

import pytest

from gmx_historical_data.ohlcv_validation import ExportValidationError
from gmx_historical_data.storage import _assert_history_preserved


def test_assert_history_preserved_raises_export_validation_error_on_shrink():
    existing_stats = {
        "rows": 10,
        "earliest": datetime(2024, 1, 1, tzinfo=UTC),
        "latest": datetime(2024, 1, 10, tzinfo=UTC),
    }
    incoming_stats = {
        "rows": 5,
        "earliest": datetime(2024, 1, 5, tzinfo=UTC),
        "latest": datetime(2024, 1, 10, tzinfo=UTC),
    }
    merged_stats = {
        "rows": 5,
        "earliest": datetime(2024, 1, 5, tzinfo=UTC),
        "latest": datetime(2024, 1, 10, tzinfo=UTC),
    }
    with pytest.raises(ExportValidationError) as excinfo:
        _assert_history_preserved(
            existing_stats, incoming_stats, merged_stats, ts_label="date", location="TEST/1h"
        )
    assert excinfo.value.reason == "history_shrink"
    assert excinfo.value.location == "TEST/1h"
    assert isinstance(excinfo.value, ValueError)
