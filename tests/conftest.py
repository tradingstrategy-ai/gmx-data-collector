import pytest


@pytest.fixture(autouse=True)
def clear_legacy_raw_schema_tracking():
    from gmx_historical_data.raw_schema import _legacy_files

    _legacy_files.clear()
    yield
    _legacy_files.clear()
