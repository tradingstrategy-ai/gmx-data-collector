"""Tests for atomic_parquet's data-defect vs. fatal-environment taxonomy."""

import errno

import polars as pl
import pytest
from pyarrow.lib import ArrowInvalid

from gmx_historical_data.atomic_parquet import (
    CORRUPT_PARQUET_ERRORS,
    DATA_DEFECT_ERRORS,
    is_fatal_environment_error,
)
from gmx_historical_data.ohlcv_validation import ExportValidationError


def test_corrupt_parquet_errors_alias_still_importable():
    """Back-compat: the deprecated alias keeps its historical value."""
    assert CORRUPT_PARQUET_ERRORS == (ArrowInvalid, pl.exceptions.ComputeError, OSError)


def test_data_defect_errors_includes_export_validation_error():
    assert ExportValidationError in DATA_DEFECT_ERRORS
    assert ArrowInvalid in DATA_DEFECT_ERRORS
    assert pl.exceptions.ComputeError in DATA_DEFECT_ERRORS
    assert OSError in DATA_DEFECT_ERRORS


@pytest.mark.parametrize(
    "errno_value", [errno.ENOSPC, errno.EROFS, errno.EDQUOT, errno.EMFILE, errno.ENFILE]
)
def test_is_fatal_environment_error_true_for_fatal_errnos(errno_value):
    exc = OSError(errno_value, "simulated")
    assert is_fatal_environment_error(exc) is True


def test_is_fatal_environment_error_false_for_other_oserror():
    exc = OSError(errno.ENOENT, "file not found")
    assert is_fatal_environment_error(exc) is False


def test_is_fatal_environment_error_false_for_non_oserror():
    assert is_fatal_environment_error(ValueError("not an OSError")) is False


def test_is_fatal_environment_error_false_for_export_validation_error():
    exc = ExportValidationError("X/1h", "non_monotonic", "X/1h: non-monotonic timestamps")
    assert is_fatal_environment_error(exc) is False


def test_is_fatal_environment_error_true_for_bare_oserror_with_os_error_suffix():
    """Reproduces DataFrame.write_ipc()'s real shape on a full disk: a bare
    OSError with errno=None, the real errno only present as message text.
    Confirmed empirically against a real loop-mounted full filesystem."""
    exc = OSError("No space left on device (os error 28)")
    assert exc.errno is None  # sanity-check the fixture matches the real shape
    assert is_fatal_environment_error(exc) is True


def test_is_fatal_environment_error_true_for_compute_error_with_os_error_suffix():
    """Reproduces DataFrame.write_parquet()'s real shape on a full disk:
    ComputeError (not even an OSError), same message suffix. Confirmed
    empirically against a real loop-mounted full filesystem."""
    exc = pl.exceptions.ComputeError(
        "parquet: File out of specification: underlying IO error: "
        "No space left on device (os error 28)"
    )
    assert is_fatal_environment_error(exc) is True


def test_is_fatal_environment_error_false_for_compute_error_without_os_error_suffix():
    """A ComputeError from genuine data corruption (no OS-error suffix) must
    still be classified as a data defect, not fatal."""
    exc = pl.exceptions.ComputeError("Parquet magic bytes not found in footer")
    assert is_fatal_environment_error(exc) is False


def test_is_fatal_environment_error_false_for_non_fatal_os_error_suffix():
    """A permission-denied message (os error 13, not in the fatal set) must
    not be misclassified as fatal even though it matches the message shape."""
    exc = OSError("Permission denied (os error 13)")
    assert is_fatal_environment_error(exc) is False
