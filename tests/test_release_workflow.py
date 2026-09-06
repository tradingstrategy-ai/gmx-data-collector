from pathlib import Path

WORKFLOW = Path(".github/workflows/release-data.yml")


def test_release_workflow_requires_previous_release_unless_explicit_seed() -> None:
    text = WORKFLOW.read_text()

    assert "allow_fresh_seed" in text
    assert "ALLOW_FRESH_SEED" in text
    assert "No previous release found" not in text
    assert "exit 1" in text


def test_release_workflow_restores_explicit_data_release_tag() -> None:
    text = WORKFLOW.read_text()

    assert "PREVIOUS_TAG" in text
    assert '--pattern "gmx-full.tar.gz"' in text
    assert '"${PREVIOUS_TAG}"' in text
    assert "gh release download \\" in text


def test_release_workflow_validates_history_and_partial_fetches() -> None:
    text = WORKFLOW.read_text()

    assert "Snapshot restored candle history" in text
    assert "/tmp/gmx-restore-manifest.json" in text
    assert "Validate candle history integrity" in text
    assert "collector reported failed OHLCV fetches" in text


def test_release_workflow_snapshots_and_validates_cadence() -> None:
    text = WORKFLOW.read_text()

    assert "Snapshot restored cadence manifest" in text
    assert "/tmp/gmx-cadence-baseline.json" in text
    assert "Generate cadence manifest" in text
    assert "Validate candle cadence" in text
    assert "_cadence_manifest.json" in text
    assert "new cadence break" in text


def test_cadence_gate_runs_after_the_manifest_it_reads() -> None:
    """The gate must read the manifest this run just wrote, so it has to sit
    after the step that generates it -- and after the baseline snapshot that
    captured the previous release's copy before collection overwrote it."""
    text = WORKFLOW.read_text()

    assert text.index("Snapshot restored cadence manifest") < text.index(
        "Generate cadence manifest"
    )
    assert text.index("Generate cadence manifest") < text.index("Validate candle cadence")


def test_cadence_gate_retries_before_declaring_a_regression() -> None:
    """A transient read failure must not be reported as a data regression;
    the gate retries up to 3 attempts, matching this repo's existing
    hand-rolled bash retry convention (collect-gmx-data.yml)."""
    text = WORKFLOW.read_text()

    assert "for attempt in 1 2 3; do" in text
    assert "Cadence check attempt $attempt" in text


def test_cadence_regression_ships_and_annotates_rather_than_blocking() -> None:
    """A new break must not starve consumers of the whole release: the data
    still publishes, the manifest records which file regressed, and the job
    only goes red at the very end."""
    text = WORKFLOW.read_text()

    assert "regressed_from" in text
    assert "CADENCE_REGRESSION_DETECTED=true" in text
    assert "Fail job if a new cadence regression was not resolved by retry" in text
    assert text.index("Validate candle cadence") < text.index("Package tarballs")
    assert text.index("Create GitHub Release") < text.index(
        "Fail job if a new cadence regression was not resolved by retry"
    )


def test_cadence_regression_files_a_deduped_issue() -> None:
    text = WORKFLOW.read_text()

    assert "issues: write" in text
    assert "gh issue comment" in text
    assert "gh issue create" in text
    assert "--assignee Aviksaikat" in text


def test_cadence_issue_dedupe_matches_a_marker_not_a_fuzzy_search() -> None:
    """`gh issue list --search` tokenises, so a shared token (`4h`,
    `futures`, `feather`, `usdc`) could rank another symbol's ticket first
    and we would comment on the wrong one. The gate lists open issues once
    and matches the exact hidden marker instead."""
    text = WORKFLOW.read_text()

    assert "in:title cadence regression" not in text
    assert "--json number,body" in text
    assert "cadence_gate find-issue" in text


def test_cadence_gate_logic_is_importable_not_inlined() -> None:
    """The embedded-heredoc version of this gate shipped a bug that no
    YAML-substring test could see. The logic must stay in a module that
    tests/test_cadence_gate.py can exercise directly."""
    text = WORKFLOW.read_text()

    for command in ("build", "check", "annotate"):
        assert f"gmx_historical_data.cadence_gate {command}" in text
