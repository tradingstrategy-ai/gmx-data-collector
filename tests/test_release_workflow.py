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
