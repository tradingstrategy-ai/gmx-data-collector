"""Keep historical funding checkpoint staging aligned with extractor paths."""

from pathlib import Path

import yaml

WORKFLOW = Path(".github/workflows/collect-full-history.yml")
FACTOR_SCRIPT = Path("scripts/extract_funding_factor.py")


def test_full_history_stages_funding_checkpoints_and_raw_events_on_failure():
    workflow = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    steps = workflow["jobs"]["funding"]["steps"]
    collect_index = next(
        i for i, step in enumerate(steps) if step.get("name") == "Collect funding rates"
    )
    persist = steps[collect_index + 1]

    assert persist["name"] == "Persist funding checkpoints"
    assert persist["if"] == "always()"
    script = FACTOR_SCRIPT.read_text(encoding="utf-8")
    assert 'output_dir / "checkpoints"' in script
    assert 'output_dir / "raw" / "funding"' in script
    assert '"$DATA_ROOT/funding/arbitrum/checkpoints"' in persist["run"]
    assert '"$DATA_ROOT/funding/arbitrum/raw"' in persist["run"]
    assert "|| true" not in persist["run"]
