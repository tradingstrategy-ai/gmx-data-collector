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


def test_release_workflow_checks_out_the_triggering_ref() -> None:
    """Hardcoding ``ref: master`` on checkout made ``workflow_dispatch --ref
    <branch>`` still run the old collector. Scheduled runs on master
    already have ``github.ref = refs/heads/master``.
    """
    import yaml

    workflow = yaml.safe_load(WORKFLOW.read_text())
    checkout = next(
        s for s in workflow["jobs"]["release"]["steps"] if str(s.get("uses", "")).startswith("actions/checkout")
    )
    assert "ref" not in (checkout.get("with") or {})


def test_release_workflow_can_opt_in_to_ohlcv_repair() -> None:
    """Interior 1m holes (2026-09-08 02:17) survive an incremental run.
    ``workflow_dispatch`` must be able to pass ``--repair-ohlcv``; the
    02:00 UTC cron must not, because a 10000-bar re-fetch of every
    timeframe every night is wasted work once the hole is gone.
    """
    text = WORKFLOW.read_text()

    assert "repair_ohlcv:" in text
    assert "default: 'false'" in text
    assert "--repair-ohlcv" in text
    assert "github.event.inputs.repair_ohlcv" in text

    import yaml

    workflow = yaml.safe_load(text)
    collect = next(
        s["run"] for s in workflow["jobs"]["release"]["steps"] if s.get("name") == "Collect daily snapshot"
    )
    assert "collect_daily_snapshot.py" in collect
    assert "--repair-ohlcv" in collect
    # Scheduled runs have empty inputs; the flag is added only when the input is true.
    assert '[ "${{ github.event.inputs.repair_ohlcv }}" = "true" ]' in collect


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
    assert "cadence_issue find-issue" in text


def test_cadence_regression_files_one_issue_per_incident() -> None:
    """A fleet-wide outage is one incident. Cap-of-10 alphabetical slices
    (#33–#42) buried the actual per-file signal in 10 duplicate bodies."""
    text = WORKFLOW.read_text()
    script = _cadence_step()

    assert 'FILED" -ge 10' not in text
    assert "while IFS= read -r FILE" not in script
    assert "cadence-regression:${FILE}" not in script
    assert "cadence_issue format" in script
    assert "cadence-regression-incident:" in script
    assert script.count("gh issue create") == 1


def test_cadence_gate_logic_is_importable_not_inlined() -> None:
    """The embedded-heredoc version of this gate shipped a bug that no
    YAML-substring test could see. The logic must stay in a module that
    tests/test_cadence_gate.py can exercise directly."""
    text = WORKFLOW.read_text()

    for command in ("build", "check", "annotate"):
        assert f"gmx_historical_data.cadence_gate {command}" in text


def _cadence_step() -> str:
    """:returns: The `Validate candle cadence` step's shell script."""
    import yaml

    workflow = yaml.safe_load(WORKFLOW.read_text())
    steps = workflow["jobs"]["release"]["steps"]
    return next(s for s in steps if s.get("name") == "Validate candle cadence")["run"]


def test_regression_flag_is_set_before_any_best_effort_reporting() -> None:
    """The reporting tail runs under `set -e` and *before* packaging, so an
    unguarded failure in it would block the release -- the fail-closed
    behaviour this design exists to avoid. The flag must land first, and
    every call after it must be `||`-guarded."""
    script = _cadence_step()
    flag = script.index('CADENCE_REGRESSION_DETECTED=true" >> "$GITHUB_ENV"')

    for call in (
        "cadence_gate annotate",
        "cadence_issue format",
        "cadence_issue find-issue",
        "gh issue comment",
        "gh issue create",
    ):
        assert flag < script.index(call), f"{call} runs before the flag is set"

    tail = script[flag:]
    assert tail.count("||") >= 5


def test_cadence_gate_exempts_delisted_and_just_relisted_markets() -> None:
    """A relisting seam is a legitimate break (MEGA 1h, 213 bars). The gate
    reads the same roster the futures-integrity step does, plus the roster
    as it stood before this run rewrote it -- a market that relists drops
    off the live roster before collection, which is exactly the run whose
    seam needs exempting."""
    text = WORKFLOW.read_text()

    assert "--delisted-roster ./user_data/data/gmx/delisted_markets.json" in text
    assert "--delisted-roster /tmp/gmx-delisted-baseline.json" in text
    assert "/tmp/gmx-delisted-baseline.json" in text
    assert text.index("Record delisted markets") < text.index("Validate candle cadence")
