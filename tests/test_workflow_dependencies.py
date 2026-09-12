"""The daily cron installs generated, pinned requirements -- not a hand list.

``release-data.yml``, ``collect-gmx-data.yml`` and ``collect-volume.yml`` each
run a repo entry point after installing dependencies. That install used to be a
literal ``uv pip install <list>`` maintained by hand, so it could drift from
what the code actually imports -- and it did. The volume feature added a
module-level ``import hypersync``, the list was never updated, and every
scheduled release died at import time for two days (runs 34554048666 and
34667231822) while ``test.yml`` stayed green because it installed from
``pyproject.toml`` via Poetry.

The list is now generated from ``poetry.lock`` by
``scripts/export_requirements.py``. These tests keep the generated file
honest:

1. The workflows install from that file rather than naming packages inline.
2. The committed file matches what the generator produces from the lock.
3. The file covers every third-party package the entry points import at
   module scope.
4. The daily entry point still does not import HyperSync at module scope, so
   a future drift costs the optional volume phase rather than the release.
"""

import ast
import subprocess
import sys
from pathlib import Path

WORKFLOWS = Path(".github/workflows")
SRC = Path("src")
REQUIREMENTS = Path("requirements-collector.txt")
GENERATOR = Path("scripts/export_requirements.py")
FIRST_PARTY = "gmx_historical_data"

#: Workflows that install dependencies and then run a collector entry point.
COLLECTOR_WORKFLOWS = (
    "release-data.yml",
    "collect-gmx-data.yml",
    "collect-volume.yml",
)

#: Entry points those workflows execute, as (label, path) pairs.
ENTRY_POINTS = (
    ("collect_daily_snapshot", Path("scripts/collect_daily_snapshot.py")),
    ("subsquid_volume", SRC / FIRST_PARTY / "subsquid_volume.py"),
)

#: Import name -> distribution name, for the packages whose two names differ.
#: Resolving this at runtime via ``importlib.metadata`` would only work when
#: the package happens to be installed, which is exactly the condition this
#: test exists to stop depending on.
IMPORT_TO_DISTRIBUTION = {
    "eth_defi": "web3-ethereum-defi",
    "eth_utils": "eth-utils",
    "eth_abi": "eth-abi",
    "yaml": "pyyaml",
    "dateutil": "python-dateutil",
}


def _install_lines(workflow_name: str) -> list[str]:
    """Return the ``uv pip install`` lines of a workflow.

    :param workflow_name: File name under ``.github/workflows``.
    :returns: Every line containing a ``uv pip install`` invocation.
    """
    text = (WORKFLOWS / workflow_name).read_text(encoding="utf-8")
    return [line for line in text.splitlines() if "uv pip install" in line]


def _pinned_distributions() -> set[str]:
    """Return the normalised distribution names pinned in the requirements file.

    :returns: Lower-cased names with ``_`` normalised to ``-``, matching the
        comparison form used against import names.
    """
    names = set()
    for line in REQUIREMENTS.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        name = line.split(";")[0].split("==")[0].strip()
        names.add(name.lower().replace("_", "-"))
    return names


def _module_file(dotted: str) -> Path | None:
    """Resolve a first-party dotted module name to its source file.

    :param dotted: Module path, e.g. ``gmx_historical_data.candle_volume``.
    :returns: The module or package file, or ``None`` if neither exists.
    """
    parts = dotted.split(".")
    module = SRC.joinpath(*parts).with_suffix(".py")
    if module.exists():
        return module
    package = SRC.joinpath(*parts, "__init__.py")
    return package if package.exists() else None


def _module_scope_imports(path: Path) -> list[str]:
    """Return the dotted names a file imports *at module scope*.

    Only direct children of the module body count. An import nested in a
    function runs when that function is called, which is exactly the escape
    hatch an optional dependency is supposed to use.

    :param path: Python source file to parse.
    :returns: Dotted module names imported when the file is imported.
    """
    tree = ast.parse(path.read_text(encoding="utf-8"))
    names: list[str] = []
    for node in tree.body:
        if isinstance(node, ast.Import):
            names.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            names.append(node.module)
    return names


def _third_party_import_closure(entry: Path) -> set[str]:
    """Collect third-party packages pulled in by importing ``entry``.

    Walks first-party imports transitively; anything else is recorded as a
    top-level distribution-ish name and not followed.

    :param entry: Entry-point source file.
    :returns: Top-level names of third-party packages imported at module scope.
    """
    seen = {entry}
    queue = [entry]
    third_party: set[str] = set()

    while queue:
        for dotted in _module_scope_imports(queue.pop()):
            top = dotted.split(".")[0]
            if top != FIRST_PARTY:
                third_party.add(top)
                continue
            path = _module_file(dotted)
            if path is not None and path not in seen:
                seen.add(path)
                queue.append(path)

    return third_party


def test_collector_workflows_install_from_generated_requirements() -> None:
    """The whole point is that no workflow names packages inline again.

    A literal package list is what drifted from the code and killed two
    releases; the install must come from the generated file instead."""
    offenders = []
    for name in COLLECTOR_WORKFLOWS:
        lines = _install_lines(name)
        assert lines, f"{name}: no `uv pip install` step to check"
        if not all(REQUIREMENTS.name in line for line in lines):
            offenders.append(name)

    assert not offenders, (
        f"workflows install a hand-written package list instead of {REQUIREMENTS.name}: {offenders}"
    )


def test_committed_requirements_match_the_lock() -> None:
    """A stale generated file is a hand-written list with extra steps.

    ``--check`` re-renders from ``poetry.lock`` and compares, so forgetting to
    regenerate after a dependency change fails a PR instead of a release."""
    result = subprocess.run(
        [sys.executable, str(GENERATOR), "--check"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        f"{REQUIREMENTS.name} is out of sync with poetry.lock.\n{result.stderr}"
    )


def test_requirements_cover_every_module_scope_import() -> None:
    """The invariant the old hand-written list could not hold.

    Every third-party package an entry point imports at module scope must be
    pinned, or the entry point cannot start on the runner."""
    pinned = _pinned_distributions()

    missing: list[str] = []
    for label, entry in ENTRY_POINTS:
        for imported in sorted(_third_party_import_closure(entry)):
            if imported in sys.stdlib_module_names:
                continue
            dist = IMPORT_TO_DISTRIBUTION.get(imported, imported)
            if dist.lower().replace("_", "-") not in pinned:
                missing.append(f"{label} imports {imported!r} (distribution {dist!r})")

    assert not missing, (
        f"entry points import packages that {REQUIREMENTS.name} does not pin: {missing}"
    )


def test_hypersync_is_pinned_for_the_optional_volume_phase() -> None:
    """HyperSync is imported at call time, so the closure check above cannot
    see it -- but the tick phase can never collect a fill without it."""
    assert "hypersync" in _pinned_distributions(), (
        f"{REQUIREMENTS.name} does not pin hypersync, so the trade-tick phase "
        f"degrades to 'no volume today' on every run"
    )


def test_daily_entry_point_does_not_import_hypersync_at_module_scope() -> None:
    """The trade-tick phase is optional by design -- ``collect_and_save_ticks``
    guards every runtime path and degrades to "no volume today".

    A module-level ``import hypersync`` throws that away: it takes down
    candles, OI, funding, tickers and APY, none of which need HyperSync at
    all. Keep the import inside the phase that uses it."""
    closure = _third_party_import_closure(ENTRY_POINTS[0][1])

    assert "hypersync" not in closure, (
        "collect_daily_snapshot imports hypersync at module scope, so a "
        "missing optional dependency fails the whole release instead of "
        "only the volume phase"
    )
