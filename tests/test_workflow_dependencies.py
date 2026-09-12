"""The daily cron installs a hand-written package list, not ``pyproject.toml``.

``release-data.yml``, ``collect-gmx-data.yml`` and ``collect-volume.yml`` each
run a repo entry point after a literal ``uv pip install <list>``. That list is
maintained by hand, so it can drift from what the code actually imports -- and
it did. The volume feature added a module-level ``import hypersync``, the list
was never updated, and every scheduled release died at import time for two
days (runs 34554048666 and 34667231822) while ``test.yml`` stayed green
because it installs from ``pyproject.toml`` via Poetry.

Two invariants keep that from recurring:

1. The hand-written list installs what the collector imports.
2. The daily entry point does not import HyperSync at module scope at all, so
   a future drift costs the optional volume phase rather than the release.
"""

import ast
from pathlib import Path

WORKFLOWS = Path(".github/workflows")
SRC = Path("src")
ENTRY_POINT = Path("scripts/collect_daily_snapshot.py")
FIRST_PARTY = "gmx_historical_data"

#: Workflows that install by hand and then run a collector entry point.
COLLECTOR_WORKFLOWS = (
    "release-data.yml",
    "collect-gmx-data.yml",
    "collect-volume.yml",
)


def _install_lines(workflow_name: str) -> list[str]:
    """Return the ``uv pip install`` lines of a workflow.

    :param workflow_name: File name under ``.github/workflows``.
    :returns: Every line containing a ``uv pip install`` invocation.
    """
    text = (WORKFLOWS / workflow_name).read_text(encoding="utf-8")
    return [line for line in text.splitlines() if "uv pip install" in line]


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


def test_collector_workflows_install_hypersync() -> None:
    """HyperSync is a declared ``pyproject.toml`` dependency, but these
    workflows do not read ``pyproject.toml``. Without it in their literal
    list, the tick phase can never collect a single fill."""
    missing = []
    for name in COLLECTOR_WORKFLOWS:
        lines = _install_lines(name)
        assert lines, f"{name}: no `uv pip install` step to check"
        if not any("hypersync" in line for line in lines):
            missing.append(name)

    assert not missing, f"workflows run the collector without installing hypersync: {missing}"


def test_daily_entry_point_does_not_import_hypersync_at_module_scope() -> None:
    """The trade-tick phase is optional by design -- ``collect_and_save_ticks``
    guards every runtime path and degrades to "no volume today".

    A module-level ``import hypersync`` throws that away: it takes down
    candles, OI, funding, tickers and APY, none of which need HyperSync at
    all. Keep the import inside the phase that uses it."""
    closure = _third_party_import_closure(ENTRY_POINT)

    assert "hypersync" not in closure, (
        "collect_daily_snapshot imports hypersync at module scope, so a "
        "missing optional dependency fails the whole release instead of "
        "only the volume phase"
    )
