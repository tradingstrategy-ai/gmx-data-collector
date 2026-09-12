"""Import everything the collector workflows run, under the production install.

``tests/test_workflow_dependencies.py`` checks this statically: it parses the
AST and asserts the requirements file pins every module-scope import. That
catches a *missing* pin, but not a broken one -- a package that resolves yet
fails to import, a transitive dependency dropped upstream, or a version whose
API moved. Only actually importing under the real install catches those.

So CI installs ``requirements-collector.txt`` -- byte for byte what the daily
release installs -- and runs this. A dependency problem then fails a PR
instead of the nightly release, which is how two releases died at import time
(runs 34554048666 and 34667231822).

The targets are derived from the workflows rather than listed here, for the
same reason the package list is generated rather than hand-written: a list
maintained by hand drifts from what actually runs. Adding a `python -m ...`
step to a collector workflow extends this smoke test automatically.

Run locally against a production-shaped environment::

    uv venv /tmp/smoke && VIRTUAL_ENV=/tmp/smoke \\
        uv pip install -r requirements-collector.txt
    VIRTUAL_ENV=/tmp/smoke uv run --no-project python scripts/smoke_imports.py
"""

import importlib
import importlib.util
import re
import sys
from pathlib import Path

WORKFLOWS = Path(".github/workflows")
FIRST_PARTY = "gmx_historical_data"

#: Workflows that install dependencies and then run a collector entry point.
COLLECTOR_WORKFLOWS = (
    "release-data.yml",
    "collect-gmx-data.yml",
    "collect-volume.yml",
)

#: `python -m gmx_historical_data.cadence_gate build ...`
MODULE_RUN = re.compile(rf"python\s+-m\s+({FIRST_PARTY}[.\w]*)")

#: `from gmx_historical_data.market_registry import ...` inside a heredoc.
FROM_IMPORT = re.compile(rf"from\s+({FIRST_PARTY}[.\w]*)\s+import")

#: `python code/scripts/collect_daily_snapshot.py --output-dir ...`
SCRIPT_RUN = re.compile(r"python\s+((?:[\w./-]+/)?scripts/[\w.-]+\.py)")


def _workflow_text() -> str:
    """Return every collector workflow concatenated.

    :returns: The combined YAML source, searched as plain text -- these appear
        inside ``run:`` blocks and heredocs, which a YAML parser would hand
        back as opaque strings anyway.
    """
    return "\n".join((WORKFLOWS / name).read_text(encoding="utf-8") for name in COLLECTOR_WORKFLOWS)


def discover() -> tuple[set[str], set[Path]]:
    """Find the modules and scripts the collector workflows execute.

    Workflow paths are written relative to a checkout that some workflows put
    under ``code/``; that prefix is stripped so the path resolves from the
    repository root either way.

    :returns: Tuple of (dotted module names, entry-point script paths).
    """
    text = _workflow_text()

    modules = set(MODULE_RUN.findall(text)) | set(FROM_IMPORT.findall(text))

    scripts = set()
    for raw in SCRIPT_RUN.findall(text):
        path = Path(raw)
        if path.parts and path.parts[0] == "code":
            path = Path(*path.parts[1:])
        scripts.add(path)

    return modules, scripts


def _import_script(path: Path) -> None:
    """Execute an entry-point script's module scope.

    The ``if __name__ == "__main__"`` guard does not fire, because the module
    is loaded under its own name -- so this exercises every module-level
    import without running a collection.

    :param path: Entry-point script to load.
    :raises ImportError: If the file cannot be loaded at all.
    """
    spec = importlib.util.spec_from_file_location(path.stem, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load {path}")
    spec.loader.exec_module(importlib.util.module_from_spec(spec))


def main() -> int:
    """Import every discovered target, reporting each outcome.

    :returns: Process exit code -- ``1`` if any import failed.
    """
    sys.path.insert(0, "src")

    modules, scripts = discover()
    if not modules and not scripts:
        print("ERROR: no entry points discovered in the collector workflows", file=sys.stderr)
        return 1

    failures: list[str] = []

    for name in sorted(modules):
        try:
            importlib.import_module(name)
        except Exception as exc:  # noqa: BLE001 - report every failure, not the first
            failures.append(f"{name}: {exc!r}")
            print(f"  FAIL  {name}: {exc}")
        else:
            print(f"  ok    {name}")

    for path in sorted(scripts):
        try:
            _import_script(path)
        except Exception as exc:  # noqa: BLE001 - report every failure, not the first
            failures.append(f"{path}: {exc!r}")
            print(f"  FAIL  {path}: {exc}")
        else:
            print(f"  ok    {path}")

    print(f"\n{len(modules) + len(scripts)} targets, {len(failures)} failed")

    if failures:
        print(
            "\nThe collector cannot start under the installed dependencies. "
            "If a package is missing, add it to pyproject.toml, then run: "
            "poetry lock && python scripts/export_requirements.py",
            file=sys.stderr,
        )
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
