"""Property tests need ``hypothesis``, which lives in an optional group.

``[tool.poetry.group.test]`` is declared ``optional = true``, so a plain
``poetry install`` does not provide ``hypothesis`` and every module in this
directory raises ``ModuleNotFoundError`` at IMPORT time. A collection error is
not a test failure: pytest reports it as an error, exits non-zero, and gives no
indication that the cause is a dependency the project deliberately made
optional.

Collecting nothing and saying why is the honest outcome. Running with
``poetry install --with test`` collects and runs them normally.
"""

from __future__ import annotations

collect_ignore_glob: list[str] = []

try:  # pragma: no cover - the branch taken depends on the environment
    import hypothesis  # noqa: F401
except ImportError:  # pragma: no cover
    collect_ignore_glob.append("*.py")

    def pytest_report_collectionfinish() -> str:
        return (
            "property tests skipped: hypothesis is in the optional poetry group "
            "'test'. Run `poetry install --with test` to include them."
        )
