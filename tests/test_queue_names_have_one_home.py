"""Queue names are written once, in ``protea/core/queues.py`` (ADR-D60, decision 6).

The operation specs declare their queues from that module, and so must every
code path that names a queue: a literal written anywhere else is a second
spelling that can drift from the spec without anything noticing. That is how
the request example of ``POST /jobs`` came to name ``protea.embedding``, a
queue that never existed.

The guard reads every Python file under ``protea/`` and ``scripts/`` and fails
on a string literal shaped like a broker name. It does not flag docstrings, log
messages, logger names or span attributes, which name a queue without routing
to it; nor dotted module paths, whose second word is a package of ``protea``;
nor the plugin entry-point groups, which share the shape. Shell scripts name
queues on command lines rather than in literals the code routes by, so they
stay outside it.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

from protea.core import queues
from protea.core.operation_catalog import build_operation_registry

ROOT = Path(__file__).resolve().parents[1]
#: The one file where a queue name may be written.
HOME = ROOT / "protea" / "core" / "queues.py"

#: The shape of a broker name: ``protea.`` and dotted lower-case words.
SHAPED_LIKE_A_QUEUE = re.compile(r"protea\.[a-z][a-z0-9_-]*(?:\.[a-z0-9_-]+)*")
#: Names of that shape that are not queues: the plugin entry-point groups.
NOT_QUEUES = frozenset({"protea.backends", "protea.runners", "protea.sources"})
#: Calls whose string arguments name a queue without routing to it.
NOT_ROUTING = frozenset(
    {
        "debug",
        "info",
        "warning",
        "warn",
        "error",
        "exception",
        "critical",
        "log",
        "getLogger",
        "set_attribute",
    }
)
#: The packages and modules of ``protea``: ``protea.core.x`` is a module path.
PACKAGES = frozenset(
    path.stem
    for path in (ROOT / "protea").iterdir()
    if (path.is_dir() and (path / "__init__.py").exists()) or path.suffix == ".py"
)


def _exempt(tree: ast.AST) -> set[int]:
    """The ids of the string nodes that are docstrings or arguments of a non-routing call."""
    exempt: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Module | ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef):
            body = node.body
            if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant):
                exempt.add(id(body[0].value))
        elif isinstance(node, ast.Call):
            func = node.func
            name = func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", None)
            if name in NOT_ROUTING:
                exempt.update(id(sub) for arg in node.args for sub in ast.walk(arg))
                exempt.update(id(sub) for kw in node.keywords for sub in ast.walk(kw.value))
    return exempt


def queue_literals(source: str) -> list[tuple[int, str]]:
    """The (line, value) of every literal in ``source`` that names a queue."""
    tree = ast.parse(source)
    exempt = _exempt(tree)
    found = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Constant) or not isinstance(node.value, str):
            continue
        value = node.value
        if id(node) in exempt or not SHAPED_LIKE_A_QUEUE.fullmatch(value):
            continue
        if value in NOT_QUEUES or value.split(".")[1] in PACKAGES:
            continue
        found.append((node.lineno, value))
    return sorted(found)


def test_queue_names_are_written_only_in_their_module() -> None:
    offenders = [
        f"{path.relative_to(ROOT)}:{line}: {value}"
        for folder in ("protea", "scripts")
        for path in sorted((ROOT / folder).rglob("*.py"))
        if path != HOME
        for line, value in queue_literals(path.read_text(encoding="utf-8"))
    ]
    assert offenders == [], "use the constants of protea.core.queues:\n" + "\n".join(offenders)


def test_the_guard_sees_a_new_literal_and_lets_prose_through() -> None:
    flagged = queue_literals(
        '"""A module that names protea.jobs in prose."""\n'
        'NEW = "protea.photos"\n'
        'publish(url, "protea.jobs.files", payload)\n'
        'logger.info("protea.jobs")\n'
        'logging.getLogger("protea.emit")\n'
        'span.set_attribute("protea.job_id", job_id)\n'
        'MODULE = "protea.core.operations.ping"\n'
        'GROUP = "protea.backends"\n'
        'PREFIX = "protea."\n'
    )
    assert flagged == [(2, "protea.photos"), (3, "protea.jobs.files")]


def test_the_module_declares_the_queues_the_specs_use() -> None:
    specs = build_operation_registry().specs().values()
    assert {spec.queue for spec in specs} == queues.WORK_QUEUES
    assert queue_literals(HOME.read_text(encoding="utf-8")) != []
