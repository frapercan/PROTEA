#!/usr/bin/env python3
"""Map a ``protea/core`` module to the property test that may kill its mutants.

WHY THE MAP HAS TO BE USED AND NOT JUST BUILT. Until 2026-10-29 this script
printed ``{"scoring": "scoring"}`` to a file nothing read, and
``.github/workflows/mutation.yml`` rendered each module's cosmic-ray config by
rewriting ``module-path`` ALONE, leaving ``test-command`` pinned to
``tests/property/test_scoring_property.py`` for every module. So a touched
module other than ``scoring`` was mutated while a different module's tests
decided whether the mutant lived.

Every mutant then survives, and the report says so in the voice of a coverage
result: the dispatch of 2026-10-29 reported **195 SURVIVED and 0 killed** on
``protea/core/_leaf_record_builder.py``, which reads as a module with no tests
and actually means the kill criterion never imported it.

``cosmic-ray baseline`` cannot catch this, and that is the part worth keeping in
mind when adding a guard here: baseline runs the test command against UNMUTATED
code and demands a pass. Another module's passing tests pass. The check is
structurally blind to a test command aimed at the wrong module, because the only
thing it can observe is green.

AND WHY THE KEY IS AN IMPORT AND NOT A FILENAME. The first version of this map
keyed each test by its own filename: ``test_<stem>_property.py`` became the
entry ``<stem>``, on the assumption that a test is named after the module it
exercises. Measured against the tree on 2026-10-29, that assumption holds for
ONE of the three property tests:

    test_scoring_property.py            imports protea.core.scoring       agrees
    test_parquet_roundtrip_property.py  imports protea.core.evaluation    does NOT
    test_contract_payloads_property.py  imports protea_contracts          not in scope

So ``protea/core/evaluation.py`` has a kill criterion and a filename-keyed map
can never find it: touching it would be reported as "no property test" and
skipped, which is an honest-sounding sentence about a module that has one. The
convention is not wrong often enough to notice and not right often enough to
rely on, which is the worst of the two.

The key is therefore read from what each test IMPORTS, and only for modules that
exist as ``protea/core/<name>.py`` -- the shape ``mutation.yml``'s resolver can
match. A test that imports a sibling package contributes nothing, which is
correct rather than unfortunate: the gate's scope is ``protea/core/``.

THE HONEST SCOPE OF THE GATE, measured the same day: 58 top-level modules live
in ``protea/core/`` and **2** of them have a property test that can kill a
mutant. The gate is a two-module gate. Nothing here changes that; it makes the
two reachable instead of one, and makes the other 56 say so out loud.
"""

from __future__ import annotations

import ast
import json
import sys
from pathlib import Path

TEST_DIR = Path("tests/property")
CORE_DIR = Path("protea/core")
PREFIX = "protea.core."


def core_modules_imported(source: str) -> set[str]:
    """The ``protea.core`` module names this source imports, by name.

    Parsed rather than grepped, so a module named inside a string or a comment
    does not count as an import. Both forms are read: ``from protea.core.x
    import y`` and ``import protea.core.x``. A deeper path such as
    ``protea.core.ontology.sparse_containment`` yields nothing, because
    ``mutation.yml`` only resolves top-level ``protea/core/*.py`` and an entry
    it cannot match would be a promise the workflow does not keep.
    """
    found: set[str] = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.ImportFrom) and node.module:
            dotted = node.module
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.startswith(PREFIX):
                    tail = alias.name[len(PREFIX) :]
                    if "." not in tail:
                        found.add(tail)
            continue
        else:
            continue
        if dotted.startswith(PREFIX):
            tail = dotted[len(PREFIX) :]
            if "." not in tail:
                found.add(tail)
    return found


def build(test_dir: Path = TEST_DIR, core_dir: Path = CORE_DIR) -> dict[str, str]:
    """``{module name: path of the property test that imports it}``.

    Keyed by the module's file stem, which is what the workflow derives from a
    changed path. Only modules that exist on disk are emitted, so a consumer
    that finds no entry knows the module has no kill criterion rather than
    guessing one. When two property tests import the same module the first by
    filename wins, deterministically, because cosmic-ray takes one test command.
    """
    found: dict[str, str] = {}
    for test_file in sorted(test_dir.glob("test_*_property.py")):
        source = test_file.read_text(encoding="utf-8")
        for name in sorted(core_modules_imported(source)):
            if (core_dir / f"{name}.py").is_file():
                found.setdefault(name, str(test_file))
    return found


def main() -> int:
    json.dump(build(), sys.stdout)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
