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

So the map emits the test FILE, the consumer rewrites ``test-command`` with it,
and a module with no property test is SKIPPED and said out loud. Skipping
reports nothing; the old behaviour reported zero kills, which is worse than
nothing because it looks like a finding.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

TEST_DIR = Path("tests/property")
NAME = re.compile(r"^test_(.+)_property\.py$")


def build(test_dir: Path = TEST_DIR) -> dict[str, str]:
    """``{module stem: path of the property test that exercises it}``.

    Keyed by the module's file stem, which is what the workflow can derive from
    a changed path. Only files that exist are emitted, so a consumer that finds
    no entry knows the module has no kill criterion rather than guessing one.
    """
    found: dict[str, str] = {}
    for test_file in sorted(test_dir.glob("test_*_property.py")):
        match = NAME.match(test_file.name)
        if match:
            found[match.group(1)] = str(test_file)
    return found


def main() -> int:
    json.dump(build(), sys.stdout)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
