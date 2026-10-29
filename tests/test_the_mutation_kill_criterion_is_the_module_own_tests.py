"""A mutation pass must be judged by the tests of the module it mutates.

THE DEFECT THIS LOCKS OUT, measured on 2026-10-29. The workflow rendered each
touched module's cosmic-ray config by rewriting ``module-path`` alone, so
``test-command`` stayed pinned to the template's
``tests/property/test_scoring_property.py``. A dispatch on ``develop`` therefore
mutated ``protea/core/_leaf_record_builder.py`` while ``protea.core.scoring``'s
property tests decided whether each mutant lived, and reported **195 SURVIVED,
0 killed** out of 427 planned. That reads as a module with no tests. It means
the kill criterion never imported the module.

AND THE EXISTING GUARD CANNOT SEE IT, which is why the checks below are about
the wiring and not about an outcome. ``cosmic-ray baseline`` runs the test
command against UNMUTATED code and demands a pass; another module's passing
tests pass. Everything baseline can observe is green.

These tests read the workflow and the template as text, the same way
``test_count_backend_parameters`` reads a backend's source: the knowledge is
duplicated between a YAML step and the config it rewrites, so it has to be
checked against them rather than restated.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "mutation.yml"
TEMPLATE = ROOT / "cosmic-ray.toml"
SCRIPT = ROOT / "scripts" / "mutation-build-module-map.py"


def _map_as_the_workflow_builds_it() -> dict[str, str]:
    """Run the script the way the step does: as a subprocess, from the root.

    Driven through the real entry point rather than by calling ``build()`` with
    a directory of the test's choosing, because the paths it emits are resolved
    against the working directory and the workflow's working directory is the
    repository root. A test that passed its own absolute directory would assert
    on a path shape production never produces.
    """
    done = subprocess.run(
        [sys.executable, str(SCRIPT)], cwd=ROOT, capture_output=True, text=True, check=True
    )
    return json.loads(done.stdout)


# --------------------------------------------------------------------- the map

def test_the_map_emits_a_test_file_that_exists_for_every_property_test():
    """A map entry whose file is missing would point the pass at nothing."""
    built = _map_as_the_workflow_builds_it()

    assert built, "no property tests found at all, so the map answers nothing"
    for stem, test_file in built.items():
        assert Path(test_file).name == f"test_{stem}_property.py"
        assert (ROOT / test_file).is_file(), f"{test_file} is in the map and not on disk"


def test_the_map_is_silent_about_a_module_with_no_property_test():
    """The miss has to be a miss, so the consumer can skip instead of guess."""
    built = _map_as_the_workflow_builds_it()

    assert "_leaf_record_builder" not in built
    assert built.get("nothing_called_this") is None


def test_the_map_emits_paths_and_not_a_name_mapped_to_itself():
    """The shape that made the output unusable, and then unused."""
    for stem, test_file in _map_as_the_workflow_builds_it().items():
        assert test_file != stem, (
            "the map emits a bare module name again; the workflow needs the test "
            "FILE, and a name mapped to itself is what made this output dead"
        )
        assert test_file.startswith("tests/property/")


# ---------------------------------------------------------------- the wiring

def test_the_workflow_rewrites_the_test_command_and_not_only_the_module_path():
    """The one that fails if the kill criterion goes back to being inherited.

    It looks for the SUBSTITUTION PATTERN and not for the words. The first
    version of this test asserted ``"test-command" in body``, which the step's
    own prose satisfies: deleting the rewrite left it green. Found by putting
    the defect back, which is the only way that kind of test announces itself.
    """
    body = WORKFLOW.read_text(encoding="utf-8")

    assert r'module-path\s*=' in body, "the module-path rewrite is gone"
    assert r'test-command\s*=' in body, (
        "mutation.yml renders a per-module cosmic-ray config without a regex "
        "that rewrites test-command, so every module is mutated against "
        "whichever tests the template names. That reports zero kills and reads "
        "as a coverage result."
    )
    assert "re.subn(" in body, (
        "the test-command rewrite has to count its substitutions and refuse on "
        "zero: a silent miss runs the pass with another module's tests"
    )


def test_the_workflow_consumes_the_map_it_builds():
    """It built the map to a file nothing read for as long as it existed."""
    body = WORKFLOW.read_text(encoding="utf-8")

    built = body.count("property_test_map.json")
    assert built >= 2, (
        "property_test_map.json is written and never read, so the map has no "
        "effect and the test-command stays whatever the template says"
    )


def test_the_workflow_skips_a_module_with_no_kill_criterion():
    """Skipping reports nothing; the old path reported zero kills."""
    body = WORKFLOW.read_text(encoding="utf-8")

    assert "continue" in body
    assert "::warning" in body, "a skipped module has to be said out loud"


# --------------------------------------------------------------- the template

def test_the_template_names_a_property_test_the_workflow_can_swap():
    """The rewrite is a regex over the template, so they have to agree.

    If the template's ``test-command`` stops naming a ``tests/property/`` file
    the substitution matches nothing, and the workflow is written to raise
    rather than run a pass whose tests may belong to another module. This test
    says so here too, because a template edit is the likely way it happens.
    """
    template = TEMPLATE.read_text(encoding="utf-8")
    command = [
        line for line in template.splitlines() if line.startswith("test-command")
    ]

    assert len(command) == 1, "the template must carry exactly one test-command"
    assert "pytest tests/property/" in command[0], (
        "cosmic-ray.toml's test-command no longer names a tests/property/ file, "
        "so mutation.yml cannot point the kill criterion at the mutated module"
    )


@pytest.mark.parametrize("stem", ["scoring", "contract_payloads", "parquet_roundtrip"])
def test_each_property_test_is_reachable_through_the_map(stem: str):
    """Named one by one, so losing a property test fails here and not silently."""
    built = _map_as_the_workflow_builds_it()

    assert stem in built, f"tests/property/test_{stem}_property.py disappeared"
