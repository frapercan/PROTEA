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

import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "mutation.yml"
TEMPLATE = ROOT / "cosmic-ray.toml"
SCRIPT = ROOT / "scripts" / "mutation-build-module-map.py"


def _script():
    """Load the hyphenated script by path; it is not importable by name."""
    spec = importlib.util.spec_from_file_location("mutation_build_module_map", SCRIPT)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


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

def test_every_entry_names_a_real_module_and_a_real_test():
    """Both halves have to exist, and neither is checked by the filename.

    The first version asserted ``test_{stem}_property.py``, which assumed the
    test is named after the module it exercises. That holds for one of the three
    property tests in the tree, so the assertion was encoding the defect.
    """
    built = _map_as_the_workflow_builds_it()

    assert built, "no property tests found at all, so the map answers nothing"
    for name, test_file in built.items():
        assert (ROOT / "protea" / "core" / f"{name}.py").is_file(), (
            f"{name!r} is a key and protea/core/{name}.py does not exist, so "
            "mutation.yml's resolver can never produce that path"
        )
        assert (ROOT / test_file).is_file(), f"{test_file} is in the map and not on disk"


def test_the_map_finds_evaluation_through_the_test_that_imports_it():
    """The one that fails if the key goes back to being the test's filename.

    ``tests/property/test_parquet_roundtrip_property.py`` imports
    ``protea.core.evaluation``. Keyed by filename it lands under
    ``parquet_roundtrip``, which is not a module, so touching
    ``protea/core/evaluation.py`` reads as "no property test" and is skipped
    while it has one.
    """
    built = _map_as_the_workflow_builds_it()

    assert built.get("evaluation") == "tests/property/test_parquet_roundtrip_property.py"
    assert "parquet_roundtrip" not in built, (
        "parquet_roundtrip is a test's filename and not a module; a key by that "
        "name is unreachable from any changed path"
    )


def test_a_property_test_of_a_sibling_package_contributes_nothing():
    """Correct rather than unfortunate: the gate's scope is protea/core/."""
    built = _map_as_the_workflow_builds_it()

    assert "contract_payloads" not in built
    imported = _script().core_modules_imported(
        (ROOT / "tests/property/test_contract_payloads_property.py").read_text(
            encoding="utf-8"
        )
    )
    assert imported == set(), "it imports protea_contracts, not protea.core"


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


@pytest.mark.parametrize("module", ["scoring", "evaluation"])
def test_each_module_with_a_kill_criterion_is_reachable(module: str):
    """Named one by one, so losing one fails here and not silently.

    These two are the whole reachable set: 58 top-level modules live in
    ``protea/core/`` and two of them have a property test. The gate is a
    two-module gate, which is worth being able to read off a test.
    """
    built = _map_as_the_workflow_builds_it()

    assert module in built, f"protea/core/{module}.py lost its kill criterion"


# ------------------------------------------------------- reading the imports

def test_a_deep_import_is_not_keyed():
    """mutation.yml resolves only protea/core/*.py, so a deeper path is no use.

    An entry the workflow cannot match is a promise it does not keep: the
    module would appear to have a kill criterion and never be mutated.
    """
    imported = _script().core_modules_imported(
        "from protea.core.ontology.sparse_containment import thing\n"
        "from protea.core.scoring import other\n"
    )

    assert imported == {"scoring"}


def test_a_module_named_in_a_string_is_not_an_import():
    """Parsed and not grepped, which is the point of using ast."""
    imported = _script().core_modules_imported(
        'DOC = "see protea.core.flows for the contract"\n'
        "# from protea.core.metrics import nothing\n"
    )

    assert imported == set()


def test_both_import_forms_are_read():
    imported = _script().core_modules_imported(
        "import protea.core.utils\nfrom protea.core.retry import backoff\n"
    )

    assert imported == {"utils", "retry"}
