Mutation Testing (F6.3)
=======================

PROTEA's algorithmic core is exercised by Hypothesis property tests
(F6.2) and by example-based unit tests. Both answer the question
"do my tests pass?", but neither answers the deeper question
"are my tests actually checking the behaviour I think they are?".
Mutation testing closes that gap.

`Cosmic Ray <https://github.com/sixty-north/cosmic-ray>`_ rewrites a
target module's AST one operator at a time (swap ``+`` for ``-``,
flip a boolean, replace ``True`` with ``False``, off-by-one a slice,
and so on), runs the full test suite against each mutant, and
classifies the mutant as:

* **killed**: at least one test failed (the test suite caught the
  behavioural change);
* **survived**: every test still passed (a mutation slipped past the
  suite, which means the suite has a blind spot);
* **incompetent**: the mutant crashed at import or syntax time
  (excluded from the score).

The headline metric is the *mutation score*: ``killed / (killed + survived)``.
Higher is better. A score of 1.0 means every behaviour-altering rewrite
was caught by at least one test; a score below 0.5 typically signals
either a coarse-grained test suite or a function whose return value is
not asserted on directly.

Why property tests as the kill criterion
----------------------------------------

The mutation workflow committed in :file:`cosmic-ray.toml` runs the
F6.2 property suite (``tests/property/``) rather than the example-based
unit tests. The reasons are mechanical:

1. Each property test generates ``max_examples`` inputs per invariant
   per execution (200 under the CI profile). A mutation that changes
   the function's behaviour for *any* input in that distribution is
   killed on the first failing example. Example-based tests only
   exercise the inputs the author thought to write down.
2. The CI profile sets ``derandomize=True`` plus a fixed seed
   (``tests/conftest.py``), so identical mutants produce identical
   pytest outcomes across re-runs. Without this pin, a survived/killed
   classification could flip between runs and corrupt the score.
3. Property tests assert on *invariants* (output in ``[0, 1]``,
   ancestor max-monotonicity, sort order) rather than on specific
   return values, which means a single test typically kills a wide
   class of mutations.

The downside is wall-clock time. A property-test invocation costs
~12 s under the CI profile (200 examples) but only ~3 s under the
``dev`` profile (50 examples). Cosmic Ray inherits that cost per
mutant, so a 227-mutant scan over a 286-line module finishes in
roughly:

* 10 to 15 minutes with the ``dev`` profile (used for the reference
  baseline above), and
* 45 to 60 minutes with the CI profile.

The committed test command uses the CI profile so seeds are pinned
across re-runs (see the next section). Operators running an
exploratory sweep on a developer laptop typically swap to ``dev``
by editing :file:`cosmic-ray.toml` for the session. Either profile
produces the same kill / survive classification for the operators
this module exercises, because the surviving mutants here all
survive on every input the strategies emit, not on a long-tail
example only the larger run reaches.

The CI workflow (:file:`.github/workflows/mutation.yml`) runs
incrementally on PR-touched modules only so that the slow profile
does not dominate the merge queue.

How to run locally
------------------

Cosmic Ray installs into the test poetry group (``poetry install --with test``).
The workflow has three stages:

.. code-block:: bash

   # 1. Generate the work-order database. Discovers every mutable
   #    location in protea/core/scoring.py and writes them to
   #    cr-session.sqlite. Fast (~1 s).
   poetry run cosmic-ray init cosmic-ray.toml cr-session.sqlite

   # 2. Run the test command once with no mutation applied. Confirms
   #    that the kill criterion passes on the unmutated source; if
   #    this fails, every mutant would be reported as killed and the
   #    score would be meaningless.
   poetry run cosmic-ray baseline cosmic-ray.toml

   # 3. Execute the work order. Each work item applies one mutation,
   #    runs the test command, records the outcome, and reverts the
   #    source file. Safe to interrupt; the next invocation resumes.
   poetry run cosmic-ray exec cosmic-ray.toml cr-session.sqlite

Once ``exec`` finishes, the session database is ready for reporting:

.. code-block:: bash

   # Human-readable summary with surviving mutants highlighted.
   poetry run cr-report cr-session.sqlite --show-output

   # Just the percentage (good for CI badges or quick smoke checks).
   poetry run cr-rate cr-session.sqlite

   # Mutants the suite did NOT catch, with file/line annotations.
   poetry run cr-report cr-session.sqlite --surviving-only --show-diff

   # Self-contained HTML report (one file, browseable).
   poetry run cr-html cr-session.sqlite > mutation-report.html

To re-target another module, edit ``module-path`` in
:file:`cosmic-ray.toml`. Scope each session to a single module
(``protea/core/scoring.py``, ``protea/core/evaluation.py``, etc.);
a full ``protea/core/`` sweep generates several thousand mutants
and is impractical on a developer laptop.

Reference baseline (scoring.py)
-------------------------------

The reference baseline is the first module wired into the mutation
workflow.

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Field
     - Value
   * - Module
     - ``protea/core/scoring.py`` (286 lines)
   * - Total mutations generated
     - 227
   * - Killed
     - 173
   * - Survived
     - 54
   * - Mutation score
     - 76.21 % (survival rate 23.79 %)
   * - Captured at commit
     - ``7da0962`` (feat/f63-cosmic-ray)
   * - Recorded on
     - 2026-05-16
   * - Kill criterion
     - ``tests/property/test_scoring_property.py`` (Hypothesis ``dev`` profile, 50 examples per invariant)

How to interpret the score:

* Aim for >= 0.80 on pure-algorithmic modules where every behaviour
  is observable from the public API. Lower scores on such modules
  point to missing assertions.
* Lower scores are acceptable on modules whose public surface is
  intentionally permissive (e.g. logging, caching). Inspect
  surviving mutants individually rather than chasing the rate.
* A score that drops between revisions is a regression signal worth
  investigating, even if both numbers are nominally healthy.

Surviving mutants of interest
-----------------------------

The interesting mutants are not the trivial ones (renaming an
unused local, replacing a constant that has no observable effect)
but those that change documented behaviour and still slip past the
tests. After the reference run, audit the ``cr-report --surviving-only``
output and either:

* extend the property suite with the invariant the mutant violates,
  then re-run cosmic-ray; the same mutant should now be killed;
* or, if the mutation does not actually change observable behaviour
  (a true equivalent mutant), accept it. Cosmic Ray has no built-in
  exclusion marker for equivalent mutants, so document them in the
  PR description rather than in code comments.

Reference baseline (scoring.py), survivor breakdown:

.. list-table::
   :header-rows: 1
   :widths: 50 15 35

   * - Operator family
     - Count
     - Notes
   * - ``ReplaceBinaryOperator_BitOr_*``
     - 19
     - All equivalent mutants. ``str | None`` annotations are
       not evaluated at runtime (the module uses
       ``from __future__ import annotations``), so swapping ``|``
       for ``+`` / ``-`` / ``*`` etc in a type hint leaves
       observable behaviour unchanged.
   * - ``NumberReplacer``
     - 8
     - Mostly affect default weight constants whose property tests
       randomise the override (so the default is rarely
       exercised). A targeted test that exercises the bare
       default would close most of these.
   * - ``AddNot`` plus boolean / comparison swaps
     - 14
     - Several survivors flip predicates inside branch tails
       that are not separately asserted (e.g. defensive ``if
       value is None`` guards that the property strategies
       always satisfy). Either tighten the strategies to also
       generate the guarded path, or accept as defensive code
       outside the invariant boundary.
   * - ``ZeroIterationForLoop``
     - 3
     - Loops that aggregate into a dict where the property
       test's invariant happens to hold for the empty
       aggregation. A direct length-of-result assertion would
       kill these.
   * - Other ``ReplaceBinaryOperator`` / ``Replace*`` survivors
     - 10
     - Heterogeneous; audit case by case.

Subtracting the 19 equivalent-mutant survivors gives an *effective*
mutation score of 173 / (227 - 19) = 83.17 %, which is the number
to compare future revisions against once the equivalents are
filtered out.

Continuous integration
----------------------

The :file:`.github/workflows/mutation.yml` workflow runs cosmic-ray
on a PR's touched modules only. It is informational
(``continue-on-error: true``), not blocking: a survived mutant
should prompt a review conversation, not auto-fail the merge.

Triggers:

* PRs that modify ``protea/core/**``, ``tests/property/**``,
  ``cosmic-ray.toml``, or the workflow file itself;
* manual ``workflow_dispatch`` from the Actions tab.

The job:

1. Diffs the PR against its base branch to discover which
   ``protea/core/*.py`` files changed;
2. Renders a per-module cosmic-ray config (inheriting timeout,
   test command, and distributor from the committed template);
3. Runs ``cosmic-ray init`` plus ``baseline`` plus ``exec`` for each
   touched module;
4. Uploads the per-module report as a workflow artifact
   (``mutation-reports``).

A future hardening pass can promote the job to a required check
once the per-module baselines are stable and contributors are
familiar with reading cosmic-ray output.

Operational caveats
-------------------

* Cosmic Ray rewrites the source file in place during ``exec``. If
  you interrupt the process with ``SIGKILL``, the file may be left
  in a mutated state; recover with ``git checkout protea/core/scoring.py``.
  ``SIGINT`` (Ctrl+C) is safe: the mutating context manager reverts
  the file before exiting.
* Pytest's ``-p no:cacheprovider`` flag is set in the committed
  test command because cosmic-ray spawns many short-lived pytest
  subprocesses; without it, ``.pytest_cache`` churns on every
  invocation and produces no useful output.
* The ``timeout`` setting (60 s in :file:`cosmic-ray.toml`) is the
  per-mutant wall-clock cap. A mutation that introduces an infinite
  loop is classified as killed-by-timeout, which is the right call
  (the suite caught a behavioural regression) but may inflate the
  score on modules that legitimately run long. Lower the timeout
  if false positives become a concern.

See also
--------

* :doc:`/guides/plugin-authoring/index` for how the plugin contracts
  layer is itself tested.
* The F6.2 property tests under ``tests/property/`` for the suite
  that cosmic-ray relies on as its kill criterion.
* `Cosmic Ray documentation <https://cosmic-ray.readthedocs.io/>`_
  for the full operator catalogue and distributor options.
