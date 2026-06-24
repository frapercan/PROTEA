Release process: trunk + snapshot promotion
===========================================

PROTEA uses two long-lived branches that map to two environments, with one
deliberate promotion step between them. The model is chosen so that the high
volume of automated changes never disturbs production and never triggers heavy
build machinery.

The two branches
----------------

``develop`` (trunk + development environment)
    Every change lands here through a PR (gated by the test / lint / docs /
    integration checks). The development box tracks ``develop`` HEAD and
    redeploys from source (``scripts/manage.sh``). A push to ``develop`` runs
    the cheap test gates only: it never builds a container and never publishes a
    package.

``main`` (production line)
    A clean sequence of single-commit snapshots of ``develop``'s tree, versioned
    by release-please. Production runs a tagged release, not ``main`` HEAD.

Why a snapshot, not a merge
---------------------------

A release is a snapshot of ``develop``'s tree copied wholesale onto ``main`` as
one commit. This is intentional and is not the same as merging the two
histories:

* It is O(1) and conflict-free no matter how far the branches have diverged,
  because nothing is three-way-merged. A ``git merge develop`` would instead
  raise a conflict for every file both branches touched.
* The single clean commit keeps the commit-attribution guard green regardless of
  what is in ``develop``'s history.
* ``main`` stays a readable list of releases rather than a tangle of merges.

Promote to production
---------------------

.. code-block:: bash

   scripts/promote.sh           # open the promotion PR
   scripts/promote.sh --auto    # open it and arm auto-merge

``promote.sh`` snapshots ``develop`` onto a ``release/promote-<sha>`` branch,
keeps the release-lineage files (``CHANGELOG.md``, the release-please manifest,
the ``pyproject`` / ``__init__`` version markers) at ``main``'s released version,
and opens the promotion PR. After it merges:

1. release-please opens a release PR on ``main`` (the version bump + changelog).
2. Merge the release PR. This cuts the tag and is the only event that builds and
   publishes containers (``docker.yml`` and the ``*-container.yml`` workflows are
   gated to ``release: published``).
3. If the release PR sits ``BLOCKED`` with no checks running, close and reopen it
   once to re-trigger them (a known release-please quirk).

Where the heavy machinery fires
-------------------------------

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Event
     - What runs
   * - PR to ``develop``
     - test / lint / docs / integration gates
   * - Push to ``develop``
     - gates only; the dev box redeploys from source
   * - Promotion PR merged
     - nothing heavy; release-please opens the release PR
   * - Release PR merged (tag)
     - container builds + package publish + production deploy

Containers can still be built on demand from any of these workflows through the
``workflow_dispatch`` trigger.
