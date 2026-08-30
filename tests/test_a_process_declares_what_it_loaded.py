"""A process declares the revision it loaded, not what the tree says now.

WHY THIS TEST EXISTS. ``code_revision()`` asked git on every call, which
answers a different question: it reports what the WORKING TREE says at that
moment, not what the process imported. Those coincide only while nobody touches
the tree.

On 2026-08-30 they did not. A branch was checked out in the deploy tree while a
twelve arm retrieval sweep was running. The workers had loaded their code at
06:04 and never reloaded, so every arm computed identically, but arms 7 and 8
recorded a revision the running processes had never held. Six arms carry a true
label and two carry a false one, produced by the reporting path of the guard
that exists to prevent exactly that.

The data was never wrong. The label was. That distinction is the reason this is
a reporting fix and not a recomputation.

A process cannot reload its own modules because a file changed underneath it,
so the revision at import IS the revision of the code that will run. The two
answers now differ precisely when something is wrong.
"""

from __future__ import annotations

import protea.core.code_revision as cr


def test_the_stamp_survives_a_tree_that_moves_underneath() -> None:
    """The exact situation: git starts answering something else mid-run.

    Driven by making the reader return a different sha, which is what a
    checkout does, and asserting the already-imported process does not follow
    it.
    """
    before = cr.code_revision()
    original = cr.resolve_protea_git_sha
    try:
        cr.resolve_protea_git_sha = lambda: "f" * 40  # type: ignore[assignment]
        assert cr.code_revision() == before, "the stamp followed the tree"
        # And the tree reader, which exists for callers that want the tree,
        # does see the move. Without this the test would pass on a module that
        # simply cached a constant and could never report drift at all.
        assert cr.tree_revision_now() != before
    finally:
        cr.resolve_protea_git_sha = original  # type: ignore[assignment]


def test_the_stamp_is_taken_at_import_not_at_first_call() -> None:
    """A worker that never receives a batch must still declare correctly.

    Taking it lazily would mean the first caller decides, and on this fleet the
    first caller can be an hour after start.
    """
    assert isinstance(cr._REVISION_AT_IMPORT, str)
    assert cr._REVISION_AT_IMPORT
    assert cr.code_revision() == cr._REVISION_AT_IMPORT


def test_the_two_readings_agree_on_an_untouched_tree() -> None:
    """The normal case has to stay silent, or the difference means nothing."""
    assert cr.code_revision() == cr.tree_revision_now()


def test_the_tree_reader_is_not_used_to_label_anything() -> None:
    """It exists for drift reporting, and using it as a label is the defect.

    Read from source across the package, because a future caller reaching for
    the wrong one of two similarly named functions is exactly how this returns.
    """
    from pathlib import Path

    root = Path(__file__).resolve().parents[1] / "protea"
    callers = [
        f"{path.relative_to(root)}:{i + 1}"
        for path in root.rglob("*.py")
        for i, line in enumerate(path.read_text().splitlines())
        if "tree_revision_now(" in line
        and "def tree_revision_now" not in line
        and "code_revision.py" not in str(path)
    ]
    assert not callers, f"tree_revision_now used outside its module: {callers}"


def test_the_startup_line_carries_the_revision() -> None:
    """grep has to answer "what was this process running" on its own.

    The line carried queue, repo and a clock verdict and not the revision,
    which is why a tree moving under two live arms was invisible until the
    labels were read back out of the database hours later.
    """
    from pathlib import Path

    src = Path(__file__).resolve().parents[1] / "scripts/worker.py"
    text_ = src.read_text()
    at = text_.index('"Worker started.')
    line = text_[at - 400 : at + 400]
    assert "revision=%s" in line
    # And it must say so when the tree has moved away from what it loaded,
    # because that is the only moment the difference matters.
    assert "TREE_HAS_MOVED_TO" in line
    assert "code_revision()" in line and "tree_revision_now()" in line
