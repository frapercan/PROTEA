"""Serving an existing window says which code built it, or says it cannot.

THE DEFECT, found by the desktop session reading #1139 to #1142. The eight
identity fields say what a delta is by DECLARATION; the code says it by
IMPLEMENTATION. On 2026-11-07 a fix to the synonym map changed the delta of a
whole class of windows with every identity field untouched, so a
re-submission after the fix got the OLD truth back under an ``info`` event.
``3bd720c9`` is such a row.

It still serves it: refusing would change what every re-submission does, and
that is the researcher's call. What was wrong was the silence.

AND ``revisions_conflict`` AND NEVER ``!=``, which is the part worth a test of
its own: it returns False when either side is unidentifying, and that is NOT
agreement. A dirty tree is unidentifying, and this fleet runs out of an edited
working tree, so the "cannot be compared" case is the normal one here and has
to be as loud as a conflict.
"""

from __future__ import annotations

import inspect

from protea.core.operations.generate_evaluation_set import _reuse_verdict

SHA_A = "637b3517e525a982694fa5ca4b4b6ed6d5b1ea81"
SHA_B = "d54162248eba37ee9d5e4c6fb961f749763f257e"


class TestTheVerdict:
    def test_a_row_with_no_revision_warns(self, monkeypatch) -> None:
        # 130 of 133 windows are in this state. "Nothing can say" is not
        # "it is fine", so it is not an info.
        monkeypatch.setattr(
            "protea.core.operations.generate_evaluation_set.code_revision", lambda: SHA_A
        )
        level, nota = _reuse_verdict(None)
        assert level == "warning"
        assert "no code_revision" in nota

    def test_a_known_conflict_warns_and_names_both(self, monkeypatch) -> None:
        monkeypatch.setattr(
            "protea.core.operations.generate_evaluation_set.code_revision", lambda: SHA_B
        )
        level, nota = _reuse_verdict(SHA_A)
        assert level == "warning"
        assert SHA_A[:12] in nota and SHA_B[:12] in nota
        assert "Delete it and regenerate" in nota

    def test_agreement_is_an_info(self, monkeypatch) -> None:
        monkeypatch.setattr(
            "protea.core.operations.generate_evaluation_set.code_revision", lambda: SHA_A
        )
        assert _reuse_verdict(SHA_A)[0] == "info"

    def test_a_dirty_tree_is_not_agreement(self, monkeypatch) -> None:
        # The case that matters on this machine: the fleet runs out of a
        # working tree that is edited between runs, so a sha read from it
        # names a commit the running code is not. ``revisions_conflict``
        # returns False for it, which is why a bare ``!=`` would have
        # reported a match.
        monkeypatch.setattr(
            "protea.core.operations.generate_evaluation_set.code_revision",
            lambda: SHA_A + "+dirty",
        )
        level, nota = _reuse_verdict(SHA_A + "+dirty")
        assert level == "warning"
        assert "cannot be compared" in nota
        assert "not" in nota and "agreement" in nota


class TestTheSeam:
    def test_the_event_carries_both_revisions_and_the_level(self) -> None:
        # A verdict nobody emits is a verdict nobody reads. This is the shape
        # that rotted in `_drop_binding_only`, where the return value was
        # computed and thrown away at the call site.
        from protea.core.operations.generate_evaluation_set import (
            GenerateEvaluationSetOperation,
        )

        code = inspect.getsource(GenerateEvaluationSetOperation._maybe_reuse_existing)
        assert "_reuse_verdict" in code
        assert "served_code_revision" in code and "running_code_revision" in code
        # The level is the verdict's, not a constant.
        assert '"info",' not in code.split("idempotent_reuse")[1]

    def test_it_still_serves_the_row(self) -> None:
        # Deliberate: refusing changes what every re-submission does.
        from protea.core.operations.generate_evaluation_set import (
            GenerateEvaluationSetOperation,
        )

        code = inspect.getsource(GenerateEvaluationSetOperation._maybe_reuse_existing)
        assert "return OperationResult(result=result)" in code
        assert "raise" not in code.split('"""')[-1]

    def test_the_rebind_of_d40_is_untouched(self) -> None:
        from protea.core.operations.generate_evaluation_set import (
            GenerateEvaluationSetOperation,
        )

        code = inspect.getsource(GenerateEvaluationSetOperation._maybe_reuse_existing)
        assert "_rebind_window_role" in code

    def test_it_uses_revisions_conflict_and_not_a_comparison(self) -> None:
        code = inspect.getsource(_reuse_verdict)
        assert "revisions_conflict" in code
        assert "served != running" not in code and "served == running" not in code
