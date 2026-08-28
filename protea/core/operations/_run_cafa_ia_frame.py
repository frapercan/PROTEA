"""Name the information-accretion table a weighted number was scored against.

Its own module because it belongs to neither of its neighbours. It is not part
of writing the grid, and it is not part of staging the evaluator's inputs: it
answers what frame the weighted variant lives in, which is a property of the
run and has to travel with the artefact so two files scored against different
tables never compare equal.
"""

from __future__ import annotations

import hashlib
from typing import Any

from protea.core.contracts.operation import EmitFn


def ia_frame(
p: Any, ia_path: str | None, emit: EmitFn
) -> str | None:
    """Name the information-accretion table the weighted numbers were scored against.

    Three cases and they are not interchangeable. A named set has a UUID
    and that is the honest identity. A bare ``ia_file``, or the snapshot's
    own ``ia_url`` downloaded to a temporary path, has no id, and the path
    is a tempdir name that says nothing about the content, so the file's
    sha256 is stamped instead: two different tables then do not compare
    equal, which is the whole point of the key. No IA table at all means
    there is no weighted variant to compare and ``"null"`` is exact rather
    than lossy.

    Never an empty string, and never a guess. The consumer treats an absent
    key and an empty one alike, and an unstamped file compares EQUAL to
    another unstamped file, so the gate would fire on the careful producer
    and never on the forgetful one.

    Returns ``None`` when the table cannot be read, which makes the grid
    artefact refuse itself rather than carry an identity nobody verified.
    The evaluation is NOT failed for it: the aggregate and the legacy
    artefact are unaffected by a stamp, and a run that scored correctly
    should not be discarded because an extra file could not be labelled.
    """
    if p.information_accretion_set_id:
        return str(p.information_accretion_set_id)
    if not ia_path:
        return "null"
    digest = hashlib.sha256()
    try:
        with open(ia_path, "rb") as handle:
            for chunk in iter(lambda: handle.read(1 << 20), b""):
                digest.update(chunk)
    except OSError as exc:
        emit(
            "run_cafa_evaluation.ia_frame_unidentifiable",
            None,
            {
                "ia_path": ia_path,
                "error": str(exc),
                "consequence": (
                    "the per-protein threshold-grid artefact will not be written for "
                    "this run: its frame cannot state which information-accretion "
                    "table the weighted numbers were scored against"
                ),
            },
            "warning",
        )
        return None
    return f"sha256:{digest.hexdigest()}"
