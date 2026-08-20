"""Every backend tag an operation writes must be registered.

``VALID_BACKENDS`` is the only gate on ``embedding_config.model_backend``:
the column is a plain ``varchar`` with no check constraint, so a tag that is
not in that set is a tag the API refuses and the operations write anyway.

Two of them were exactly that for months. ``encode_residue_sparse`` has been
writing ``residue-sparse`` and ``apply_learned_encoder`` has been writing
``learned-code``, both merged, neither registered. Nothing caught it because
the set is hand-maintained: it holds the values somebody remembered, and
remembering is not a mechanism.

This reads the operations instead. It is a static check rather than a
behavioural one on purpose: the alternative is running every operation, and
an operation that writes an unregistered tag should fail review rather than
fail at the moment somebody first dispatches it.
"""

from __future__ import annotations

import ast
import pathlib

from protea.services._embeddings_validation_helpers import VALID_BACKENDS

OPERATIONS = pathlib.Path(__file__).resolve().parent.parent / "protea" / "core" / "operations"


def _module_constants(tree: ast.Module) -> dict[str, str]:
    """Module-level ``NAME = "literal"`` bindings, for indirect writes.

    ``encode_residue_sparse`` passes ``model_backend=TARGET_BACKEND`` rather
    than a literal, so a check that only reads literals at the call site sees
    nothing and passes. That is the operation whose tag was missing.
    """
    out: dict[str, str] = {}
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        if not (len(node.targets) == 1 and isinstance(node.targets[0], ast.Name)):
            continue
        if isinstance(node.value, ast.Constant) and isinstance(node.value.value, str):
            out[node.targets[0].id] = node.value.value
    return out


def _written_backends(path: pathlib.Path) -> set[str]:
    """Resolvable string values passed as ``model_backend=`` in one module.

    Anything not resolvable to a string is skipped rather than guessed: a
    pass-through such as ``model_backend=source.model_backend`` carries
    whatever the source carried, and that value is itself checked wherever it
    was first written.
    """
    tree = ast.parse(path.read_text(encoding="utf-8"))
    consts = _module_constants(tree)
    found: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        for kw in node.keywords:
            if kw.arg != "model_backend":
                continue
            if isinstance(kw.value, ast.Constant) and isinstance(kw.value.value, str):
                found.add(kw.value.value)
            elif isinstance(kw.value, ast.Name) and kw.value.id in consts:
                found.add(consts[kw.value.id])
    return found


def test_every_backend_an_operation_writes_is_registered():
    unregistered: dict[str, set[str]] = {}
    for path in sorted(OPERATIONS.glob("*.py")):
        extra = _written_backends(path) - VALID_BACKENDS
        if extra:
            unregistered[path.name] = extra
    assert not unregistered, (
        "operations write model_backend values that VALID_BACKENDS does not "
        f"accept, so the API would refuse configs the platform creates: {unregistered}"
    )


def test_the_check_can_see_an_indirect_write():
    # The tag that went missing longest was passed through a module constant
    # rather than written at the call site. A check blind to that would have
    # reported clean on the very file that motivated it.
    written = _written_backends(OPERATIONS / "encode_residue_sparse.py")
    assert "residue-sparse" in written


def test_the_check_can_see_a_literal_write():
    written = _written_backends(OPERATIONS / "apply_learned_encoder.py")
    assert "learned-code" in written


def test_the_two_encoder_orders_have_distinct_tags():
    # They partition the artifact space only if they differ. If a later change
    # made both operations write the same tag, an ablation control and a
    # serving candidate would become indistinguishable by filter, which is the
    # property the pairing exists to provide.
    sparse = _written_backends(OPERATIONS / "encode_residue_sparse.py")
    learned = _written_backends(OPERATIONS / "apply_learned_encoder.py")
    assert sparse & VALID_BACKENDS
    assert learned & VALID_BACKENDS
    assert not (sparse & learned)
