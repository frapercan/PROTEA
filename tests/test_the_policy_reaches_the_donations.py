"""The donor policy governs what is donated, not only who may donate.

It used to gate the query that admitted PROTEINS to the reference pool and
nothing else. A protein admitted on the strength of one experimental
annotation then donated every annotation it had, so 1,523,939 of 2,801,404
stored rows carried an evidence code the policy excludes, and 1,300 cells
with no experimental prior in their aspect were predicted from the
protein's own IEA row.

Two things have to hold, and the second is the one that is easy to get
wrong. The policy must reach the loads that feed a transfer. And it must
NOT reach the three loads that read a query's OWN terms, because two of
those exist to find non-experimental annotations and a policy filter would
empty them.
"""

from __future__ import annotations

import ast
import pathlib

_PKG = pathlib.Path(__file__).resolve().parents[1] / "protea/core/operations/predict_go_terms"

#: Loads that feed a transfer. The policy belongs on these.
_DONATION_SITES = {
    ("_unified_path.py", "op._load_annotations_for"),
    ("_aspect_helpers.py", "op._load_annotations_for"),
}

#: Loads that read the QUERY's own terms. The policy must stay off these:
#: apply_self_prior wants the non-experimental ones, and the association
#: helpers do their own filtering afterwards.
_OWN_TERM_FILES = {
    "_post_knn_pipeline.py",
    "_category_dispatch.py",
}


def _calls_to(name: str) -> list[tuple[str, int, set[str]]]:
    found: list[tuple[str, int, set[str]]] = []
    for path in _PKG.glob("*.py"):
        for node in ast.walk(ast.parse(path.read_text())):
            if not isinstance(node, ast.Call):
                continue
            if getattr(node.func, "attr", None) != name:
                continue
            found.append((path.name, node.lineno, {k.arg for k in node.keywords if k.arg}))
    return found


def test_every_donation_load_carries_the_policy() -> None:
    missing = [
        f"{f}:{line}"
        for f, line, kwargs in _calls_to("_load_annotations_for")
        if f in {name for name, _ in _DONATION_SITES} and "donor_policy" not in kwargs
    ]
    assert not missing, (
        f"{len(missing)} load(s) that feed a transfer do not pass donor_policy, "
        f"so a protein admitted on one experimental annotation donates every "
        f"annotation it has: {missing}"
    )


def test_the_own_term_loads_stay_unfiltered() -> None:
    """Filtering these would empty apply_self_prior, whose whole purpose is
    the non-experimental terms a protein already carries."""
    wrong = [
        f"{f}:{line}"
        for f, line, kwargs in _calls_to("_load_annotations_for")
        if f in _OWN_TERM_FILES and "donor_policy" in kwargs
    ]
    assert not wrong, (
        f"{len(wrong)} load(s) of a query's OWN terms now carry the donor "
        f"policy. apply_self_prior looks for non-experimental annotations, so "
        f"an experimental-only filter empties it: {wrong}"
    )


def test_the_csr_builder_is_filtered_too() -> None:
    """The aspect path reads a CSR cache and only falls back to a query.
    Filtering the fallback alone leaves almost every real run unfiltered."""
    source = (_PKG / "_batch_op_reference.py").read_text()
    builder = source[source.index("def _collect_aspect_annotations") :]
    builder = builder[: builder.index("return aspect_to_accset")]
    assert "donor_policy" in builder.split(")")[0], (
        "_collect_aspect_annotations does not take a donor policy, so the CSR "
        "it builds is unfiltered and the hot path never sees the policy."
    )
    assert "_restrict_annotations" in builder, (
        "_collect_aspect_annotations takes a policy and does not apply it."
    )
