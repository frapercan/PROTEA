"""The protein-binding rule: CAFA's and LAFA's refusal to score GO:0005515 alone.

Lives in its own module because ``core/evaluation.py`` is at the 800-LOC ceiling
the smell budget enforces, and because the rule is pure: it needs no session, so
it can be tested without one.
"""

from __future__ import annotations

#: The one MFO term CAFA and LAFA refuse to score on its own.
#:
#: ``GO:0005515`` is "protein binding", and it is nearly content-free: curators
#: attach it from any interaction experiment, so a predictor that always answers
#: "binds a protein" would score without knowing anything. It is the MFO
#: equivalent of predicting the root. democafa therefore drops a protein's WHOLE
#: F annotation when every F term it carries is this one or a descendant
#: (``democafa/groundtruth/process_ground_truth.py``, "Remove 3 binding terms").
#:
#: MEASURED on GOA 220 before this rule existed here: of 130.456 direct
#: experimental F pairs, **56.199 (43,1%) are binding terms**, and **18.824 of
#: the 60.343 proteins with any F annotation have nothing else in F**. So it is
#: not an edge case: without the rule a third of the MFO targets reward
#: predicting "interacts with something", in the aspect that already carries the
#: widest paired spread of the nine panels.
PROTEIN_BINDING = "GO:0005515"


def drop_binding_only_mfo(
    raw: dict[str, set[str]],
    binding: set[str],
    aspect_by_go_id: dict[str, str],
) -> int:
    """Strip F terms from proteins whose entire F annotation is protein binding.

    Mutates ``raw`` in place and returns how many proteins lost their F side.

    MUST RUN ON THE DIRECT ANNOTATIONS, BEFORE ANCESTOR PROPAGATION, and that
    order is load-bearing rather than incidental. Propagating first adds the
    ancestors of a binding term -- ``GO:0005488`` binding, ``GO:0003674``
    molecular_function -- which are NOT descendants of ``GO:0005515``. The test
    "every F term is a binding term" would then be false for every protein and
    the rule would silently never fire. A guard that cannot refuse is worse than
    no guard, because the panel reports as though it had been applied.

    PER SIDE, NOT UNIONED ACROSS THE DELTA, which mirrors democafa: it processes
    each release's ground truth on its own. One consequence has to be written
    down so nobody later reads it as a defect. If a protein's only F annotation
    at t0 is binding and at t1 it gains a real F term, then t0's F is emptied
    while t1 keeps both, so the binding terms show up as GAINED in that window.
    That is what LAFA scores, and matching it is the point; unioning the
    exclusion across both sides, as the NOT side does, would measure something
    LAFA does not.
    """
    dropped = 0
    for go_ids in raw.values():
        f_terms = {go_id for go_id in go_ids if aspect_by_go_id.get(go_id) == "F"}
        if not f_terms or not f_terms <= binding:
            continue
        go_ids -= f_terms
        dropped += 1
    return dropped
