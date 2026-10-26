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
#: equivalent of predicting the root, so democafa drops a protein's WHOLE F
#: annotation when its F side falls entirely within the binding terms
#: (``democafa/groundtruth/process_ground_truth.py``, "Remove 3 binding terms").
#:
#: WHAT "THE BINDING TERMS" MEANS IN democafa, AND IT IS NOT WHAT THIS MODULE
#: USES. democafa builds the set as
#: ``nx.descendants(F_subgraph, "GO:0005515") | {"GO:0005515"}``
#: (``classify_ground_truth.py:118-134``, democafa_package at 1102e34e). obonet
#: orients its edges from child to parent, so ``nx.descendants`` walks UPWARD
#: and returns superterms: the set is exactly
#: ``{GO:0005515, GO:0005488, GO:0003674}`` -- protein binding, binding, and the
#: molecular_function root. That is why democafa's own comment says THREE terms.
#:
#: This module is called with the opposite set. ``evaluation.py:526`` descends
#: children from ``GO:0005515``, so ``binding`` here is the full descendant
#: closure -- thousands of specific binding terms. The subset test below is the
#: same shape as democafa's; the set it tests against is not, and neither is the
#: moment (democafa tests the PROPAGATED t1 set, this runs on direct annotations
#: before propagation) nor the side (democafa leaves t0 untouched).
#:
#: MEASURED against LAFA's own published truth for Sep_2025_Mar_2026: this
#: module's set and timing would drop F entries LAFA keeps -- 6 of 135 in NK,
#: 7 of 156 in LK and 38 of 1.086 in PK, for instance a protein whose only F
#: term is ``GO:0042802``. So the two rules are different rules and must be
#: named rather than merged; the LAFA variant is not implemented here yet, and
#: ``tests/test_lafa_profile_acceptance.py`` pins the divergence meanwhile.
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

    MUST RUN ON THE DIRECT ANNOTATIONS, BEFORE ANCESTOR PROPAGATION, as long as
    ``binding`` is the DESCENDANT closure of ``GO:0005515``, which is what
    ``evaluation.py:526`` passes. Propagating first adds ``GO:0005488`` binding
    and ``GO:0003674`` molecular_function, which are not in that closure, so the
    subset test would be false for every protein and the rule would silently
    never fire. A guard that cannot refuse is worse than no guard, because the
    panel reports as though it had been applied.

    That constraint is a consequence of the set, not a property of the rule.
    democafa tests against the three ancestors instead, and therefore runs AFTER
    propagation, where the test fires exactly for the proteins whose propagated F
    side is nothing but those three. See this module's header.

    PER SIDE, NOT UNIONED ACROSS THE DELTA. This was written as mirroring
    democafa, and that attribution is wrong: democafa applies the rule to the t1
    side only, and LAFA's published truth confirms it -- 134 of its PK entries in
    F carry exactly those three terms at t0 and are kept. Running it on t0 as
    well is this module's own choice and it changes the population. One consequence has to be written
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
