"""The fields a published arm is told apart by, written once.

WHY ONE PLACE. Three surfaces name arms: the graph panels, the strata list and
the per-protein stratum view. Each carried its own copy of the same CASE
expression, and a copy is how a field comes to be added to two of three. The
same shape produced the defect this project keeps meeting, where a level is
named by fewer fields than it varies in, so the fragment lives here and the
three read it.

WHY THE CODE REVISION IS ONE OF THEM. ``donor_policy`` records which evidence
codes a donor needed. Until 2026-08-29 that set gated which proteins entered
the reference pool; afterwards it gates which annotations may be donated. The
stored object is byte-identical across that change, so one string, 'evidence
gated', names two incompatible experiments and nothing else in the record
separates them. The revision does, and it separates every future change of the
same kind without anyone having to notice it in advance. A set produced before
the revision was recorded reads 'unrecorded', which is a different level from
any sha and correctly refuses to share an axis with one.

WHY IT IS NOT INTERPRETED. It would be possible to map old sets onto 'admission
gated' and new ones onto 'donation gated'. That reads better and asserts more
than the record holds: nothing in an old row says which semantics ran, only
that they cannot be established. Naming the level by the revision says exactly
what is known.

WHY DEPTH IS HERE TOO. Three different quantities used to be folded into one
column called ``depth`` by a COALESCE: ``ps.limit_per_entry``, which is the
RETRIEVAL depth and decides which candidates were ever fetched, and
``er.max_sequence_rank`` and ``er.max_k_position``, which are cuts taken at
evaluation time over a candidate list that had already been retrieved. Only the
sequence cut carried a mark, the suffix 'seq'; a k-position cut of 10 and a
retrieval depth of 10 both rendered as the bare string '10'. The record holds
both today: prediction set d5b634b2 was retrieved at depth 10 and evaluated
both uncut and cut at k-position 10, and those sixteen results shared one level
name. The cut is a truncation and the retrieval is a re-run, so a ladder over
one of them read as a ladder over the other is the project's recurring defect
in its plainest form, and the rendering now says which quantity it is.
"""

from __future__ import annotations

#: A select-list fragment. Expects the prediction set aliased ``ps``.
#: Interpolated into ``text()`` and never near user input.
ARM_IDENTITY_COLUMNS = """
           CASE
               WHEN ps.meta -> 'donor_policy' ->> 'evidence_codes' IS NULL
                   THEN 'permissive'
               ELSE 'evidence-gated'
           END                                      AS donor_policy,
           CASE
               WHEN ps.meta ->> 'exclude_self_neighbour' IS NULL
                   THEN 'self-unrecorded'
               WHEN (ps.meta ->> 'exclude_self_neighbour')::boolean
                   THEN 'self-excluded'
               ELSE 'self-allowed'
           END                                      AS self_exclusion,
           CASE
               WHEN jsonb_typeof(ps.meta -> 'features') = 'array'
                   THEN COALESCE(
                       (SELECT string_agg(replace(f, 'compute_', ''), '+' ORDER BY f)
                          FROM jsonb_array_elements_text(ps.meta -> 'features') AS f),
                       'none')
               ELSE 'unrecorded'
           END                                      AS features,
           COALESCE(left(ps.meta ->> 'code_revision', 7), 'unrecorded')
                                                    AS code_revision"""

#: A select-list fragment naming the depth a result was read at, and WHICH of
#: the three depths that is. Expects the evaluation result aliased ``er`` and
#: the prediction set aliased ``ps``.
#:
#: The order of the branches is the precedence the COALESCE this replaces
#: already had, and it is right: a result that declares a cut was read at that
#: cut whatever its prediction set was retrieved at, and only a result that
#: declares none was read at the whole retrieval.
DEPTH_IDENTITY_COLUMN = """
           CASE
               WHEN er.max_sequence_rank IS NOT NULL
                   THEN 'cut at sequence rank ' || er.max_sequence_rank::text
               WHEN er.max_k_position IS NOT NULL
                   THEN 'cut at protein rank ' || er.max_k_position::text
               WHEN ps.limit_per_entry IS NOT NULL
                   THEN 'retrieval depth ' || ps.limit_per_entry::text
               ELSE 'unrecorded'
           END                                      AS depth"""

#: What :func:`depth_kind` can answer. Two of them are quantities and the third
#: is the absence of one, which is a third answer rather than a missing one: a
#: row that does not say what its depth was cannot be put on either axis.
RETRIEVAL_DEPTH = "retrieval depth"
EVALUATION_CUT = "evaluation cut"
UNRECORDED_DEPTH = "unrecorded depth"


def depth_kind(depth: str | None) -> str:
    """Which of the three quantities a rendered depth is.

    Read off the rendering rather than carried in a column of its own. A second
    column would be a second thing to name a level by, and a level named by two
    fields that always move together is how a surface comes to print one of
    them and drop the other. The prefix is produced here and parsed here, so
    the vocabulary has exactly one owner.

    Anything this module did not write reads as unrecorded. That covers the
    bare integers older callers and fixtures pass, and it is the honest answer:
    such a string says a number and not which quantity the number is of.
    """
    if depth is None:
        return UNRECORDED_DEPTH
    if depth.startswith("retrieval "):
        return RETRIEVAL_DEPTH
    if depth.startswith("cut "):
        return EVALUATION_CUT
    return UNRECORDED_DEPTH


_PLACEHOLDER = "{ARM_IDENTITY_COLUMNS}"
_DEPTH_PLACEHOLDER = "{DEPTH_IDENTITY_COLUMN}"


def with_arm_identity(sql: str) -> str:
    """Substitute the identity fragments into a query.

    Plain replacement rather than an f-string: these queries carry JSON
    literals such as ``'{}'::jsonb``, and a brace-aware template would need
    every one of them escaped, which is a footgun aimed at the next person to
    edit the SQL rather than at this function.

    The arm fragment is required and the depth fragment is substituted only
    when asked for. Every surface that names an arm needs the first; only the
    surfaces that name a RESULT can produce the second, because two of the
    three depths live on ``evaluation_result`` and a query that has no such row
    in scope has no cut to report. Which surfaces ask is pinned by
    ``tests/test_three_surfaces_name_an_arm_alike.py`` rather than by a raise
    here, so a surface cannot quietly stop asking.
    """
    if _PLACEHOLDER not in sql:
        raise ValueError("query does not ask for the arm identity columns")
    sql = sql.replace(_PLACEHOLDER, ARM_IDENTITY_COLUMNS)
    return sql.replace(_DEPTH_PLACEHOLDER, DEPTH_IDENTITY_COLUMN)
