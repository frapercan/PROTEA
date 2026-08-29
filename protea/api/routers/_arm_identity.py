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


_PLACEHOLDER = "{ARM_IDENTITY_COLUMNS}"


def with_arm_identity(sql: str) -> str:
    """Substitute the identity fragment into a query.

    Plain replacement rather than an f-string: these queries carry JSON
    literals such as ``'{}'::jsonb``, and a brace-aware template would need
    every one of them escaped, which is a footgun aimed at the next person to
    edit the SQL rather than at this function.
    """
    if _PLACEHOLDER not in sql:
        raise ValueError("query does not ask for the arm identity columns")
    return sql.replace(_PLACEHOLDER, ARM_IDENTITY_COLUMNS)
