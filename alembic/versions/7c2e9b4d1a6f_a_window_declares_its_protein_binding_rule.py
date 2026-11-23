"""A window declares its protein-binding rule

Which protein-binding rule built a window's F side was never declared. Every
window on the reconciled path was built under PROTEA's own rule, the
descendant closure of GO:0005515 on direct annotations at both ends, and every
window on the same-snapshot path under none. The author chose CAFA's and
LAFA's rule for the benchmark on 2026-11-20 (a protein's F side is dropped at
t1 when its propagated F set is binding only, and t0 is left alone), so the
rule becomes a truth rule a window declares, like the other four.

``binding_rule`` is the request: a column in the unique key, because it
changes the delta, NOT NULL with a server default of ``descendants_both_ends``
so that no stored window moves.

``truth_rules`` gains ``binding``, what the build APPLIED. Each stored row is
given the rule its own path applied when it was built: ``none`` for the
same-snapshot path, whose closure is ``direct_terms``, and
``descendants_both_ends`` for every other row.

Revision ID: 7c2e9b4d1a6f
Revises: d1f5a8c3e7b2
Create Date: 2026-11-20 14:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "7c2e9b4d1a6f"
down_revision: str | Sequence[str] | None = "d1f5a8c3e7b2"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

#: The model's checks, by the same text; a test compares them.
BINDING_RULE_CHECK = "binding_rule IN ('descendants_both_ends', 'cafa')"

TRUTH_RULES_CHECK = (
    "truth_rules IS NULL OR ("
    "jsonb_typeof(truth_rules) = 'object' "
    "AND COALESCE(truth_rules ->> 'profile', '') IN ('as_closed', 'projected') "
    "AND COALESCE(truth_rules ->> 'toi_rule', '') IN ('pivot_terms', 'live_in_both_ends') "
    "AND COALESCE(truth_rules ->> 'not_codes', '') IN ('any', 'experimental') "
    "AND COALESCE(truth_rules ->> 'closure', '') IN "
    "('per_side', 't0_graph', 't1_graph', 'explicit', 'direct_terms') "
    "AND COALESCE(truth_rules ->> 'binding', '') IN "
    "('descendants_both_ends', 'cafa', 'none'))"
)

#: The text b71e2396e5c3 installed, so a downgrade lands where the chain was.
PREVIOUS_TRUTH_RULES_CHECK = (
    "truth_rules IS NULL OR ("
    "jsonb_typeof(truth_rules) = 'object' "
    "AND COALESCE(truth_rules ->> 'profile', '') IN ('as_closed', 'projected') "
    "AND COALESCE(truth_rules ->> 'toi_rule', '') IN ('pivot_terms', 'live_in_both_ends') "
    "AND COALESCE(truth_rules ->> 'not_codes', '') IN ('any', 'experimental') "
    "AND COALESCE(truth_rules ->> 'closure', '') IN "
    "('per_side', 't0_graph', 't1_graph', 'explicit', 'direct_terms'))"
)

#: The applied rule of each stored row: what its path ran when it was built.
_DECLARE_THE_APPLIED_RULE = (
    "UPDATE evaluation_set SET truth_rules = truth_rules || jsonb_build_object("
    "'binding', CASE WHEN truth_rules ->> 'closure' = 'direct_terms' "
    "THEN 'none' ELSE 'descendants_both_ends' END) "
    "WHERE truth_rules IS NOT NULL"
)

_IDENTITY = (
    "old_annotation_set_id",
    "new_annotation_set_id",
    "pivot_snapshot_id",
    "old_native_snapshot_id",
    "new_native_snapshot_id",
    "truth_profile",
    "toi_rule",
    "not_codes",
    "target_population",
)


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        "evaluation_set",
        sa.Column(
            "binding_rule",
            sa.String(length=24),
            nullable=False,
            server_default="descendants_both_ends",
        ),
    )
    op.create_check_constraint(
        "ck_evaluation_set_binding_rule", "evaluation_set", BINDING_RULE_CHECK
    )
    op.drop_constraint("uq_evaluation_set_identity", "evaluation_set", type_="unique")
    op.create_unique_constraint(
        "uq_evaluation_set_identity", "evaluation_set", [*_IDENTITY, "binding_rule"]
    )
    op.drop_constraint("ck_evaluation_set_truth_rules", "evaluation_set", type_="check")
    op.execute(_DECLARE_THE_APPLIED_RULE)
    op.create_check_constraint("ck_evaluation_set_truth_rules", "evaluation_set", TRUTH_RULES_CHECK)


def downgrade() -> None:
    """Downgrade schema."""
    bind = op.get_bind()
    cafa = bind.execute(
        sa.text(
            "select count(*) from evaluation_set where binding_rule <> 'descendants_both_ends' "
            "or truth_rules ->> 'binding' = 'cafa'"
        )
    ).scalar_one()
    if cafa:
        raise RuntimeError(
            f"{cafa} evaluation_set rows were built under CAFA's binding rule; dropping the "
            "column would make them indistinguishable from windows built under PROTEA's. "
            "Delete those windows first."
        )
    op.drop_constraint("ck_evaluation_set_truth_rules", "evaluation_set", type_="check")
    op.execute(
        "UPDATE evaluation_set SET truth_rules = truth_rules - 'binding' "
        "WHERE truth_rules IS NOT NULL"
    )
    op.create_check_constraint(
        "ck_evaluation_set_truth_rules", "evaluation_set", PREVIOUS_TRUTH_RULES_CHECK
    )
    op.drop_constraint("uq_evaluation_set_identity", "evaluation_set", type_="unique")
    op.create_unique_constraint("uq_evaluation_set_identity", "evaluation_set", list(_IDENTITY))
    op.drop_constraint("ck_evaluation_set_binding_rule", "evaluation_set", type_="check")
    op.drop_column("evaluation_set", "binding_rule")
