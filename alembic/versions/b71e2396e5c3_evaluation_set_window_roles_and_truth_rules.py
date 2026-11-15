"""evaluation_set takes the roles of a frame and stores the truth rules it applied

ADR-D60, decision 1: a frame is windows with a role (``training``, ``validation``
or ``test_spent``) and declared truth rules, stored with each window. Two changes
to ``evaluation_set``, both additive.

THE ROLES. ``window_role`` keeps ADR-D40's ``valid`` and ``test``, because rows
carry them, and gains the three of a frame. The column grows from 8 to 16
characters: ``validation`` and ``test_spent`` are 10.

THE RULES. ``truth_rules`` is a JSON object with four keys, ``profile``,
``toi_rule``, ``not_codes`` and ``closure``, holding what the build applied and
not what its payload asked for. The CHECK closes every vocabulary, and a missing
key or a JSON null fails it: ``->>`` gives NULL for both, and a CHECK that
evaluates to NULL passes, hence the COALESCE.

NOTHING IS BACKFILLED. Every existing row keeps NULL, which reads as legacy:
what a past build applied is a question about the code that ran then, and most
of those rows cannot even say which code that was.

Revision ID: b71e2396e5c3
Revises: b7c3f91d2a04
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

from alembic import op

revision = "b71e2396e5c3"
down_revision = "b7c3f91d2a04"
branch_labels = None
depends_on = None

#: The same text the ORM model declares; a test compares the two.
WINDOW_ROLE_CHECK = (
    "window_role IS NULL OR window_role IN "
    "('valid', 'test', 'training', 'validation', 'test_spent')"
)

#: What the CHECK said before this revision, restored by the downgrade.
PREVIOUS_WINDOW_ROLE_CHECK = "window_role IS NULL OR window_role IN ('valid', 'test')"

TRUTH_RULES_CHECK = (
    "truth_rules IS NULL OR ("
    "jsonb_typeof(truth_rules) = 'object' "
    "AND COALESCE(truth_rules ->> 'profile', '') IN ('as_closed', 'projected') "
    "AND COALESCE(truth_rules ->> 'toi_rule', '') IN ('pivot_terms', 'live_in_both_ends') "
    "AND COALESCE(truth_rules ->> 'not_codes', '') IN ('any', 'experimental') "
    "AND COALESCE(truth_rules ->> 'closure', '') IN "
    "('per_side', 't0_graph', 't1_graph', 'explicit', 'direct_terms'))"
)


def upgrade() -> None:
    op.alter_column(
        "evaluation_set",
        "window_role",
        existing_type=sa.String(length=8),
        type_=sa.String(length=16),
        existing_nullable=True,
    )
    op.drop_constraint("ck_evaluation_set_window_role", "evaluation_set", type_="check")
    op.create_check_constraint("ck_evaluation_set_window_role", "evaluation_set", WINDOW_ROLE_CHECK)
    op.add_column("evaluation_set", sa.Column("truth_rules", JSONB(), nullable=True))
    op.create_check_constraint("ck_evaluation_set_truth_rules", "evaluation_set", TRUTH_RULES_CHECK)


def downgrade() -> None:
    op.drop_constraint("ck_evaluation_set_truth_rules", "evaluation_set", type_="check")
    op.drop_column("evaluation_set", "truth_rules")
    # The CHECK first and the width second. A row already holding one of the
    # three new roles makes the CHECK fail here, by name, which is correct:
    # narrowing the vocabulary while rows use it leaves a role the schema
    # forbids and the data asserts. Narrowing the width first would fail too,
    # but on a length, which says less.
    op.drop_constraint("ck_evaluation_set_window_role", "evaluation_set", type_="check")
    op.create_check_constraint(
        "ck_evaluation_set_window_role", "evaluation_set", PREVIOUS_WINDOW_ROLE_CHECK
    )
    op.alter_column(
        "evaluation_set",
        "window_role",
        existing_type=sa.String(length=16),
        type_=sa.String(length=8),
        existing_nullable=True,
    )
