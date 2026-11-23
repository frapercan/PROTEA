"""A window can be built by LAFA's frame

LAFA's frame (``protea.core.lafa_frame``) builds a window's truth the way LAFA
builds its own, and a window built by it says so with values no other path
writes: the profile ``own_graph``, the gain rule ``both_ends_trimmed``, the
closure ``per_side_t0_pivot`` in ``truth_rules``, and LAFA's targets, the
population ``swissprot_same_sequence``. The four checks that list those
vocabularies gain them. No stored row moves.

The downgrade refuses while a row holds any of them: dropping the values would
leave rows the restored checks reject.

Revision ID: 5d3a8c1f7b24
Revises: 9e4f2a7c5b13
Create Date: 2026-11-20 18:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "5d3a8c1f7b24"
down_revision: str | Sequence[str] | None = "9e4f2a7c5b13"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

#: The model's checks, by the same text; a test compares them.
TRUTH_PROFILE_CHECK = "truth_profile IN ('as_closed', 'projected', 'own_graph')"
TOI_RULE_CHECK = "toi_rule IN ('pivot_terms', 'live_in_both_ends', 'both_ends_trimmed')"
TARGET_POPULATION_CHECK = (
    "target_population IN ('every_protein', 'superset_at_t0', 'swissprot_same_sequence')"
)
TRUTH_RULES_CHECK = (
    "truth_rules IS NULL OR ("
    "jsonb_typeof(truth_rules) = 'object' "
    "AND COALESCE(truth_rules ->> 'profile', '') IN ('as_closed', 'projected', 'own_graph') "
    "AND COALESCE(truth_rules ->> 'toi_rule', '') IN "
    "('pivot_terms', 'live_in_both_ends', 'both_ends_trimmed') "
    "AND COALESCE(truth_rules ->> 'not_codes', '') IN ('any', 'experimental') "
    "AND COALESCE(truth_rules ->> 'closure', '') IN "
    "('per_side', 't0_graph', 't1_graph', 'per_side_t0_pivot', 'explicit', 'direct_terms') "
    "AND COALESCE(truth_rules ->> 'binding', '') IN "
    "('descendants_both_ends', 'cafa', 'none'))"
)

#: The texts the earlier migrations installed, so a downgrade lands where the
#: chain was; a test compares each with its migration.
PREVIOUS_TRUTH_PROFILE_CHECK = "truth_profile IN ('as_closed', 'projected')"
PREVIOUS_TOI_RULE_CHECK = "toi_rule IN ('pivot_terms', 'live_in_both_ends')"
PREVIOUS_TARGET_POPULATION_CHECK = "target_population IN ('every_protein', 'superset_at_t0')"
PREVIOUS_TRUTH_RULES_CHECK = (
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

_CHECKS = (
    ("ck_evaluation_set_truth_profile", TRUTH_PROFILE_CHECK, PREVIOUS_TRUTH_PROFILE_CHECK),
    ("ck_evaluation_set_toi_rule", TOI_RULE_CHECK, PREVIOUS_TOI_RULE_CHECK),
    (
        "ck_evaluation_set_target_population",
        TARGET_POPULATION_CHECK,
        PREVIOUS_TARGET_POPULATION_CHECK,
    ),
    ("ck_evaluation_set_truth_rules", TRUTH_RULES_CHECK, PREVIOUS_TRUTH_RULES_CHECK),
)

#: The rows the restored checks would reject.
_ROWS_OF_THE_FRAME = (
    "select count(*) from evaluation_set where truth_profile = 'own_graph' "
    "or toi_rule = 'both_ends_trimmed' or target_population = 'swissprot_same_sequence' "
    "or truth_rules ->> 'closure' = 'per_side_t0_pivot'"
)


def upgrade() -> None:
    """Upgrade schema."""
    for name, check, _previous in _CHECKS:
        op.drop_constraint(name, "evaluation_set", type_="check")
        op.create_check_constraint(name, "evaluation_set", check)


def downgrade() -> None:
    """Downgrade schema."""
    rows = op.get_bind().execute(sa.text(_ROWS_OF_THE_FRAME)).scalar_one()
    if rows:
        raise RuntimeError(
            f"{rows} evaluation_set rows were built by LAFA's frame or score its targets; "
            "the restored checks would reject them. Delete those windows first."
        )
    for name, _check, previous in _CHECKS:
        op.drop_constraint(name, "evaluation_set", type_="check")
        op.create_check_constraint(name, "evaluation_set", previous)
