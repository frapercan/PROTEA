"""A row carries the donors that voted for it

WHY. A stored row is one (protein, term) pair holding vote_count, the
neighbor_* aggregates and a vote fraction. Those are functions of the
neighbourhood the retrieval used, so truncating that neighbourhood later does
not change them: an arm cut to depth 2 carries a consensus measured over depth
30 and reports it as its own. Recomputing them at the cut was not possible
because the detail was gone. Measured on the live store: 1,086,706 rows for
1,086,706 distinct (protein, term) pairs, a ratio of 1.000. The donors were
summed away at write time.

WHAT THESE HOLD. One entry per DISTINCT donor, parallel across the arrays and
ordered by position. A cut at depth d then recounts rather than inherits.

WHY DONOR_COUNT IS NOT VOTE_COUNT. vote_count counts annotation rows, not
donors: 5,518,069 of 14,694,523 (protein, term) pairs carry more than one such
row, up to sixteen. That is how a ten-neighbour retrieval stores a vote
fraction of 4.9 on 104,627 rows, with an implied divisor of exactly 10 across
all 1,839,492 rows. vote_count is left alone here rather than redefined under
its readers; the two sit side by side so the gap is measurable.

NULLABLE, AND NULL IS NOT EMPTY. Every row retrieved before these columns
existed gets null, which says the retrieval predates the question. An empty
array would say the term had no donors, which cannot happen: a term is on the
row because something donated it. The producer lands with the migration rather
than after it.

NO INDEX. Nothing filters on these; they are read alongside a row already
selected by prediction_set_id. An index on an array column that no predicate
touches is pure write cost.

Revision ID: a4f8d2b91c73
Revises: e3c8b7a1f592
"""

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "a4f8d2b91c73"
down_revision = "e3c8b7a1f592"
branch_labels = None
depends_on = None

_COLUMNS = (
    ("donor_accessions", postgresql.ARRAY(sa.String())),
    ("donor_k_positions", postgresql.ARRAY(sa.Integer())),
    ("donor_sequence_ranks", postgresql.ARRAY(sa.Integer())),
    ("donor_distances", postgresql.ARRAY(sa.Float())),
    ("donor_count", sa.Integer()),
)


def upgrade() -> None:
    for name, type_ in _COLUMNS:
        op.add_column("go_prediction", sa.Column(name, type_, nullable=True))


def downgrade() -> None:
    for name, _ in reversed(_COLUMNS):
        op.drop_column("go_prediction", name)
