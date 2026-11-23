"""A retrieval asked about a list records the list's digest, and so does its scored set

A prediction set names the proteins it was asked about in ``query_set_id``, or,
since this revision, in ``meta.query_population`` when the run was handed a list
(``protea.core.query_population``): the list's size and the sha256 of its sorted
accessions. The method seal reads either one as the population declared
(``protea.core.method_seal``) and refuses a set that declares neither.

The retrievals that ran before this revision were handed their lists and kept
them in their job's payload (``query_accessions``) but not on the set, so every
comparison between them was refused. This writes each one the digest of the list
its job was handed, computed as the new code computes it (a test holds the two
to each other), and gives each scored set the population of the candidates it
was scored from. A set that names a query set, or already records a population,
or whose job is gone or was handed no list, keeps what it has.

The downgrade takes the key away: the code before this revision classifies no
``query_population`` in the seal and would refuse every set carrying one.

Revision ID: 7c5e1f0a9d42
Revises: a5e954c5bab4
Create Date: 2026-11-21 20:30:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "7c5e1f0a9d42"
down_revision: str | Sequence[str] | None = "a5e954c5bab4"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

#: A retrieval's population, from the list its job was handed: the distinct
#: accessions, sorted by code point (COLLATE "C", whatever the database's locale),
#: one per line with its newline, hashed.
RETRIEVALS = """
UPDATE prediction_set AS ps
SET meta = ps.meta || jsonb_build_object(
    'query_population', jsonb_build_object('proteins', q.proteins, 'sha256', q.sha256)
)
FROM (
    SELECT s.id,
           count(*) AS proteins,
           encode(
               sha256(convert_to(
                   string_agg(a.accession, E'\\n' ORDER BY a.accession COLLATE "C") || E'\\n',
                   'UTF8'
               )),
               'hex'
           ) AS sha256
    FROM prediction_set AS s
    JOIN job AS j ON j.id = s.job_id
    CROSS JOIN LATERAL (
        SELECT DISTINCT jsonb_array_elements_text(j.payload->'query_accessions') AS accession
    ) AS a
    WHERE s.query_set_id IS NULL
      AND NOT s.meta ? 'query_population'
      AND NOT s.meta ? 'scored_from'
      AND j.operation = 'predict_go_terms'
      AND jsonb_typeof(j.payload->'query_accessions') = 'array'
    GROUP BY s.id
) AS q
WHERE ps.id = q.id
"""

#: A scored set was asked about what its candidates were asked about.
SCORED = """
UPDATE prediction_set AS s
SET meta = s.meta || jsonb_build_object('query_population', c.meta->'query_population')
FROM prediction_set AS c
WHERE c.id::text = s.meta->>'scored_from'
  AND c.meta ? 'query_population'
  AND s.query_set_id IS NULL
  AND NOT s.meta ? 'query_population'
"""

FORGET = (
    "UPDATE prediction_set SET meta = meta - 'query_population' WHERE meta ? 'query_population'"
)


def upgrade() -> None:
    op.execute(sa.text(RETRIEVALS))
    op.execute(sa.text(SCORED))


def downgrade() -> None:
    op.execute(sa.text(FORGET))
