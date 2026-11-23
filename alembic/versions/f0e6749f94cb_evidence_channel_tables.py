"""The evidence channel table: evidence_channel_table, evidence_channel, evidence_channel_empty_go_ref

ADR-D59, decision 2: the channel of a GOA row is a declared table, not a column.
Three tables keyed by what they depend on (ADR-D56): one row per version of the
table, named by the digest every photo records; its keys (evidence code, reference
class, reviewed) with their channel; and how it reads a ``GO_REF`` with no number.
Two SQL functions compute a row's reference class and channel from them, so any
reader can classify a stored row without Python. ``cut_photo`` gets a foreign key
to the table its photo was classified with.

The table the code declared when this revision was written is seeded here, its
rows restated from ``protea.core.cut_photo.channels`` (a test pins the two equal
while the code declares that table), so a photo loaded before this revision has a
table to point at. Its digest is recomputed from the restated references before a
row is written, as ``channel_table_digest`` computes it. A later version is
written by the first ``load_cut_photo`` that classifies with it.

Revision ID: f0e6749f94cb
Revises: d4a7c2e9f310
"""

from __future__ import annotations

import hashlib
import json
from datetime import date
from typing import Any

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID

from alembic import op

revision = "f0e6749f94cb"
down_revision = "d4a7c2e9f310"
branch_labels = None
depends_on = None

#: The same texts the ORM model declares; a test compares the two.
CHANNEL_CHECK = (
    "channel IN ('curated', 'no_data', 'orthology_electronic', 'phylogenetic', "
    "'reviewed_field_electronic', 'signature_electronic', 'truth')"
)
SHORT_NAME_CHECK = "short_name ~ '^[a-z0-9][a-z0-9-]*$' AND length(short_name) <= 64"
TABLE_CHECK = "length(sha256) = 64 AND length(description) > 0"
REFERENCE_CLASS_FUNCTION = r"""
CREATE OR REPLACE FUNCTION evidence_reference_class(
    p_table text, p_reference text, p_with_from text
) RETURNS text
LANGUAGE sql STABLE PARALLEL SAFE AS $$
    SELECT CASE
        WHEN p_reference ~ 'GO_REF:[0-9]{7}'
            THEN substring(p_reference FROM 'GO_REF:[0-9]{7}')
        WHEN p_reference ~ '(^|\|)(PMID|DOI):' THEN 'publication'
        WHEN p_reference ~ '(^|\|)GO_REF:(\||$)' THEN coalesce((
            SELECT min(m.reads_as)
            FROM unnest(string_to_array(p_with_from, '|')) AS item
            LEFT JOIN evidence_channel_empty_go_ref m
                ON m.table_sha256 = p_table AND m.with_from_prefix = split_part(item, ':', 1)
            WHERE item <> ''
            HAVING count(*) > 0
                AND count(*) = count(m.reads_as)
                AND count(DISTINCT m.reads_as) = 1
        ), 'empty_go_ref')
        ELSE 'other'
    END
$$
"""
CHANNEL_FUNCTION = r"""
CREATE OR REPLACE FUNCTION evidence_channel_of(
    p_table text, p_code text, p_reference text, p_with_from text, p_reviewed boolean
) RETURNS text
LANGUAGE sql STABLE PARALLEL SAFE AS $$
    SELECT coalesce(
        (SELECT c.channel FROM evidence_channel c
          WHERE c.table_sha256 = p_table AND c.evidence_code = p_code
            AND c.reference_class = '*' AND c.reviewed = p_reviewed),
        (SELECT c.channel FROM evidence_channel c
          WHERE c.table_sha256 = p_table AND c.evidence_code = p_code
            AND c.reference_class = evidence_reference_class(p_table, p_reference, p_with_from)
            AND c.reviewed = p_reviewed)
    )
$$
"""

#: The table this revision seeds, as protea.core.cut_photo declared it on 2026-11-16.
SEED_SHA256 = "781a0ac1a02a8156a4b873704728529751acf61b98cff45d71c22d6d370155bc"
SEED_GOREF_LIST_READ_ON = date(2026, 11, 16)
SEED_SHORT_NAME = "evidence-channels-2026-11-16-781a0ac1"
SEED_DESCRIPTION = (
    "The evidence channel of a GOA row, checked against GO's GO_REF list read on "
    "2026-11-16: 13 truth codes, 7 curated, 4 phylogenetic and ND as no data; 18 "
    "electronic references (2 orthology, 7 field translations, 9 signatures); an "
    "empty GO_REF read through its with/from (UniProtKB-KW as GO_REF:0000043, "
    "UniProtKB-SubCell as GO_REF:0000044). 86 rows."
)
TRUTH_CODES = (
    "EXP",
    "HDA",
    "HEP",
    "HGI",
    "HMP",
    "HTP",
    "IC",
    "IDA",
    "IEP",
    "IGI",
    "IMP",
    "IPI",
    "TAS",
)
CURATED_CODES = ("IGC", "ISA", "ISM", "ISO", "ISS", "NAS", "RCA")
PHYLOGENETIC_CODES = ("IBA", "IBD", "IKR", "IRD")
NO_DATA_CODE = "ND"
AUTOMATIC_CODE = "IEA"
ANY_REFERENCE = "*"
#: Every ``GO_REF`` an ``IEA`` row may cite: its kind and its title in GO's list.
ELECTRONIC_REFERENCES: dict[str, tuple[str, str]] = {
    "GO_REF:0000002": (
        "signature",
        ("Gene Ontology annotation through association of InterPro records with GO terms"),
    ),
    "GO_REF:0000003": (
        "field",
        "Gene Ontology annotation based on Enzyme Commission mapping",
    ),
    "GO_REF:0000019": (
        "orthology",
        (
            "OBSOLETE Automatic transfer of experimentally verified manual GO "
            "annotation data to orthologs using Ensembl Compara"
        ),
    ),
    "GO_REF:0000020": (
        "signature",
        (
            "OBSOLETE Electronic Gene Ontology annotations created by transferring "
            "manual GO annotations between orthologous microbial proteins"
        ),
    ),
    "GO_REF:0000037": (
        "field",
        (
            "OBSOLETE Gene Ontology annotation based on manual assignment of "
            "UniProtKB keywords in UniProtKB/Swiss-Prot entries"
        ),
    ),
    "GO_REF:0000038": (
        "signature",
        (
            "OBSOLETE Gene Ontology annotation based on automatic assignment of "
            "UniProtKB keywords in UniProtKB/TrEMBL entries"
        ),
    ),
    "GO_REF:0000039": (
        "field",
        (
            "OBSOLETE Gene Ontology annotation based on the manual assignment of "
            "UniProtKB Subcellular Location terms in UniProtKB/Swiss-Prot entries"
        ),
    ),
    "GO_REF:0000040": (
        "signature",
        (
            "OBSOLETE Gene Ontology annotation based on the automatic assignment of "
            "UniProtKB Subcellular Location terms in UniProtKB/TrEMBL entries"
        ),
    ),
    "GO_REF:0000041": (
        "field",
        "Gene Ontology annotation based on UniPathway vocabulary mapping",
    ),
    "GO_REF:0000043": (
        "field",
        "Gene Ontology annotation based on UniProtKB/Swiss-Prot keyword mapping",
    ),
    "GO_REF:0000044": (
        "field",
        (
            "Gene Ontology annotation based on UniProtKB/Swiss-Prot Subcellular "
            "Location vocabulary mapping, accompanied by conservative changes to GO "
            "terms applied by UniProt"
        ),
    ),
    "GO_REF:0000104": (
        "signature",
        (
            "Electronic Gene Ontology annotations created by transferring manual GO "
            "annotations between related proteins based on shared sequence features"
        ),
    ),
    "GO_REF:0000107": (
        "orthology",
        (
            "Automatic transfer of experimentally verified manual GO annotation data "
            "to orthologs using Ensembl Compara"
        ),
    ),
    "GO_REF:0000108": (
        "signature",
        (
            "Automatic assignment of GO terms using logical inference, based on on "
            "inter-ontology links"
        ),
    ),
    "GO_REF:0000116": (
        "field",
        "Automatic Gene Ontology annotation based on Rhea mapping",
    ),
    "GO_REF:0000117": (
        "signature",
        ("Electronic Gene Ontology annotations created by ARBA machine learning models"),
    ),
    "GO_REF:0000118": (
        "signature",
        "TreeGrafter-generated GO annotations",
    ),
    "GO_REF:0000120": (
        "signature",
        "Combined Automated Annotation using Multiple IEA Methods",
    ),
}
EMPTY_GO_REF_BY_WITH_FROM = {
    "UniProtKB-KW": "GO_REF:0000043",
    "UniProtKB-SubCell": "GO_REF:0000044",
}
#: The channel of an electronic reference's kind, unreviewed and reviewed.
_ELECTRONIC_CHANNEL = {
    "orthology": ("orthology_electronic", "orthology_electronic"),
    "field": ("signature_electronic", "reviewed_field_electronic"),
    "signature": ("signature_electronic", "signature_electronic"),
}


def seed_digest() -> str:
    """The digest of the restated table, computed as ``channel_table_digest`` does."""
    declared = {
        "references": sorted(ELECTRONIC_REFERENCES.items()),
        "empty_go_ref_by_with_from": sorted(EMPTY_GO_REF_BY_WITH_FROM.items()),
    }
    return hashlib.sha256(json.dumps(declared, sort_keys=True).encode()).hexdigest()


def seed_rows() -> list[dict[str, Any]]:
    """The rows of the seeded table, in the order ``channels.channel_rows`` lists them."""
    by_code = {code: "truth" for code in TRUTH_CODES}
    by_code |= {code: "curated" for code in CURATED_CODES}
    by_code |= {code: "phylogenetic" for code in PHYLOGENETIC_CODES}
    by_code[NO_DATA_CODE] = "no_data"
    rows = [
        (code, ANY_REFERENCE, reviewed, channel, None)
        for code, channel in sorted(by_code.items())
        for reviewed in (False, True)
    ]
    rows += [
        (AUTOMATIC_CODE, reference, reviewed, _ELECTRONIC_CHANNEL[kind][reviewed], title)
        for reference, (kind, title) in sorted(ELECTRONIC_REFERENCES.items())
        for reviewed in (False, True)
    ]
    keys = ("evidence_code", "reference_class", "reviewed", "channel", "reference_title")
    return [{"table_sha256": SEED_SHA256, **dict(zip(keys, row, strict=True))} for row in rows]


def upgrade() -> None:
    if seed_digest() != SEED_SHA256:
        raise RuntimeError(
            f"the restated channel table hashes to {seed_digest()}, not {SEED_SHA256}"
        )
    version = op.create_table(
        "evidence_channel_table",
        sa.Column("sha256", sa.String(64), primary_key=True),
        sa.Column("goref_list_read_on", sa.Date(), nullable=False),
        sa.Column("short_name", sa.String(64), nullable=False, unique=True),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("code_revision", sa.String(64), nullable=True),
        sa.Column(
            "job_id",
            UUID(as_uuid=True),
            sa.ForeignKey("job.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.CheckConstraint(SHORT_NAME_CHECK, name="ck_evidence_channel_table_short_name"),
        sa.CheckConstraint(TABLE_CHECK, name="ck_evidence_channel_table_identity"),
    )
    channels = op.create_table(
        "evidence_channel",
        sa.Column(
            "table_sha256",
            sa.String(64),
            sa.ForeignKey("evidence_channel_table.sha256", ondelete="RESTRICT"),
            primary_key=True,
        ),
        sa.Column("evidence_code", sa.String(8), primary_key=True),
        sa.Column("reference_class", sa.String(32), primary_key=True),
        sa.Column("reviewed", sa.Boolean(), primary_key=True),
        sa.Column("channel", sa.String(32), nullable=False),
        sa.Column("reference_title", sa.Text(), nullable=True),
        sa.CheckConstraint(CHANNEL_CHECK, name="ck_evidence_channel_channel"),
    )
    empty = op.create_table(
        "evidence_channel_empty_go_ref",
        sa.Column(
            "table_sha256",
            sa.String(64),
            sa.ForeignKey("evidence_channel_table.sha256", ondelete="RESTRICT"),
            primary_key=True,
        ),
        sa.Column("with_from_prefix", sa.String(64), primary_key=True),
        sa.Column("reads_as", sa.String(32), nullable=False),
    )
    op.execute(REFERENCE_CLASS_FUNCTION)
    op.execute(CHANNEL_FUNCTION)
    op.bulk_insert(
        version,
        [
            {
                "sha256": SEED_SHA256,
                "goref_list_read_on": SEED_GOREF_LIST_READ_ON,
                "short_name": SEED_SHORT_NAME,
                "description": SEED_DESCRIPTION,
                "code_revision": None,
                "job_id": None,
            }
        ],
    )
    op.bulk_insert(channels, seed_rows())
    op.bulk_insert(
        empty,
        [
            {"table_sha256": SEED_SHA256, "with_from_prefix": prefix, "reads_as": reads_as}
            for prefix, reads_as in sorted(EMPTY_GO_REF_BY_WITH_FROM.items())
        ],
    )
    op.create_foreign_key(
        "fk_cut_photo_channel_table",
        "cut_photo",
        "evidence_channel_table",
        ["channel_table_sha256"],
        ["sha256"],
        ondelete="RESTRICT",
    )


def downgrade() -> None:
    op.drop_constraint("fk_cut_photo_channel_table", "cut_photo", type_="foreignkey")
    op.execute("DROP FUNCTION IF EXISTS evidence_channel_of(text, text, text, text, boolean)")
    op.execute("DROP FUNCTION IF EXISTS evidence_reference_class(text, text, text)")
    op.drop_table("evidence_channel_empty_go_ref")
    op.drop_table("evidence_channel")
    op.drop_table("evidence_channel_table")
