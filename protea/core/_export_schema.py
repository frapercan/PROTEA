"""Single explicit canonical pyarrow schema for the export parquet shards.

The KNN-transfer export writer (``_KnnTransferRunner._flush``) used to let
``pyarrow`` INFER the parquet schema from the FIRST batch it wrote, then
reject every later batch whose inferred schema differed. That inference is
composition-sensitive in two independent ways, and both have bitten the
export in production:

* column ORDER: a batch built from a different record-builder path could
  emit the canonical columns in a different dict-insertion order (fixed at
  the builder level in #650).
* column TYPE: a numeric field that is an int in an all-KNN batch
  (``k_position`` 3) but ``NaN`` in a batch that also contains
  classifier-only / interpro-only rows (which set it missing) is inferred
  ``int64`` for the first and ``float64`` for the second, so the writer
  raises ``Table schema does not match schema used to create file``.

With per-chunk batching (#651) any single chunk can be all-KNN or contain
non-KNN NaNs, so the inferred type is not stable across flushes.

The robust fix is to stop relying on per-batch inference entirely: define
ONE explicit schema here, construct the ``ParquetWriter`` with it, and
conform (cast) every batch to it before ``write_table``. That permanently
eliminates BOTH the order drift and the type drift.

Type policy:

* identity / categorical columns are ``string`` (``pa.string()``).
* ``label`` is the only integral column that is ALWAYS present and never
  ``NaN`` (0/1), so it stays ``int64``.
* ``bool`` presence/flag columns (``knn_present``, ``interpro_*``) stay
  ``bool``.
* every remaining numeric feature is ``float64``. Any numeric feature that
  can be ``NaN`` / absent for a non-KNN record (classifier-only,
  interpro-only, ancestor-expanded) MUST be ``float64`` because ``NaN`` is
  not representable in ``int64``. This covers the int-valued KNN fields
  ``k_position`` / ``go_term_frequency`` / ``ref_annotation_density``
  (the ones in the live error) plus ``length_query`` / ``length_ref`` /
  ``taxonomic_distance`` / ``taxonomic_common_ancestors`` (``None`` for a
  non-KNN row). ``vote_count`` and ``interpro_n_signatures`` are also kept
  ``float64`` for a uniform numeric layout: they are read as floats by the
  LightGBM booster anyway, casting ints to ``float64`` is value-preserving
  (``3`` stays ``3.0``), and a single numeric dtype removes the last source
  of int-vs-float drift.

The column ORDER below mirrors ``_LeafRecordBuilder.make_leaf_record`` (the
canonical emit order from #650) exactly so nothing regresses.
"""

from __future__ import annotations

import pyarrow as pa

from protea.core.reranker import EMBEDDING_PCA_DIM

# Identity / label columns carried alongside the ``ALL_FEATURES`` set. These
# are not feature columns but are written into every export record.
_STRING_COLUMNS: frozenset[str] = frozenset(
    {
        "protein_accession",
        "go_id",
        "aspect",
        "ref_protein_accession",
        "qualifier",
        "evidence_code",
        "taxonomic_relation",
    }
)

# The only integral, always-present, never-NaN column.
_INT_COLUMNS: frozenset[str] = frozenset({"label"})

# Boolean presence / flag columns.
_BOOL_COLUMNS: frozenset[str] = frozenset(
    {
        "interpro_hit",
        "interpro_db_pfam",
        "interpro_db_panther",
        "interpro_db_superfamily",
        "interpro_db_smart",
        "interpro_db_cdd",
        "interpro_db_prosite",
        "knn_present",
        "interpro_present",
    }
)

# Canonical column ORDER, byte-identical to the dict-insertion order of
# ``_LeafRecordBuilder.make_leaf_record`` (and its non-KNN twins). Keeping
# this list in lockstep with the builder is what makes the cast a no-op on
# a well-formed record and preserves the #650 ordering fix.
CANONICAL_COLUMN_ORDER: tuple[str, ...] = (
    # identity / label
    "protein_accession",
    "go_id",
    "aspect",
    "label",
    "distance",
    "ref_protein_accession",
    "qualifier",
    "evidence_code",
    # alignment (NW + SW + lengths)
    "identity_nw",
    "similarity_nw",
    "alignment_score_nw",
    "gaps_pct_nw",
    "alignment_length_nw",
    "identity_sw",
    "similarity_sw",
    "alignment_score_sw",
    "gaps_pct_sw",
    "alignment_length_sw",
    "length_query",
    "length_ref",
    # taxonomy
    "taxonomic_distance",
    "taxonomic_common_ancestors",
    "taxonomic_relation",
    # reranker
    "vote_count",
    "k_position",
    "go_term_frequency",
    "ref_annotation_density",
    "neighbor_distance_std",
    "neighbor_vote_fraction",
    "neighbor_min_distance",
    "neighbor_mean_distance",
    # anc2vec
    "anc2vec_neighbor_cos",
    "anc2vec_neighbor_maxcos",
    "anc2vec_has_emb",
    "anc2vec_query_known_cos",
    "anc2vec_query_known_maxcos",
    "anc2vec_query_known_count",
    # taxonomy consensus
    "tax_voters_same_frac",
    "tax_voters_close_frac",
    "tax_voters_mean_common_ancestors",
    # embedding PCA projection
    *(f"emb_pca_query_{i}" for i in range(EMBEDDING_PCA_DIM)),
    # lineage
    "lineage_is_ancestor_of_known",
    "lineage_is_descendant_of_known",
    "lineage_ancestor_of_count",
    "lineage_descendant_of_count",
    # interpro
    "interpro_hit",
    "interpro_score",
    "interpro_n_signatures",
    "interpro_db_pfam",
    "interpro_db_panther",
    "interpro_db_superfamily",
    "interpro_db_smart",
    "interpro_db_cdd",
    "interpro_db_prosite",
    "knn_present",
    "interpro_present",
    # lafa (classifier / self-prior / association)
    "classifier_score",
    "classifier_present",
    "self_prior_score",
    "association_total",
    "association_cross",
    "association_present",
)


def _pa_type_for(column: str) -> pa.DataType:
    """Explicit pyarrow type for one canonical export column."""
    if column in _STRING_COLUMNS:
        return pa.string()
    if column in _BOOL_COLUMNS:
        return pa.bool_()
    if column in _INT_COLUMNS:
        return pa.int64()
    # Every other canonical column is a numeric feature: float64 so a NaN
    # for a non-KNN record is representable and no int/float drift can occur.
    return pa.float64()


def _build_canonical_schema() -> pa.Schema:
    return pa.schema(
        [pa.field(name, _pa_type_for(name)) for name in CANONICAL_COLUMN_ORDER]
    )


# The single canonical schema every export batch is written with.
CANONICAL_EXPORT_SCHEMA: pa.Schema = _build_canonical_schema()


def export_table_from_records(records: list[dict[str, object]]) -> pa.Table:
    """Build a pyarrow ``Table`` from export records under the fixed schema.

    Every batch is materialised with :data:`CANONICAL_EXPORT_SCHEMA`, so the
    resulting table has a stable column set, order, and dtype regardless of
    the batch's KNN / non-KNN composition. ``pa.Table.from_pylist`` with an
    explicit schema casts int-valued fields (``k_position`` 3) to
    ``float64`` (``3.0``, value-preserving) and represents missing non-KNN
    fields as ``NaN`` / null, ending the recurring batch type/order drift.
    """
    return pa.Table.from_pylist(records, schema=CANONICAL_EXPORT_SCHEMA)


__all__ = [
    "CANONICAL_COLUMN_ORDER",
    "CANONICAL_EXPORT_SCHEMA",
    "export_table_from_records",
]
