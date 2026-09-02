"""Every database read behind ``GET /v1/graph``, and nothing else.

Split out of the router so the whole read surface of that endpoint is visible in
one file and can be audited in one pass. Two properties are load-bearing and
both are checked by ``tests/test_graph_endpoint.py``:

* every statement here is a SELECT, so the endpoint cannot write;
* the statements are module constants, so a test can answer them by identity
  instead of matching on SQL text, and the router can be exercised with no
  database at all.

Cost. All of these are small. Two reach a large table: the candidate rows
grouped by prediction set, and the stored-embedding rows grouped by embedding
configuration. The planner satisfies both with a parallel index-only scan in a
few hundred milliseconds. Nothing here is cached, because a surface whose job is
to report what the record holds right now must not answer from a copy of what it
held earlier.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import TextClause, text
from sqlalchemy.orm import Session

from protea.api.routers._arm_identity import with_arm_identity

_Q_EVALUATION_SETS = text(
    """
    SELECT es.id::text                               AS id,
           es.window_role                            AS window_role,
           es.stats ->> 'mode'                       AS mode,
           es.stats ->> 'pivot_ontology_snapshot_id' AS pivot_snapshot_id,
           os.obo_version                            AS pivot_version,
           lo.source_version                         AS window_from,
           hi.source_version                         AS window_to,
           -- A release number names a file and dates nothing. 220 and 227 are
           -- fourteen months apart, and a reader who cannot see that cannot
           -- judge how much annotation the window had room to accumulate,
           -- which is the first thing anyone asks of a temporal benchmark.
           lo.source_published_at::date::text        AS window_from_date,
           hi.source_published_at::date::text        AS window_to_date,
           es.old_annotation_set_id::text            AS bank_annotation_set_id,
           es.groundtruth_uri                        AS groundtruth_uri
    FROM evaluation_set es
    LEFT JOIN annotation_set lo ON lo.id = es.old_annotation_set_id
    LEFT JOIN annotation_set hi ON hi.id = es.new_annotation_set_id
    LEFT JOIN ontology_snapshot os
           ON os.id::text = es.stats ->> 'pivot_ontology_snapshot_id'
    ORDER BY es.created_at DESC
    """
)

#: Accretion tables, with the two facts that settle the frame's edge: whether a
#: producing job named this one, and whether it is eligible at all, which is a
#: question about the pivot snapshot it was computed over. A table built on a
#: different snapshot is not a rejected alternative, it is not an alternative,
#: because it weights terms the frame does not contain.
#:
#: ``in_use`` reaches through the job because no column on ``evaluation_result``
#: records the weighting. The producing job's payload names it, and that payload
#: is the only surviving statement of which accretion table a published number
#: was weighted by.
_Q_ACCRETION = text(
    """
    SELECT ia.id::text                   AS id,
           ia.ontology_snapshot_id::text AS ontology_snapshot_id,
           ia.evidence_regime            AS regime,
           ia.content_sha256             AS sha256,
           EXISTS (
               SELECT 1
               FROM evaluation_result er
               JOIN job j ON j.id = er.job_id
               WHERE j.payload ->> 'information_accretion_set_id' = ia.id::text
           )                             AS in_use
    FROM information_accretion_set ia
    ORDER BY ia.created_at
    """
)

_Q_QUERY_SETS = text(
    """
    SELECT qs.id::text AS id,
           qs.name     AS name,
           (SELECT count(*) FROM query_set_entry e WHERE e.query_set_id = qs.id) AS entries,
           EXISTS (SELECT 1 FROM prediction_set ps WHERE ps.query_set_id = qs.id) AS in_use
    FROM query_set qs
    ORDER BY qs.created_at
    """
)

#: Every registered representation with the two facts a level needs: whether a
#: prediction set instantiated it, and whether it could be instantiated at all.
#: A configuration with no stored embedding is not an untried alternative, it is
#: an unbuilt one, and counting it as available would overstate what was passed
#: over.
#:
#: ``stored`` is what settles that second question with a number rather than a
#: boolean. It is a row count, and a row is a sequence only while chunking is
#: off, so ``use_chunking`` travels beside it and the caller refuses to turn a
#: chunked count into a coverage ratio.
#:
#: ``param_count`` is nullable and null for several of these. It is carried
#: through as null rather than filled in, and nothing here orders by it: a
#: column that is missing for half the rows cannot rank them.
#:
#: ``trained_on_annotation_set_id`` splits the list in two. An encoding fitted
#: against an annotation release and a pretrained backbone used as it ships are
#: not two settings of one knob, so the release it was fitted against is read
#: here rather than left as an opaque id.
_Q_SUBSTRATES = text(
    """
    SELECT ec.id::text                              AS id,
           ec.model_name                            AS model_name,
           ec.model_backend                         AS model_backend,
           ec.layer_indices::text                   AS layer_indices,
           ec.layer_agg                             AS layer_agg,
           ec.pooling                               AS pooling,
           ec.normalize::text                       AS normalize,
           ec.normalize_residues::text              AS normalize_residues,
           ec.max_length::text                      AS max_length,
           ec.use_chunking::text                    AS use_chunking,
           ec.chunk_size::text                      AS chunk_size,
           ec.chunk_overlap::text                   AS chunk_overlap,
           COALESCE(ec.display_name, ec.model_name) AS label,
           ec.display_name                          AS display_name,
           ec.family                                AS family,
           ec.param_count                           AS param_count,
           ec.trained_on_annotation_set_id::text    AS trained_on_id,
           tr.source                                AS trained_on_source,
           tr.source_version                        AS trained_on_version,
           tr.source_published_at::date::text       AS trained_on_date,
           COALESCE(cov.stored, 0)                  AS stored,
           EXISTS (
               SELECT 1 FROM prediction_set ps WHERE ps.embedding_config_id = ec.id
           )                                        AS in_use,
           EXISTS (
               SELECT 1 FROM sequence_embedding se WHERE se.embedding_config_id = ec.id
           )                                        AS producible
    FROM embedding_config ec
    LEFT JOIN annotation_set tr ON tr.id = ec.trained_on_annotation_set_id
    -- Grouped once and joined, not one correlated count per configuration.
    -- The grouping is an index-only scan over ix_sequence_embedding_
    -- embedding_config_id; thirteen correlated counts would each walk the
    -- heap instead.
    LEFT JOIN (
        SELECT embedding_config_id, count(*) AS stored
        FROM sequence_embedding
        GROUP BY embedding_config_id
    ) cov ON cov.embedding_config_id = ec.id
    ORDER BY ec.model_name
    """
)

#: The corpus a representation's coverage is a fraction OF. Sequences and not
#: proteins: embeddings are stored per distinct amino-acid sequence, so proteins
#: sharing a sequence share one row and a protein count would put the ratio
#: under one against a fully embedded corpus.
_Q_CORPUS = text(
    """
    SELECT count(*) AS sequences FROM sequence
    """
)

#: The retriever and the bank both live on the prediction set, one row per run.
#: The donor policy is split into its three fields rather than fetched whole,
#: because the question asked of it is whether anybody ever set it, and that is
#: a question about each field on its own.
_Q_PREDICTION_SETS = text(
    """
    SELECT ps.id::text                             AS id,
           ps.embedding_config_id::text            AS embedding_config_id,
           ps.annotation_set_id::text              AS annotation_set_id,
           a.source                                AS bank_source,
           a.source_version                        AS bank_version,
           ps.query_set_id::text                   AS query_set_id,
           -- A prediction set has no scored depth: it is the RETRIEVAL
           -- depth here, and it is correct. One set can be evaluated at many
           -- depths, and each of those results carries its own cut. The
           -- surfaces that read a RESULT use DEPTH_IDENTITY_COLUMN, which
           -- says which of the three quantities it rendered. This one is not
           -- prefixed: every row of this read is a retrieval depth, so there
           -- is nothing here to tell apart, and the retriever node prints
           -- these values in a sentence that already names them.
           ps.limit_per_entry::text                AS depth,
           ps.distance_threshold::text             AS distance_threshold,
           ps.meta ->> 'metric'                    AS metric,
           ps.meta ->> 'search_backend'            AS search_backend,
           ps.meta ->> 'aspect_separated_knn'      AS aspect_separated,
           ps.meta ->> 'expand_votes_to_ancestors' AS expand_to_ancestors,
           -- Null on every set written before the flag existed, which is not the
           -- same as false and must not be coalesced into it: null says the run
           -- predates the question, false says the run was asked and allowed it.
           ps.meta ->> 'exclude_self_neighbour'     AS exclude_self_neighbour,
           ps.meta -> 'donor_policy' ->> 'reviewed_only'  AS donor_reviewed_only,
           ps.meta -> 'donor_policy' ->> 'evidence_codes' AS donor_evidence_codes,
           ps.meta -> 'donor_policy' ->> 'exclude_reference_prefixes' AS donor_exclusions,
           (SELECT string_agg(f.value, ', ' ORDER BY f.value)
            FROM jsonb_array_elements_text(
                     CASE WHEN jsonb_typeof(ps.meta -> 'features') = 'array'
                          THEN ps.meta -> 'features' ELSE '[]'::jsonb END
                 ) AS f(value))                    AS features
    FROM prediction_set ps
    LEFT JOIN annotation_set a ON a.id = ps.annotation_set_id
    ORDER BY ps.created_at
    """
)

_Q_BANKS = text(
    """
    SELECT a.id::text       AS id,
           a.source         AS source,
           a.source_version AS source_version,
           EXISTS (
               SELECT 1 FROM prediction_set ps WHERE ps.annotation_set_id = a.id
           )                AS in_use
    FROM annotation_set a
    ORDER BY a.source_version
    """
)

_Q_SCORING = text(
    """
    SELECT sc.id::text               AS id,
           sc.name                   AS name,
           sc.formula                AS formula,
           sc.weights::text          AS weights,
           sc.evidence_weights::text AS evidence_weights,
           sc.params::text           AS params,
           (SELECT count(*) FROM evaluation_result er WHERE er.scoring_config_id = sc.id)
                                     AS results
    FROM scoring_config sc
    ORDER BY sc.name
    """
)

#: One row per published result, carrying the identity of every node it fixes.
#: This is what makes a node's result count a count of surviving evidence rather
#: than of dispatched work. The job table is not consulted for counting anywhere
#: in this endpoint, because it outlived the results once already.
_Q_RESULTS = text(
    """
    SELECT er.id::text                  AS id,
           er.evaluation_set_id::text   AS evaluation_set_id,
           er.prediction_set_id::text   AS prediction_set_id,
           er.scoring_config_id::text   AS scoring_config_id,
           er.reranker_model_id::text   AS reranker_model_id,
           -- The seal, not the harness label. ``frame`` is varchar(8) under a
           -- check constraint admitting only lafa or internal, so it says which
           -- harness produced a number and nothing about the parameters that
           -- decide whether two numbers can be read side by side. A result is
           -- attributable when it carries a digest of those parameters.
           er.frame_digest              AS frame,
           ps.embedding_config_id::text AS embedding_config_id,
           ps.annotation_set_id::text   AS annotation_set_id
    FROM evaluation_result er
    LEFT JOIN prediction_set ps ON ps.id = er.prediction_set_id
    ORDER BY er.created_at
    """
)

#: The results blob unnested to one row per (result, panel). Both lateral joins
#: are guarded with a type test, because the blob also carries an ``artifacts``
#: key whose value is an object of lists, and an unguarded ``jsonb_each`` over a
#: non-object raises rather than skipping.
_Q_PANELS = text(
    with_arm_identity(
    """
    SELECT er.id::text                              AS result_id,
           sc.name                                  AS scoring_name,
           COALESCE(ec.display_name, ec.model_name) AS embedding_name,
{DEPTH_IDENTITY_COLUMN},
           -- The donor policy is a level of the Bank node, so two arms that
           -- differ only in it are two levels. Leaving it out of the fields a
           -- level is named by rendered both under one name, which made the
           -- head of a panel ambiguous and folded the bank effect into what
           -- reads as scoring spread.
{ARM_IDENTITY_COLUMNS},
           ps.meta ->> 'metric'                     AS metric,
           cat.k                                    AS category,
           asp.k                                    AS aspect,
           (asp.v ->> 'f_micro_w')::float8          AS f_micro_w,
           (asp.v ->> 'tau')::float8                AS tau,
           (asp.v ->> 'n_proteins')::int            AS n_at_tau,
           (asp.v ->> 'coverage_at_tau')::float8    AS coverage_at_tau
    FROM evaluation_result er
    LEFT JOIN scoring_config sc ON sc.id = er.scoring_config_id
    LEFT JOIN prediction_set ps ON ps.id = er.prediction_set_id
    LEFT JOIN embedding_config ec ON ec.id = ps.embedding_config_id
    CROSS JOIN LATERAL jsonb_each(
        CASE WHEN jsonb_typeof(er.results) = 'object' THEN er.results ELSE '{}'::jsonb END
    ) AS cat(k, v)
    CROSS JOIN LATERAL jsonb_each(
        CASE WHEN jsonb_typeof(cat.v) = 'object' THEN cat.v ELSE '{}'::jsonb END
    ) AS asp(k, v)
    WHERE asp.v ? 'f_micro_w'
    ORDER BY er.created_at
    """
    )
)

_Q_CANDIDATES = text(
    """
    SELECT prediction_set_id::text AS prediction_set_id,
           count(*)                AS candidates
    FROM go_prediction
    GROUP BY prediction_set_id
    """
)

#: Whether the candidate table can hold a term that arrived without a donor. A
#: NOT NULL column is a stronger statement than any row count: it says the shape
#: of the record forbids the other mechanism, not merely that nobody ran it.
#: Read from the catalog so it tracks the migration rather than a comment.
_Q_DONOR_COLUMN = text(
    """
    SELECT is_nullable AS is_nullable
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'go_prediction'
      AND column_name = 'ref_protein_accession'
    """
)

_Q_ARTIFACTS = text(
    """
    SELECT (SELECT count(*) FROM reranker_model)      AS reranker_model,
           (SELECT count(*) FROM interpro_annotation) AS interpro_annotation,
           (SELECT count(*) FROM interpro_go_mapping) AS interpro_go_mapping,
           (SELECT count(*) FROM evaluation_result WHERE reranker_model_id IS NOT NULL)
                                                      AS reranked_results
    """
)

#: The only place in this schema where a comparison can name its floor. An
#: experiment run already carries a declared intent, so a floor rides on its
#: config rather than on a column invented for the purpose. A run declares one
#: with ``config`` holding ``graph_node`` (the node key the comparison is about)
#: and ``floor`` (the level it is measured against).
_Q_FLOORS = text(
    """
    SELECT er.config ->> 'graph_node' AS node,
           er.config ->> 'floor'      AS floor,
           er.name                    AS name
    FROM experiment_run er
    WHERE er.config ? 'graph_node' AND er.config ? 'floor'
    """
)

#: The read surface, by name. Iterating this is the only way the router talks to
#: the database.

#: Everything the record can place on a date, annotation releases and ontology
#: releases alike. They are read together because the frame is built from both
#: and a reader judging a window needs to see them on one axis: which releases
#: fall inside it, which ontology it is scored under, and where that ontology
#: sits relative to the ends it is reconciling.
_TIMELINE = text("""
    SELECT 'annotation_set'                       AS kind,
           'GOA ' || a.source_version             AS label,
           a.source_version                       AS version,
           a.source_published_at::date::text      AS date,
           a.id::text                             AS id
    FROM annotation_set a
    WHERE a.source_published_at IS NOT NULL
    UNION ALL
    SELECT 'ontology_snapshot',
           o.obo_version,
           o.obo_version,
           NULLIF(REPLACE(o.obo_version, 'releases/', ''), '')::date::text,
           o.id::text
    FROM ontology_snapshot o
    WHERE o.obo_version LIKE 'releases/%'
    ORDER BY 4
""")

QUERIES: dict[str, TextClause] = {
    "timeline": _TIMELINE,
    "evaluation_sets": _Q_EVALUATION_SETS,
    "accretion": _Q_ACCRETION,
    "query_sets": _Q_QUERY_SETS,
    "substrates": _Q_SUBSTRATES,
    "corpus": _Q_CORPUS,
    "prediction_sets": _Q_PREDICTION_SETS,
    "banks": _Q_BANKS,
    "scoring": _Q_SCORING,
    "results": _Q_RESULTS,
    "panels": _Q_PANELS,
    "candidates": _Q_CANDIDATES,
    "donor_column": _Q_DONOR_COLUMN,
    "artifacts": _Q_ARTIFACTS,
    "floors": _Q_FLOORS,
}


#: The parameterised reads. They cannot sit in ``QUERIES`` because that dict is
#: run without arguments, but leaving them unregistered made the read surface
#: wider than the module declared it to be, and a test double built from
#: ``QUERIES`` alone refused a statement the endpoint legitimately issues.
PARAM_QUERIES: dict[str, TextClause] = {}  # populated below, after the statements exist


def read_record(session: Session) -> dict[str, list[dict[str, Any]]]:
    """Run every read once and return the rows by name.

    Separated from the assembly so the assembly is a pure function of rows and
    can be exercised without a database.
    """
    return {
        name: [dict(row) for row in session.execute(clause).mappings().all()]
        for name, clause in QUERIES.items()
    }


#: The aspect of every term of one ontology snapshot. Keyed by the GO accession
#: and not by the row id, because the accession is what the ground truth stores
#: and the row id is scoped to a snapshot, so joining on it across releases
#: matches nothing while looking like it matched.
_PIVOT_ASPECTS = text("""
    SELECT go_id, aspect
    FROM go_term
    WHERE ontology_snapshot_id = :snapshot_id
      AND aspect IS NOT NULL
""")


def read_pivot_aspects(session: Session, record: dict[str, list[dict[str, Any]]]) -> dict[str, str]:
    """Map each GO accession to its aspect under the window's own pivot.

    The pivot is the graph the ground truth is expressed in, so it is the only
    snapshot that can say which panel a gained term belongs to. Returns an empty
    mapping when no window is recorded, which the caller reads as a population
    it cannot count rather than one that is zero.
    """
    head = record["evaluation_sets"][0] if record["evaluation_sets"] else None
    snapshot_id = (head or {}).get("pivot_snapshot_id")
    if not snapshot_id:
        return {}
    rows = session.execute(_PIVOT_ASPECTS, {"snapshot_id": snapshot_id}).mappings().all()
    return {str(r["go_id"]): str(r["aspect"]) for r in rows}


def read_timeline(session: Session) -> list[dict[str, Any]]:
    """Every dated release the record holds, annotation and ontology together.

    Rows without a usable date are dropped rather than stacked at one end. A
    release whose publication date was never fetched is not a release that
    happened at an unknown edge of the axis, and placing it there invents a
    position the record does not support.
    """
    return [dict(r) for r in session.execute(_TIMELINE).mappings().all()]


PARAM_QUERIES["pivot_aspects"] = _PIVOT_ASPECTS
