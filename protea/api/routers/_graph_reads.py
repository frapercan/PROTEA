"""Every database read behind ``GET /v1/graph``, and nothing else.

Split out of the router so the whole read surface of that endpoint is visible in
one file and can be audited in one pass. Two properties are load-bearing and
both are checked by ``tests/test_graph_endpoint.py``:

* every statement here is a SELECT, so the endpoint cannot write;
* the statements are module constants, so a test can answer them by identity
  instead of matching on SQL text, and the router can be exercised with no
  database at all.

Cost. All of these are small. The widest touches thirteen embedding
configurations; the only one that reaches a large table groups candidate rows by
their prediction set, which the planner satisfies with a parallel scan in tens
of milliseconds. Nothing here is cached, because a surface whose job is to
report what the record holds right now must not answer from a copy of what it
held earlier.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import TextClause, text
from sqlalchemy.orm import Session

_Q_EVALUATION_SETS = text(
    """
    SELECT es.id::text                               AS id,
           es.window_role                            AS window_role,
           es.stats ->> 'mode'                       AS mode,
           es.stats ->> 'pivot_ontology_snapshot_id' AS pivot_snapshot_id,
           os.obo_version                            AS pivot_version,
           lo.source_version                         AS window_from,
           hi.source_version                         AS window_to,
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
           EXISTS (
               SELECT 1 FROM prediction_set ps WHERE ps.embedding_config_id = ec.id
           )                                        AS in_use,
           EXISTS (
               SELECT 1 FROM sequence_embedding se WHERE se.embedding_config_id = ec.id
           )                                        AS producible
    FROM embedding_config ec
    ORDER BY ec.model_name
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
           ps.limit_per_entry::text                AS depth,
           ps.distance_threshold::text             AS distance_threshold,
           ps.meta ->> 'metric'                    AS metric,
           ps.meta ->> 'search_backend'            AS search_backend,
           ps.meta ->> 'aspect_separated_knn'      AS aspect_separated,
           ps.meta ->> 'expand_votes_to_ancestors' AS expand_to_ancestors,
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
    """
    SELECT er.id::text                              AS result_id,
           sc.name                                  AS scoring_name,
           COALESCE(ec.display_name, ec.model_name) AS embedding_name,
           ps.limit_per_entry::text                 AS depth,
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
QUERIES: dict[str, TextClause] = {
    "evaluation_sets": _Q_EVALUATION_SETS,
    "accretion": _Q_ACCRETION,
    "query_sets": _Q_QUERY_SETS,
    "substrates": _Q_SUBSTRATES,
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
