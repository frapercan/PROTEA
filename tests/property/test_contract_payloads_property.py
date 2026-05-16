"""Property tests for ``protea_contracts`` payload models.

Every payload class declared in ``protea_contracts`` is frozen +
strict; the round-trip we exercise here is::

    payload == Payload.model_validate(payload.model_dump(mode="json"))

That equality is the contract every job dispatcher relies on: the
payload that left the API must be reconstructible verbatim by the
worker after JSON travels through RabbitMQ. A drift here (a field that
silently coerces, a default that mutates, a list ordering that
re-sorts) is a cross-repo break that no example-based test would
catch reliably because the failure depends on the specific value.

Strategies are hand-rolled per payload so we stay independent of
hypothesis-jsonschema (extra dependency for one feature). Each strategy
generates values inside each field's declared bounds, including
``None`` for optional fields and the explicit positive-integer
constraint on ``limit_per_entry`` / ``batch_size``.
"""

from __future__ import annotations

import json
import math

import pytest
from hypothesis import given
from hypothesis import strategies as st
from protea_contracts import (
    EcoMappingPayload,
    GoaAnnotationRecord,
    GoaStreamPayload,
    PredictGOTermsBatchPayload,
    PredictGOTermsPayload,
    QuickGoAnnotationRecord,
    QuickGoStreamPayload,
    RerankerSpec,
    StorePredictionsPayload,
    UniProtFastaStreamPayload,
    UniProtMetadataRecord,
    UniProtMetadataStreamPayload,
    UniProtProteinRecord,
)
from pydantic import ValidationError

# ---------------------------------------------------------------------------
# Primitive strategies
# ---------------------------------------------------------------------------

# Plain text without control chars, matches "non-empty after strip".
non_empty_text = st.text(
    alphabet=st.characters(min_codepoint=33, max_codepoint=126),
    min_size=1,
    max_size=32,
).filter(lambda s: s.strip() != "")

uniprot_accession = st.from_regex(r"\A[A-NR-Z][0-9][A-Z0-9]{3}[0-9]\Z", fullmatch=True)
go_id = st.from_regex(r"\AGO:[0-9]{7}\Z", fullmatch=True)
eco_id = st.from_regex(r"\AECO:[0-9]{7}\Z", fullmatch=True)

positive_int = st.integers(min_value=1, max_value=10_000)
non_neg_float = st.floats(
    min_value=0.0,
    max_value=1_000.0,
    allow_nan=False,
    allow_infinity=False,
)


def _roundtrip_via_json(model):
    """Serialise -> JSON -> deserialise and assert equality.

    JSON is the wire format every payload travels through. Using
    ``mode="json"`` ensures pydantic produces JSON-compatible primitives
    (str for UUID, list for set) which a downstream worker would receive.
    """
    dumped = model.model_dump(mode="json")
    # JSON-roundtrip catches "field stored as tuple but JSON yields list"
    # mismatches that ``model_dump`` alone would hide.
    blob = json.dumps(dumped, sort_keys=True)
    restored = type(model).model_validate(json.loads(blob))
    assert restored == model
    # Re-dump must be stable: a single round-trip must be a fixed point.
    assert restored.model_dump(mode="json") == dumped


# ---------------------------------------------------------------------------
# predict_go_terms payloads
# ---------------------------------------------------------------------------


@st.composite
def predict_go_terms_payloads(draw) -> PredictGOTermsPayload:
    queries: list[str] | None = draw(
        st.one_of(
            st.none(),
            st.lists(uniprot_accession, min_size=0, max_size=8, unique=True),
        )
    )
    return PredictGOTermsPayload(
        embedding_config_id=draw(non_empty_text),
        annotation_set_id=draw(non_empty_text),
        ontology_snapshot_id=draw(non_empty_text),
        query_accessions=queries,
        query_set_id=draw(st.one_of(st.none(), non_empty_text)),
        limit_per_entry=draw(positive_int),
        distance_threshold=draw(
            st.one_of(
                st.none(),
                st.floats(min_value=0.0, max_value=2.0, allow_nan=False, allow_infinity=False),
            )
        ),
        batch_size=draw(positive_int),
        search_backend=draw(st.sampled_from(["numpy", "faiss"])),
        metric=draw(st.sampled_from(["cosine", "l2", "ip"])),
        faiss_index_type=draw(st.sampled_from(["Flat", "IVF", "HNSW"])),
        faiss_nlist=draw(st.integers(min_value=1, max_value=4096)),
        faiss_nprobe=draw(st.integers(min_value=1, max_value=128)),
        faiss_hnsw_m=draw(st.integers(min_value=4, max_value=64)),
        faiss_hnsw_ef_search=draw(st.integers(min_value=1, max_value=512)),
        compute_alignments=draw(st.booleans()),
        compute_taxonomy=draw(st.booleans()),
        compute_reranker_features=draw(st.booleans()),
        compute_v6_features=draw(st.booleans()),
        compute_lineage_features=draw(st.booleans()),
        expand_votes_to_ancestors=draw(st.booleans()),
        aspect_separated_knn=draw(st.booleans()),
        reranker_model_id=draw(st.one_of(st.none(), non_empty_text)),
    )


@given(payload=predict_go_terms_payloads())
def test_predict_go_terms_payload_roundtrips(payload: PredictGOTermsPayload) -> None:
    _roundtrip_via_json(payload)


def test_predict_go_terms_payload_rejects_blank_required_ids() -> None:
    # field_validator strips and rejects empty strings on the three IDs.
    with pytest.raises(ValueError):
        PredictGOTermsPayload(
            embedding_config_id="   ",
            annotation_set_id="x",
            ontology_snapshot_id="y",
        )


@st.composite
def predict_go_terms_batch_payloads(draw) -> PredictGOTermsBatchPayload:
    return PredictGOTermsBatchPayload(
        embedding_config_id=draw(non_empty_text),
        annotation_set_id=draw(non_empty_text),
        ontology_snapshot_id=draw(non_empty_text),
        prediction_set_id=draw(non_empty_text),
        parent_job_id=draw(non_empty_text),
        query_accessions=draw(st.lists(uniprot_accession, min_size=1, max_size=8, unique=True)),
        query_set_id=draw(st.one_of(st.none(), non_empty_text)),
        limit_per_entry=draw(positive_int),
        distance_threshold=draw(
            st.one_of(
                st.none(),
                st.floats(min_value=0.0, max_value=2.0, allow_nan=False, allow_infinity=False),
            )
        ),
        search_backend=draw(st.sampled_from(["numpy", "faiss"])),
        metric=draw(st.sampled_from(["cosine", "l2", "ip"])),
        compute_alignments=draw(st.booleans()),
        compute_taxonomy=draw(st.booleans()),
        compute_reranker_features=draw(st.booleans()),
        compute_v6_features=draw(st.booleans()),
        compute_lineage_features=draw(st.booleans()),
        expand_votes_to_ancestors=draw(st.booleans()),
        aspect_separated_knn=draw(st.booleans()),
        reranker_model_id=draw(st.one_of(st.none(), non_empty_text)),
        reranker_artifact_uri=draw(st.one_of(st.none(), non_empty_text)),
        reranker_feature_schema_sha=draw(st.one_of(st.none(), non_empty_text)),
    )


@given(payload=predict_go_terms_batch_payloads())
def test_predict_go_terms_batch_payload_roundtrips(payload: PredictGOTermsBatchPayload) -> None:
    _roundtrip_via_json(payload)


# ---------------------------------------------------------------------------
# StorePredictionsPayload
# ---------------------------------------------------------------------------


prediction_dict = st.fixed_dictionaries(
    {
        "protein_accession": uniprot_accession,
        "go_id": go_id,
    },
    optional={
        "distance": st.floats(min_value=0.0, max_value=2.0, allow_nan=False, allow_infinity=False),
        "identity_nw": st.floats(min_value=0.0, max_value=1.0, allow_nan=False, allow_infinity=False),
        "identity_sw": st.floats(min_value=0.0, max_value=1.0, allow_nan=False, allow_infinity=False),
        "taxonomic_distance": non_neg_float,
        "neighbor_vote_fraction": st.floats(
            min_value=0.0, max_value=1.0, allow_nan=False, allow_infinity=False
        ),
        "evidence_code": st.sampled_from(["IDA", "IEA", "IBA", "ISS", "NAS", "ND"]),
    },
)


@given(
    parent=non_empty_text,
    prediction_set=non_empty_text,
    predictions=st.lists(prediction_dict, max_size=8),
    is_final=st.booleans(),
)
def test_store_predictions_payload_roundtrips(
    parent: str, prediction_set: str, predictions: list[dict], is_final: bool
) -> None:
    payload = StorePredictionsPayload(
        parent_job_id=parent,
        prediction_set_id=prediction_set,
        predictions=predictions,
        is_final_chunk=is_final,
    )
    _roundtrip_via_json(payload)


# ---------------------------------------------------------------------------
# RerankerSpec
# ---------------------------------------------------------------------------


@st.composite
def reranker_specs(draw) -> RerankerSpec:
    extras_value = st.recursive(
        st.one_of(st.none(), st.booleans(), st.integers(), non_empty_text),
        lambda inner: st.lists(inner, max_size=4)
        | st.dictionaries(non_empty_text, inner, max_size=4),
        max_leaves=6,
    )
    return RerankerSpec(
        runner=draw(non_empty_text),
        objective=draw(st.sampled_from(["binary", "lambdarank"])),
        enabled_feature_families=draw(
            st.one_of(st.none(), st.lists(non_empty_text, max_size=4, unique=True))
        ),
        drop_features=draw(st.lists(non_empty_text, max_size=4, unique=True)),
        seed=draw(st.integers(min_value=0, max_value=2**31 - 1)),
        extras=draw(st.dictionaries(non_empty_text, extras_value, max_size=4)),
    )


@given(spec=reranker_specs())
def test_reranker_spec_roundtrips(spec: RerankerSpec) -> None:
    _roundtrip_via_json(spec)


def test_reranker_spec_rejects_blank_runner() -> None:
    with pytest.raises(ValueError):
        RerankerSpec(runner="   ")


# ---------------------------------------------------------------------------
# Source-side stream payloads + records
# ---------------------------------------------------------------------------


@given(
    url=non_empty_text,
    timeout=positive_int,
)
def test_goa_stream_payload_roundtrips(url: str, timeout: int) -> None:
    payload = GoaStreamPayload(gaf_url=url, timeout_seconds=timeout)
    _roundtrip_via_json(payload)


@given(
    ids=st.one_of(st.none(), st.lists(uniprot_accession, max_size=8, unique=True)),
    batch=positive_int,
    timeout=positive_int,
)
def test_quickgo_stream_payload_roundtrips(
    ids: list[str] | None, batch: int, timeout: int
) -> None:
    payload = QuickGoStreamPayload(
        gene_product_ids=ids,
        gene_product_batch_size=batch,
        timeout_seconds=timeout,
    )
    _roundtrip_via_json(payload)


@given(url=non_empty_text, timeout=positive_int)
def test_eco_mapping_payload_roundtrips(url: str, timeout: int) -> None:
    payload = EcoMappingPayload(url=url, timeout_seconds=timeout)
    _roundtrip_via_json(payload)


@st.composite
def quickgo_annotation_records(draw) -> QuickGoAnnotationRecord:
    return QuickGoAnnotationRecord(
        accession=draw(uniprot_accession),
        go_id=draw(go_id),
        qualifier=draw(st.one_of(st.none(), st.sampled_from(["enables", "involved_in"]))),
        eco_id=draw(st.one_of(st.none(), eco_id)),
        db_reference=draw(st.one_of(st.none(), non_empty_text)),
        with_from=draw(st.one_of(st.none(), non_empty_text)),
        assigned_by=draw(st.one_of(st.none(), non_empty_text)),
        annotation_date=draw(st.one_of(st.none(), st.from_regex(r"\A\d{8}\Z", fullmatch=True))),
    )


@given(record=quickgo_annotation_records())
def test_quickgo_annotation_record_roundtrips(record: QuickGoAnnotationRecord) -> None:
    _roundtrip_via_json(record)


@st.composite
def goa_annotation_records(draw) -> GoaAnnotationRecord:
    return GoaAnnotationRecord(
        accession=draw(uniprot_accession),
        go_id=draw(go_id),
        qualifier=draw(st.one_of(st.none(), st.sampled_from(["enables", "involved_in"]))),
        evidence_code=draw(
            st.one_of(st.none(), st.sampled_from(["IDA", "IEA", "IBA", "ISS", "NAS", "ND"]))
        ),
        db_reference=draw(st.one_of(st.none(), non_empty_text)),
        with_from=draw(st.one_of(st.none(), non_empty_text)),
        assigned_by=draw(st.one_of(st.none(), non_empty_text)),
        annotation_date=draw(st.one_of(st.none(), st.from_regex(r"\A\d{8}\Z", fullmatch=True))),
    )


@given(record=goa_annotation_records())
def test_goa_annotation_record_roundtrips(record: GoaAnnotationRecord) -> None:
    _roundtrip_via_json(record)


# ---------------------------------------------------------------------------
# UniProt payloads + records
# ---------------------------------------------------------------------------


@st.composite
def uniprot_fasta_payloads(draw) -> UniProtFastaStreamPayload:
    return UniProtFastaStreamPayload(
        search_criteria=draw(non_empty_text),
        page_size=draw(st.integers(min_value=1, max_value=500)),
        timeout_seconds=draw(positive_int),
        include_isoforms=draw(st.booleans()),
        compressed=draw(st.booleans()),
        max_retries=draw(st.integers(min_value=1, max_value=20)),
        backoff_base_seconds=draw(
            st.floats(min_value=0.0, max_value=10.0, allow_nan=False, allow_infinity=False)
        ),
        backoff_max_seconds=draw(
            st.floats(min_value=0.0, max_value=120.0, allow_nan=False, allow_infinity=False)
        ),
        jitter_seconds=draw(
            st.floats(min_value=0.0, max_value=5.0, allow_nan=False, allow_infinity=False)
        ),
        user_agent=draw(non_empty_text),
        base_url=draw(non_empty_text),
    )


@given(payload=uniprot_fasta_payloads())
def test_uniprot_fasta_payload_roundtrips(payload: UniProtFastaStreamPayload) -> None:
    _roundtrip_via_json(payload)


@st.composite
def uniprot_metadata_payloads(draw) -> UniProtMetadataStreamPayload:
    return UniProtMetadataStreamPayload(
        search_criteria=draw(non_empty_text),
        fields=draw(st.lists(non_empty_text, min_size=1, max_size=8, unique=True)),
        page_size=draw(st.integers(min_value=1, max_value=500)),
        timeout_seconds=draw(positive_int),
        compressed=draw(st.booleans()),
        max_retries=draw(st.integers(min_value=1, max_value=20)),
        backoff_base_seconds=draw(
            st.floats(min_value=0.0, max_value=10.0, allow_nan=False, allow_infinity=False)
        ),
        backoff_max_seconds=draw(
            st.floats(min_value=0.0, max_value=120.0, allow_nan=False, allow_infinity=False)
        ),
        jitter_seconds=draw(
            st.floats(min_value=0.0, max_value=5.0, allow_nan=False, allow_infinity=False)
        ),
        user_agent=draw(non_empty_text),
        base_url=draw(non_empty_text),
    )


@given(payload=uniprot_metadata_payloads())
def test_uniprot_metadata_payload_roundtrips(payload: UniProtMetadataStreamPayload) -> None:
    _roundtrip_via_json(payload)


amino_acid_seq = st.text(
    alphabet="ACDEFGHIKLMNPQRSTVWY", min_size=1, max_size=64
)


@st.composite
def uniprot_protein_records(draw) -> UniProtProteinRecord:
    canonical = draw(uniprot_accession)
    is_canonical = draw(st.booleans())
    isoform_idx = None if is_canonical else draw(st.integers(min_value=2, max_value=20))
    accession = canonical if is_canonical else f"{canonical}-{isoform_idx}"
    sequence = draw(amino_acid_seq)
    return UniProtProteinRecord(
        accession=accession,
        entry_name=draw(st.one_of(st.none(), non_empty_text)),
        canonical_accession=canonical,
        is_canonical=is_canonical,
        isoform_index=isoform_idx,
        organism=draw(st.one_of(st.none(), non_empty_text)),
        taxonomy_id=draw(st.one_of(st.none(), st.from_regex(r"\A\d{1,7}\Z", fullmatch=True))),
        gene_name=draw(st.one_of(st.none(), non_empty_text)),
        reviewed=draw(st.booleans()),
        sequence=sequence,
        length=len(sequence),
        sequence_hash=draw(st.from_regex(r"\A[0-9a-f]{32}\Z", fullmatch=True)),
    )


@given(record=uniprot_protein_records())
def test_uniprot_protein_record_roundtrips(record: UniProtProteinRecord) -> None:
    _roundtrip_via_json(record)
    # length must equal len(sequence) — implied by the schema; assert
    # the invariant the property tests *generate* is also what the contract
    # would enforce on a real ingest path.
    assert record.length == len(record.sequence)


@given(
    accession=uniprot_accession,
    raw=st.dictionaries(non_empty_text, st.text(max_size=32), max_size=6),
)
def test_uniprot_metadata_record_roundtrips(accession: str, raw: dict[str, str]) -> None:
    record = UniProtMetadataRecord(accession=accession, raw_fields=raw)
    _roundtrip_via_json(record)


# ---------------------------------------------------------------------------
# Cross-cutting invariants
# ---------------------------------------------------------------------------


@given(payload=predict_go_terms_payloads())
def test_predict_go_terms_payload_is_frozen(payload: PredictGOTermsPayload) -> None:
    """Hypothesis-witnessed: every generated payload refuses field mutation."""
    with pytest.raises(ValidationError):
        payload.batch_size = payload.batch_size + 1  # type: ignore[misc]


@given(
    parent=non_empty_text,
    prediction_set=non_empty_text,
    predictions=st.lists(prediction_dict, max_size=4),
)
def test_store_predictions_payload_predictions_are_serializable(
    parent: str, prediction_set: str, predictions: list[dict]
) -> None:
    """Every generated payload's ``predictions`` survives ``json.dumps``.

    Hypothesis here protects against silently introducing a float NaN
    or a non-JSON-serialisable Python object into the candidate strategy.
    """
    payload = StorePredictionsPayload(
        parent_job_id=parent,
        prediction_set_id=prediction_set,
        predictions=predictions,
    )
    blob = json.dumps(payload.model_dump(mode="json"), sort_keys=True)
    # All numeric values that came in finite must remain finite after JSON.
    parsed = json.loads(blob)
    for row in parsed["predictions"]:
        for v in row.values():
            if isinstance(v, float):
                assert math.isfinite(v)
