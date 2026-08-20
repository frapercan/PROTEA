"""A prediction set has to say how it was produced.

Before this, sets were stored with ``meta={}``. The columns name the
inputs and none of the decisions, and the job that made those decisions
recorded no link to what it produced, so a published score could not be
traced to the regime that earned it.
"""

from __future__ import annotations

import uuid

import pytest
from protea_contracts import PredictGOTermsPayload
from protea_contracts.payloads import DonorPolicy

from protea.core.operations.predict_go_terms._receipt import run_receipt


def _payload(**over) -> PredictGOTermsPayload:
    base = dict(
        embedding_config_id=str(uuid.uuid4()),
        annotation_set_id=str(uuid.uuid4()),
        ontology_snapshot_id=str(uuid.uuid4()),
        limit_per_entry=3,
        search_backend="numpy",
        metric="cosine",
        donor_policy=DonorPolicy(
            reviewed_only=False,
            evidence_codes=["EXP", "IDA"],
            exclude_reference_prefixes=[],
        ),
    )
    base.update(over)
    return PredictGOTermsPayload(**base)


class TestTheDecisionsTravel:
    def test_records_the_search_regime(self):
        r = run_receipt(_payload(), uuid.uuid4())
        assert r["search_backend"] == "numpy"
        assert r["metric"] == "cosine"

    def test_records_which_donors_were_allowed_to_vote(self):
        # The difference between an experimental-only bank and one that
        # lets electronic annotations vote is the difference between two
        # entirely different claims, and the score alone does not show it.
        r = run_receipt(_payload(), uuid.uuid4())
        assert r["donor_policy"]["evidence_codes"] == ["EXP", "IDA"]
        assert r["donor_policy"]["reviewed_only"] is False

    def test_records_the_job_that_ran_it(self):
        # The load-bearing field: with it, anything not copied here is
        # still recoverable from the job. Without it, nothing is.
        jid = uuid.uuid4()
        assert run_receipt(_payload(), jid)["job_id"] == str(jid)

    def test_names_the_feature_blocks_that_were_computed(self):
        # Three are on by default, so the interesting assertion is that an
        # opt-in one joins them rather than that the list is exactly these.
        r = run_receipt(_payload(compute_lineage_features=True), uuid.uuid4())
        assert "compute_lineage_features" in r["features"]
        assert "compute_alignments" in r["features"]

    def test_omits_features_that_were_off(self):
        r = run_receipt(_payload(compute_alignments=False), uuid.uuid4())
        assert "compute_alignments" not in r["features"]
        assert "compute_protst" not in r["features"]


class TestItDoesNotInventContext:
    def test_no_faiss_block_under_an_exact_backend(self):
        # numpy searches exhaustively. Printing nprobe beside it would
        # describe a knob that did nothing to this run.
        assert "faiss" not in run_receipt(_payload(), uuid.uuid4())

    def test_faiss_block_when_the_search_was_approximate(self):
        r = run_receipt(_payload(search_backend="faiss", faiss_index_type="IVF"), uuid.uuid4())
        assert r["faiss"]["index_type"] == "IVF"
        assert "nprobe" in r["faiss"]

    def test_no_reranker_key_when_none_was_bound(self):
        assert "rerankers" not in run_receipt(_payload(), uuid.uuid4())

    def test_reranker_recorded_per_category_when_bound(self):
        rid = str(uuid.uuid4())
        r = run_receipt(_payload(reranker_model_id_nk=rid), uuid.uuid4())
        assert r["rerankers"] == {"nk": rid}


class TestItSurvivesTheDatabase:
    def test_the_receipt_is_json_serialisable(self):
        # It goes into a JSONB column. A UUID left unstringified would
        # fail at flush, after the batches were already built.
        import json

        json.dumps(run_receipt(_payload(reranker_model_id=str(uuid.uuid4())), uuid.uuid4()))


@pytest.mark.parametrize("field", ["job_id", "search_backend", "metric", "donor_policy"])
def test_no_field_is_silently_absent(field):
    assert field in run_receipt(_payload(), uuid.uuid4())
