"""A comparison says which method field it varies, and the seal checks the rest.

THE PAIR THIS FILE IS BUILT ON IS REAL. On 2026-08-28 prediction sets 31dd3eb8
and 0e076cb3 were compared believing only the donor bank had moved. The values
below are the ones ``prediction_set.meta`` actually holds for them. Three method
fields differ besides the bank -- ``expand_votes_to_ancestors``,
``aspect_separated_knn`` and ``code_revision`` -- and so does
``ontology_snapshot_id``, which the bank change dragged along with it. The first
alone explains one arm producing 2.76 times the candidates per protein of the
other, which was read as an ontology effect for five hours. Nine deltas with
intervals were published off that pair and every one of them is void.

EVERY FIXTURE HERE IS HETEROGENEOUS, and that is not decoration. A pair built
from two rows of defaults -- both booleans false, no features, no threshold --
passes a seal that reads the wrong keys, reads no keys at all, or compares the
two rows by identity. This project has shipped a uniform fixture past a live
defect twice. So the arms below differ in values a careless implementation
cannot accidentally agree on, and the arm that is supposed to be IDENTICAL to
its baseline is identical in a row full of distinct, non-default values.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import numpy as np
import pytest

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

from protea.core.method_seal import (
    EXECUTION_ONLY_FIELDS,
    METHOD_IDENTITY_FIELDS,
    CrossedMethodAxes,
    assert_one_axis,
    method_identity,
    unclassified_columns,
)
from protea.core.operations._paired_panels_artifact import PanelComparabilityError
from protea.core.operations.compare_paired_panels import (
    ComparePairedPanelsOperation,
    ComparePairedPanelsPayload,
)
from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
from tests.helpers.paired_panels import tau_grid_for, write_panel

TH_STEP = 0.4
TAUS = tau_grid_for(TH_STEP)
MFO = "molecular_function"

_A_RESULT = "aaaaaaaa-0000-0000-0000-00000000000a"
_B_RESULT = "bbbbbbbb-0000-0000-0000-00000000000b"

# The two prediction sets of the invalidated comparison, as the database holds
# them. Written out rather than derived so the regression is legible: a reader
# can put this table beside ``SELECT meta FROM prediction_set`` and check it.
_BANK_220 = "31dd3eb8-f36f-423a-bd03-45515dfe44ce"
_BANK_165 = "0e076cb3-d616-402b-8ca5-60743038cf92"
_DEPENDENCIES = {
    "protea-method": "a120f613b9d2f1646ab5a566f97a8eadc1310a23",
    "protea-runners": "ccd06fd80c12b5195339478dae7255d1ea58f4a6",
    "protea-sources": "ad0810b9a2d117c336189d6f3f55cb3fa9aba86a",
    "protea-backends": "9b76719a14d729390be7d0faf957fae06a31ced5",
    "protea-contracts": "74d329ff22a77baa5142f477889dd6e2e2ca2a4f",
    "protea-reranker-lab": "6d4dc2d4b5330485b8830b20f7789356af7be1d4",
}


def _pset(
    pset_id: str,
    *,
    annotation_set_id: str,
    ontology_snapshot_id: str,
    code_revision: str,
    aspect_separated_knn: bool,
    expand_votes_to_ancestors: bool,
    **columns: Any,
) -> dict[str, Any]:
    """One whole ``prediction_set`` row, in the shape ``SELECT *`` returns."""
    meta_over = columns.pop("meta", {})
    row: dict[str, Any] = {
        "id": pset_id,
        "embedding_config_id": "4d5d29ee-5a8c-53d2-bdc2-080187971454",
        "annotation_set_id": annotation_set_id,
        "ontology_snapshot_id": ontology_snapshot_id,
        "query_set_id": "4951cdda-c352-4b54-84d0-0076834bc1ca",
        "limit_per_entry": 200,
        "distance_threshold": None,
        "created_at": "2026-08-27T22:14:03Z",
        "meta": {
            "job_id": f"job-for-{pset_id[:8]}",
            "batch_size": 1024,
            "metric": "cosine",
            "search_backend": "numpy",
            "features": [],
            "donor_policy": {
                "reviewed_only": False,
                "evidence_codes": None,
                "exclude_reference_prefixes": [],
            },
            "exclude_self_neighbour": True,
            "code_revision": code_revision,
            "dependency_revisions": dict(_DEPENDENCIES),
            "aspect_separated_knn": aspect_separated_knn,
            "expand_votes_to_ancestors": expand_votes_to_ancestors,
            **meta_over,
        },
    }
    row.update(columns)
    return row


def _the_invalidated_pair() -> tuple[dict[str, Any], dict[str, Any]]:
    """31dd3eb8 and 0e076cb3, exactly as the record holds them."""
    return (
        _pset(
            _BANK_220,
            annotation_set_id="cbb35a32-44e4-4e39-b524-05b4b7433727",
            ontology_snapshot_id="6b78af68-eb01-477e-a599-fcde11ff0135",
            code_revision="8699bfd7eec382aadd03f471b127f8be6f71cffd",
            aspect_separated_knn=True,
            expand_votes_to_ancestors=False,
        ),
        _pset(
            _BANK_165,
            annotation_set_id="575cb8da-f302-474e-9525-2be2f93d6791",
            ontology_snapshot_id="c6c7f402-3f59-4f8b-8a44-fc8cfa1264b9",
            code_revision="087fefebc494a231443255739a673bcfe5c9194f",
            aspect_separated_knn=False,
            expand_votes_to_ancestors=True,
        ),
    )


def _a_clean_bank_contrast() -> tuple[dict[str, Any], dict[str, Any]]:
    """What that comparison should have been: the bank moves, the method does not.

    Both rows carry the same non-default method -- aspect-separated retrieval on,
    ancestor expansion off, a reviewed-only donor policy, a distance threshold, a
    feature block -- so a seal that ignored the meta entirely, or compared only
    the columns, would pass this pair AND the invalidated one above. Only a seal
    reading both halves tells them apart.
    """
    shared: dict[str, Any] = {
        "ontology_snapshot_id": "6b78af68-eb01-477e-a599-fcde11ff0135",
        "code_revision": "8699bfd7eec382aadd03f471b127f8be6f71cffd",
        "aspect_separated_knn": True,
        "expand_votes_to_ancestors": False,
        "distance_threshold": 0.35,
        "meta": {
            "features": ["compute_alignments", "compute_taxonomy"],
            "donor_policy": {
                "reviewed_only": True,
                "evidence_codes": ["EXP", "IDA"],
                "exclude_reference_prefixes": ["GO_REF"],
            },
        },
    }
    return (
        _pset(_BANK_220, annotation_set_id="cbb35a32-44e4-4e39-b524-05b4b7433727", **shared),
        _pset(_BANK_165, annotation_set_id="575cb8da-f302-474e-9525-2be2f93d6791", **shared),
    )


# ---------------------------------------------------------------------------
# The two rows each gate reads, and the operation driven end to end
# ---------------------------------------------------------------------------


class _StubResult:
    def __init__(self, row: dict[str, Any] | None) -> None:
        self._row = row

    def mappings(self) -> _StubResult:
        return self

    def first(self) -> dict[str, Any] | None:
        return self._row


class _StubSession:
    """``evaluation_result`` by ``id``, ``prediction_set`` by ``psid``."""

    def __init__(self, results: dict[str, Any], psets: dict[str, Any]) -> None:
        self._results = results
        self._psets = psets

    def execute(self, statement: Any, params: dict[str, Any] | None = None) -> _StubResult:
        bound = params or {}
        if "psid" in bound:
            return _StubResult(self._psets.get(bound["psid"]))
        return _StubResult(self._results.get(bound.get("id", "")))


def _result_row(result_id: str, prediction_set_id: str) -> dict[str, Any]:
    """One sealed ``evaluation_result``, so the FRAME gate passes and the METHOD gate decides.

    Both sides carry one digest and one leakage role on purpose. The frame gate
    runs first, and a fixture that let it fire would prove only that the first
    gate works, which is the test that was already there.
    """
    return {
        "id": result_id,
        "evaluation_set_id": "eval-set-select-220-227",
        "prediction_set_id": prediction_set_id,
        "scoring_config_id": None,
        "reranker_model_id": None,
        "frame": "internal",
        "temporal_window": "220->227",
        "frame_digest": "f-0f2a71c4b9e5d3a6c8b10f24",
        "leakage_role": "select",
        "results": {},
    }


def _session(a_pset: dict[str, Any], b_pset: dict[str, Any]) -> _StubSession:
    return _StubSession(
        {
            _A_RESULT: _result_row(_A_RESULT, a_pset["id"]),
            _B_RESULT: _result_row(_B_RESULT, b_pset["id"]),
        },
        {a_pset["id"]: a_pset, b_pset["id"]: b_pset},
    )


def _write_panels(tmp_path: Path) -> None:
    """A panel with heterogeneous ground-truth mass, so the verdict is a real one.

    Six proteins whose weighted masses span an order of magnitude, and two arms
    that separate on them. A panel of identical proteins would produce a verdict
    too, and it would prove nothing about whether the bytes were read.
    """
    n_gt = np.array([40.0, 36.0, 30.0, 8.0, 5.0, 4.0])
    for side, gain in (("a", 1.0), ("b", 0.55)):
        tp = np.stack([np.array([m * 0.6 * gain, m * 0.2 * gain]) for m in n_gt])
        pred = np.stack([np.array([m * 0.9, m * 0.3]) for m in n_gt])
        write_panel(
            tmp_path / side,
            "NK",
            MFO,
            accessions=[f"Q{i:05d}" for i in range(len(n_gt))],
            tp=tp,
            pred=pred,
            n_gt=n_gt,
            th_step=TH_STEP,
        )


def _run(tmp_path: Path, session: _StubSession, **payload: Any) -> dict[str, Any]:
    body = {
        "evaluation_result_id": _A_RESULT,
        "baseline_evaluation_result_id": _B_RESULT,
        "artifacts_root": str(tmp_path / "a"),
        "baseline_artifacts_root": str(tmp_path / "b"),
        "panels": ["NK:MFO"],
        "n_resamples": 1000,
        "seed": 0,
        "min_population": 1,
        **payload,
    }
    return ComparePairedPanelsOperation().execute(
        cast("Session", session), body, emit=lambda *a, **k: None
    ).result


# ---------------------------------------------------------------------------
# 1. It refuses, and it names every field that moved
# ---------------------------------------------------------------------------


class TestItRefusesTheComparisonThatWasPublished:
    def test_the_real_pair_is_refused_naming_every_undeclared_field(
        self, tmp_path: Path
    ) -> None:
        # The whole point of the seal, on the pair that cost five hours. A
        # refusal naming ONE field would have been survivable: the reader fixes
        # that one, re-runs, and publishes a delta still carrying the other two.
        _write_panels(tmp_path)
        a, b = _the_invalidated_pair()
        with pytest.raises(PanelComparabilityError) as caught:
            _run(tmp_path, _session(a, b), method_axis=["annotation_set_id"])
        message = str(caught.value)
        for field in (
            "aspect_separated_knn",
            "code_revision",
            "expand_votes_to_ancestors",
            "ontology_snapshot_id",
        ):
            assert field in message, f"the refusal does not name {field}"

    def test_the_refusal_carries_both_values_of_each_field(self) -> None:
        # A refusal that names a field and not its two values sends the reader
        # back to the database to find out what actually differed, which is the
        # step nobody took on 2026-08-28.
        a, b = _the_invalidated_pair()
        with pytest.raises(CrossedMethodAxes) as caught:
            assert_one_axis(["annotation_set_id"], a, b, names=(_BANK_220, _BANK_165))
        message = str(caught.value)
        assert "expand_votes_to_ancestors: 31dd3eb8" in message
        assert "8699bfd7eec382aadd03f471b127f8be6f71cffd" in message
        assert "087fefebc494a231443255739a673bcfe5c9194f" in message
        assert message.count("\n  ") == 4

    def test_a_guard_that_only_forbade_the_ontology_would_have_passed_three(self) -> None:
        # The guard drafted on the night of the defect forbade
        # ontology_snapshot_id and nothing else. Declaring the bank AND the
        # ontology is that guard exactly, and the seal still refuses, because it
        # is the complement of the axis and not a list of banned fields.
        a, b = _the_invalidated_pair()
        with pytest.raises(CrossedMethodAxes) as caught:
            assert_one_axis(
                ["annotation_set_id", "ontology_snapshot_id"], a, b, names=("A", "B")
            )
        message = str(caught.value)
        assert "aspect_separated_knn" in message
        assert "code_revision" in message
        assert "expand_votes_to_ancestors" in message

    def test_an_undeclared_method_difference_is_refused_by_default(self) -> None:
        # No axis at all is the claim that the method was held still, which is
        # what a scoring-config or reranker comparison asserts. It is not a
        # permissive default.
        a, b = _the_invalidated_pair()
        with pytest.raises(CrossedMethodAxes, match="declared a comparison of nothing"):
            assert_one_axis([], a, b)


# ---------------------------------------------------------------------------
# 2. It acts when the axis is the whole truth
# ---------------------------------------------------------------------------


class TestItActsOnAComparisonThatIsWhatItSaysItIs:
    def test_a_clean_bank_contrast_returns_a_verdict(self, tmp_path: Path) -> None:
        _write_panels(tmp_path)
        a, b = _a_clean_bank_contrast()
        result = _run(tmp_path, _session(a, b), method_axis=["annotation_set_id"])
        assert result["verdict"], "a comparison that passed the seal produced no verdict"
        assert result["panels"]["NK:MFO"]["status"] != "refused"

    def test_the_declared_axis_is_recorded_beside_the_number(self, tmp_path: Path) -> None:
        # In the RESULT and not only in an event. A stored delta that does not
        # say which field it is a delta in is the artefact the nine invalid
        # deltas were, and nobody reads a job's event stream a month later.
        _write_panels(tmp_path)
        a, b = _a_clean_bank_contrast()
        result = _run(tmp_path, _session(a, b), method_axis=["annotation_set_id"])
        assert result["method_axis"] == ["annotation_set_id"]

    def test_two_arms_on_one_prediction_set_need_no_axis(self, tmp_path: Path) -> None:
        # Two scoring configs over one prediction method: the method did not
        # move, the empty axis is true, and the seal must not stand in the way
        # of the comparisons it was never about.
        _write_panels(tmp_path)
        a, _ = _a_clean_bank_contrast()
        result = _run(tmp_path, _session(a, dict(a)))
        assert result["method_axis"] == []

    def test_an_axis_that_did_not_move_is_refused(self) -> None:
        # The other direction, and the one that keeps the seal switched on.
        # Without it a caller declares every field in METHOD_IDENTITY_FIELDS,
        # passes any pair at all, and the job row reads like care.
        a, b = _a_clean_bank_contrast()
        with pytest.raises(CrossedMethodAxes, match="do not differ in it"):
            assert_one_axis(["annotation_set_id", "metric"], a, b)

    def test_declaring_the_whole_surface_does_not_disable_the_seal(self) -> None:
        a, b = _the_invalidated_pair()
        with pytest.raises(CrossedMethodAxes, match="do not differ in it"):
            assert_one_axis(list(METHOD_IDENTITY_FIELDS), a, b)


# ---------------------------------------------------------------------------
# 3. It does not widen itself
# ---------------------------------------------------------------------------


class TestItDoesNotWidenInSilence:
    def test_an_unclassified_meta_key_refuses(self, tmp_path: Path) -> None:
        # The failure mode a deny-list has: somebody adds a decision to the
        # receipt, every existing guard keeps passing, and the new field is free
        # to vary between two arms nobody knew differed. Here it stops the
        # comparison until it is classified.
        _write_panels(tmp_path)
        a, b = _a_clean_bank_contrast()
        b["meta"] = {**b["meta"], "soft_propagation_alpha": 0.4}
        with pytest.raises(PanelComparabilityError, match="soft_propagation_alpha"):
            _run(tmp_path, _session(a, b), method_axis=["annotation_set_id"])

    def test_an_unclassified_meta_key_refuses_even_when_both_sides_carry_it(self) -> None:
        # Agreeing on an unclassified field is not evidence that it is safe to
        # ignore: the next pair will disagree on it, and by then the seal has
        # been silently reading around it for a month.
        a, b = _a_clean_bank_contrast()
        for row in (a, b):
            row["meta"] = {**row["meta"], "soft_propagation_alpha": 0.4}
        with pytest.raises(CrossedMethodAxes, match="neither method identity nor execution"):
            assert_one_axis(["annotation_set_id"], a, b)

    def test_every_column_of_prediction_set_is_classified(self) -> None:
        # The build-breaking half. A column added to the model without a line in
        # method_seal fails here, on the commit that adds it, rather than at the
        # first comparison that happens to vary in it.
        columns = [c.name for c in PredictionSet.__table__.columns]
        assert unclassified_columns(columns) == ()

    def test_every_key_the_receipt_writes_is_classified(self) -> None:
        # The producer side of the same rule. ``run_receipt`` is what fills
        # prediction_set.meta, including the two blocks it writes only
        # conditionally, and neither of those appears in any row of the current
        # campaign -- so a seal built by reading the database alone would have
        # missed both.
        import uuid

        from protea_contracts import PredictGOTermsPayload
        from protea_contracts.payloads import DonorPolicy

        from protea.core.operations.predict_go_terms._receipt import run_receipt

        written: set[str] = set()
        for backend in ("numpy", "faiss"):
            payload = PredictGOTermsPayload(
                embedding_config_id=str(uuid.uuid4()),
                annotation_set_id=str(uuid.uuid4()),
                ontology_snapshot_id=str(uuid.uuid4()),
                limit_per_entry=5,
                search_backend=backend,
                metric="cosine",
                donor_policy=DonorPolicy(reviewed_only=False, evidence_codes=["EXP"]),
                reranker_model_id=str(uuid.uuid4()),
            )
            written |= set(run_receipt(payload, uuid.uuid4()))
        classified = set(METHOD_IDENTITY_FIELDS) | set(EXECUTION_ONLY_FIELDS)
        assert written - classified == set(), "the receipt writes keys the seal does not know"
        assert {"faiss", "rerankers"} <= written, "the conditional blocks stopped being written"

    def test_an_unrecorded_revision_is_not_a_shared_revision(self) -> None:
        # Two absences agree everywhere else in this seal, and correctly: two
        # runs with no faiss block both ran no faiss. A revision is the
        # exception, because the stored donor_policy was byte-identical either
        # side of the 2026-08-29 change of what it gates.
        a, b = _a_clean_bank_contrast()
        for row in (a, b):
            row["meta"] = {k: v for k, v in row["meta"].items() if k != "code_revision"}
        with pytest.raises(CrossedMethodAxes, match="does not record"):
            assert_one_axis(["annotation_set_id"], a, b)

    def test_a_misspelled_axis_is_refused_rather_than_ignored(self) -> None:
        # 'annotation_set' declares nothing, so the bank difference would then be
        # refused as undeclared: the right refusal for the wrong reason, sending
        # the reader after a difference they had already named.
        a, b = _a_clean_bank_contrast()
        with pytest.raises(CrossedMethodAxes, match="is not method identity"):
            assert_one_axis(["annotation_set"], a, b)

    def test_the_payload_refuses_a_misspelled_axis_too(self) -> None:
        with pytest.raises(ValueError, match="not method identity"):
            ComparePairedPanelsPayload(
                evaluation_result_id=_A_RESULT,
                baseline_evaluation_result_id=_B_RESULT,
                declared_tau=None,
                effect_of_interest=None,
                restrict_to_stratum=None,
                method_axis=["bank"],
            )


class TestTheJobListSaysWhatWasCompared:
    def test_the_summary_line_names_the_axis(self) -> None:
        # The one line a reader scanning a job list sees. Two ids say which rows
        # were compared and not what the comparison was OF, which is the sentence
        # the nine invalid deltas were filed under.
        line = ComparePairedPanelsOperation().summarize_payload(
            {
                "evaluation_result_id": _A_RESULT,
                "baseline_evaluation_result_id": _B_RESULT,
                "method_axis": ["annotation_set_id"],
            }
        )
        assert "varying annotation_set_id" in line

    def test_the_summary_line_says_so_when_nothing_varies(self) -> None:
        # An axis declared and empty. The caller said the method does not move,
        # and the gate held them to it.
        line = ComparePairedPanelsOperation().summarize_payload(
            {
                "evaluation_result_id": _A_RESULT,
                "baseline_evaluation_result_id": _B_RESULT,
                "method_axis": [],
            }
        )
        assert "one method held still" in line

    def test_a_job_that_predates_the_seal_is_not_reported_as_held_still(self) -> None:
        # The stored payload of job 96e1942d, byte for byte, which is the job
        # the nine invalid deltas were published from. It carries no method_axis
        # key because it ran before there was one, and its two arms differ in
        # four method fields. Rendering that absence as the empty axis would put
        # "one method held still" on the job list row of exactly the comparison
        # that did not, which is this operation making the D2 claim itself. The
        # jobs endpoint renders summarize_payload over every historical row, so
        # this is the live reading, not a hypothetical one.
        line = ComparePairedPanelsOperation().summarize_payload(
            {
                "seed": 0,
                "n_resamples": 2000,
                "evaluation_result_id": "8a17d0e2-28ca-4d59-bf1f-4fc251a4941e",
                "baseline_evaluation_result_id": "10535ac0-24af-4df2-9ef7-57c1e9fdffc8",
            }
        )
        assert "one method held still" not in line
        assert "no method axis declared" in line

    def test_an_axis_that_is_not_a_list_is_named_rather_than_spelled_out(self) -> None:
        # POST /jobs stores the payload blob unvalidated and the operation model
        # only sees it on dequeue, so the job list renders strings the contract
        # would have refused. Joined character by character, one field name
        # reads as seventeen fields varying.
        line = ComparePairedPanelsOperation().summarize_payload(
            {
                "evaluation_result_id": _A_RESULT,
                "baseline_evaluation_result_id": _B_RESULT,
                "method_axis": "annotation_set_id",
            }
        )
        assert "varying a, n, n, o" not in line
        assert "is not a list of fields" in line


class TestTheClassificationIsAPartition:
    def test_no_field_is_both_identity_and_execution(self) -> None:
        assert set(METHOD_IDENTITY_FIELDS) & set(EXECUTION_ONLY_FIELDS) == set()

    def test_every_exclusion_states_why(self) -> None:
        # An exclusion put wrong is the hole the seal exists to close, so none of
        # them is allowed to be a bare name in a tuple.
        for field, reason in EXECUTION_ONLY_FIELDS.items():
            assert len(reason) > 40, f"{field} is excluded without an argument"

    def test_the_search_backend_is_identity(self) -> None:
        # The exclusion a reader reaches for first, and the one that would be
        # wrong: numpy searches exactly and faiss approximately, so two arms on
        # different backends can hold different donors for the same query.
        assert "search_backend" in METHOD_IDENTITY_FIELDS
        assert "faiss" in METHOD_IDENTITY_FIELDS

    def test_a_uuid_column_and_its_text_spelling_are_one_value(self) -> None:
        # SELECT * hands back UUID objects for the columns and plain strings for
        # anything inside meta. Compared raw, two spellings of one id would be
        # reported as a difference in the bank, which is a refusal nobody can act
        # on and which would have fired on the pair that WAS a clean contrast.
        import uuid as _uuid

        a, b = _a_clean_bank_contrast()
        b["annotation_set_id"] = a["annotation_set_id"]
        b["ontology_snapshot_id"] = _uuid.UUID(str(a["ontology_snapshot_id"]))
        assert method_identity(a)["ontology_snapshot_id"] == str(a["ontology_snapshot_id"])
        assert assert_one_axis([], a, b) == ()

    def test_the_identity_of_a_row_survives_a_re_run(self) -> None:
        # batch_size, job_id, id and created_at are the whole of what a re-run of
        # the same method may change. If any of them leaked into identity, no
        # prediction set would ever compare equal to its own re-run.
        a, _ = _a_clean_bank_contrast()
        rerun = {
            **a,
            "id": "99999999-9999-9999-9999-999999999999",
            "created_at": "2026-09-14T09:00:00Z",
            "meta": {**a["meta"], "job_id": "job-rerun", "batch_size": 256},
        }
        assert method_identity(a) == method_identity(rerun)
        assert assert_one_axis([], a, rerun) == ()
