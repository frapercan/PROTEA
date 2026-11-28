"""A window's targets read from their own non-experimental annotations at t0 (democafa goa_nonexp).

The pure parts (codes through the closure, rows without roots and denials, the
side reconciliation fed other rows) are tested alone; the loader and the method's
writing run against Postgres, with the reconciliation stubbed in the latter the way
``tests/test_run_count_map.py`` stubs the count map's inputs.
"""

from __future__ import annotations

import io
import uuid
from pathlib import Path
from types import SimpleNamespace

import pyarrow.parquet as pq
import pytest
from pydantic import ValidationError
from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from protea.core import evaluation, method_write
from protea.core._binding_rule import DESCENDANTS_STEP
from protea.core._evaluation_graph import _load_parents_by_go_id
from protea.core._lafa_frame_data import EndOntology
from protea.core.code_revision import code_revision
from protea.core.method_seal import method_identity
from protea.core.operations import run_own_annotations as op
from protea.core.query_population import population_of
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.infrastructure.orm.models.annotation.go_term import GOTerm
from protea.infrastructure.orm.models.annotation.protein_go_annotation import ProteinGOAnnotation
from protea.infrastructure.orm.models.embedding.go_prediction import GOPrediction
from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
from protea.infrastructure.orm.models.method.method import method_short_name
from protea.infrastructure.orm.models.method.prediction_explanation import PredictionExplanation
from protea.infrastructure.orm.models.protein.protein import Protein
from protea.infrastructure.storage.local import LocalFsArtifactStore
from tests.helpers.native_methods import World, make_world
from tests.helpers.windows import make_window
from tests.test_run_count_map import pg  # noqa: F401

#: A prediction set as ``SELECT *`` returns it, which is what the method seal reads.
_WHOLE_ROW = text("SELECT * FROM prediction_set WHERE id = :p")

PARENTS = {"GO:0000003": {"GO:0000002"}, "GO:0000002": {"GO:0008150"}}


def test_a_terms_codes_are_those_of_the_direct_terms_that_imply_it() -> None:
    codes = op.term_codes({"GO:0000003": {"IEA"}, "GO:0000002": {"IBA"}}, PARENTS)
    assert codes == {
        "GO:0000003": {"IEA"},
        "GO:0000002": {"IEA", "IBA"},
        "GO:0008150": {"IEA", "IBA"},
    }


def test_rows_leave_out_roots_denials_and_terms_the_pivot_lacks() -> None:
    positives = {"P1": {"P": {"GO:0008150", "GO:0000002", "GO:0000003", "GO:0000009"}}}
    codes = {"P1": {"GO:0000002": {"IBA"}, "GO:0000003": {"IEA"}}}
    ids = {"GO:0000002": 2, "GO:0000003": 3, "GO:0000004": 4}
    rows = list(op.prediction_rows(positives, {"P1": {"GO:0000003"}}, codes, ids))
    assert rows == [("P1", 2, 1.0, {"own": True, "aspect": "P", "codes": ["IBA"]})]
    # a term the reconciliation placed that no direct closure names (a remap) gets every code
    remapped = {"P1": {"P": {"GO:0000004"}}}
    (row,) = op.prediction_rows(remapped, {}, codes, ids)
    assert row[3]["codes"] == ["IBA", "IEA"]


def test_the_side_reconciles_the_rows_it_is_handed(monkeypatch: pytest.MonkeyPatch) -> None:
    def never(*a: object, **k: object) -> None:
        raise AssertionError("the experimental rows must not be read when raw is given")

    monkeypatch.setattr(evaluation, "_load_experimental_raw_go_ids", never)
    monkeypatch.setattr(evaluation, "_load_parents_by_go_id", lambda s, i: PARENTS)
    monkeypatch.setattr(evaluation, "_drop_binding_only", lambda s, i, raw: 0)
    side = SimpleNamespace(
        native_snapshot_id=uuid.uuid4(),
        annotation_set_id=uuid.uuid4(),
        frame=None,
        binding=DESCENDANTS_STEP,
    )
    out, _ = evaluation._reconcile_experimental_side(
        None,  # type: ignore[arg-type]
        side,  # type: ignore[arg-type]
        {"GO:0000002", "GO:0000003"},
        {"GO:0000002": "P", "GO:0000003": "P"},
        raw={"P1": {"GO:0000003"}},
    )
    assert out == {"P1": {"P": {"GO:0000002", "GO:0000003"}}}


def test_a_lafa_window_states_the_terms_as_its_frame_states_t0(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Named and closed under t0's own ontology, a NOT term of t0 taken out before the closure."""
    t0 = EndOntology(
        live={"GO:0008150": "P", "GO:0000002": "P", "GO:0000003": "P", "GO:0000005": "P"},
        parents={
            "GO:0000003": frozenset({"GO:0000002"}),
            "GO:0000005": frozenset({"GO:0000002"}),
            "GO:0000002": frozenset({"GO:0008150"}),
        },
        alt_ids={"GO:0000013": "GO:0000003"},
        replaced_by={},
    )
    asked: dict[str, object] = {}

    def end_rows(_session: object, set_id: object, experimental_only: bool) -> tuple:
        asked["set"], asked["experimental_only"] = set_id, experimental_only
        return {}, {"P1": {"GO:0000005"}, "ELSEWHERE": {"GO:0000003"}}

    monkeypatch.setattr(op, "load_end_ontology", lambda _session, _snapshot: t0)
    monkeypatch.setattr(op, "load_end_rows", end_rows)
    eset = SimpleNamespace(old_annotation_set_id=uuid.uuid4(), not_codes="experimental")
    direct = {"P1": {"GO:0000013": {"IEA"}, "GO:0000005": {"IBA"}}}

    stated = op.lafa_positives(None, eset, uuid.uuid4(), direct)  # type: ignore[arg-type]

    # The alt id is named as its primary and closed upwards; the NOT term left first.
    assert stated == {"P1": {"P": {"GO:0000003", "GO:0000002", "GO:0008150"}}}
    assert asked == {"set": eset.old_annotation_set_id, "experimental_only": True}


def test_the_method_of_a_lafa_window_is_another_method() -> None:
    photo = SimpleNamespace(
        id=uuid.uuid4(),
        cut_release=211,
        ontology_snapshot_id=uuid.uuid4(),
        manifest_sha256="a" * 64,
    )
    reconciled = op.method_spec(photo)  # type: ignore[arg-type]
    assert op.method_spec(photo, "as_closed").method_id == reconciled.method_id  # type: ignore[arg-type]
    lafa = op.method_spec(photo, "own_graph")  # type: ignore[arg-type]
    assert lafa.method_id != reconciled.method_id
    # A variant at the same cut needs a short name of its own, which the store keeps unique.
    assert method_short_name(lafa) == "own-annotations-nonexp-lafa-211"
    assert method_short_name(reconciled) == "own-annotations-nonexp-211"
    assert (lafa.recipe["state"], reconciled.recipe["state"]) == (
        op.LAFA_STATE,
        op.RECONCILED_STATE,
    )


def _window_with_rows(pg: Session) -> EvaluationSet:  # noqa: F811
    eset = pg.get(EvaluationSet, make_window(pg, snapshot_version="own-annotations"))
    terms = {}
    for go_id in ("GO:0000002", "GO:0000003", "GO:0008150"):
        term = GOTerm(go_id=go_id, aspect="P", ontology_snapshot_id=eset.pivot_snapshot_id)
        pg.add(term)
        terms[go_id] = term
    for acc in ("P1", "P2", "P3"):
        pg.add(Protein(accession=acc, canonical_accession=acc))
    pg.flush()
    rows = [
        ("P1", "GO:0000003", "IEA", None),
        ("P1", "GO:0000002", "IDA", None),
        ("P1", "GO:0008150", "ND", None),
        ("P1", "GO:0000002", "IEA", "NOT|involved_in"),
        ("P2", "GO:0000002", "IBA", None),
        ("P3", "GO:0000003", "IEA", None),
    ]
    for acc, go_id, code, qualifier in rows:
        pg.add(
            ProteinGOAnnotation(
                annotation_set_id=eset.old_annotation_set_id,
                protein_accession=acc,
                go_term_id=terms[go_id].id,
                evidence_code=code,
                qualifier=qualifier,
            )
        )
    pg.commit()
    return eset


@pytest.mark.integration
def test_a_run_writes_its_method_rows_and_explanations_once(
    pg: Session,  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    # Whatever this environment installed its siblings from, the set records them.
    monkeypatch.setattr(method_write, "dependency_revisions", lambda: {"protea-x": "c" * 40})
    monkeypatch.setattr(op, "_store", lambda: LocalFsArtifactStore(tmp_path / "store"))
    eset = _window_with_rows(pg)
    photo = SimpleNamespace(
        id=uuid.uuid4(),
        cut_release=211,
        ontology_snapshot_id=eset.pivot_snapshot_id,
        annotation_set_id=eset.old_annotation_set_id,
        manifest_sha256="b" * 64,
        kind="full",
    )
    ids = op.term_ids(pg, eset.pivot_snapshot_id)
    why = {"own": True, "aspect": "P", "codes": ["IEA"], "channel_mask": 8}
    rows = [("P1", ids["GO:0000003"], 1.0, why), ("P1", ids["GO:0000002"], 1.0, why)]
    monkeypatch.setattr(op, "resolve", lambda session, e: (eset, photo))
    monkeypatch.setattr(
        op, "read_own", lambda session, e, ph, ch=None: op.OwnRead(["P1", "P2"], None, rows)
    )
    payload = {"evaluation_set_id": str(eset.id)}
    result = op.RunOwnAnnotationsOperation().execute(pg, payload, emit=lambda *a, **k: None).result
    pg.commit()
    assert result["rows"] == result["explanations"] == 2 and result["targets_with_a_term"] == 1
    pset = pg.get(PredictionSet, uuid.UUID(result["prediction_set_id"]))
    assert pset.meta["evaluation_set_id"] == str(eset.id)
    # It was asked about the window's targets, with or without a term, and says so.
    assert pset.meta["query_population"] == population_of(["P1", "P2"])
    assert pset.meta["code_revision"] == code_revision()
    # The channel record of its rows is its evidence, by sha256.
    assert pset.meta["evidence_artifact"]["rows"] == 2
    # And the seal can read it whole: nothing unclassified, nothing unrecorded.
    method_identity(dict(pg.execute(_WHOLE_ROW, {"p": pset.id}).mappings().one()))
    explained = pg.execute(select(func.count()).select_from(PredictionExplanation)).scalar_one()
    assert explained == 2
    again = op.RunOwnAnnotationsOperation().execute(pg, payload, emit=lambda *a, **k: None).result
    assert again["skipped"] is True and again["prediction_set_id"] == result["prediction_set_id"]


def test_the_channels_are_own_channels_once_and_every_one_is_the_unset_rule() -> None:
    window = "4228a366-3dc4-4c30-b4a4-287482a59edf"
    every = op.RunOwnAnnotationsPayload.model_validate(
        {"evaluation_set_id": window, "evidence_channels": list(reversed(op.OWN_CHANNELS))}
    )
    assert every.evidence_channels is None
    recipe = op.RunOwnAnnotationsPayload.model_validate(
        {"evaluation_set_id": window, "evidence_channels": list(reversed(op.RECIPE_OWN_CHANNELS))}
    )
    assert recipe.evidence_channels == list(op.RECIPE_OWN_CHANNELS)
    for bad in (["truth"], ["no_data"], [], ["curated", "curated"]):
        with pytest.raises(ValidationError):
            op.RunOwnAnnotationsPayload.model_validate(
                {"evaluation_set_id": window, "evidence_channels": bad}
            )


def test_unset_channels_keep_the_method_and_a_channel_set_is_a_method_of_its_own() -> None:
    photo = SimpleNamespace(
        id=uuid.uuid4(),
        cut_release=211,
        ontology_snapshot_id=uuid.uuid4(),
        manifest_sha256="a" * 64,
    )
    before = op.method_spec(photo, "own_graph")  # type: ignore[arg-type]
    assert "evidence_channels" not in before.recipe
    assert op.method_spec(photo, "own_graph", None).method_id == before.method_id  # type: ignore[arg-type]
    recipe = op.method_spec(photo, "own_graph", op.RECIPE_OWN_CHANNELS)  # type: ignore[arg-type]
    assert recipe.method_id != before.method_id
    assert method_short_name(recipe) == "own-annotations-recipe-channels-lafa-211"
    single = op.method_spec(photo, None, ("reviewed_field_electronic",))  # type: ignore[arg-type]
    assert method_short_name(single) == "own-annotations-reviewed-field-211"


def test_the_explanation_carries_the_channels_that_imply_a_term() -> None:
    positives = {"P1": {"P": {"GO:0000002", "GO:0000003", "GO:0000004"}}}
    codes = {"P1": {"GO:0000002": {"IBA"}, "GO:0000003": {"IEA", "IBA"}}}
    channels = {
        "P1": {
            "GO:0000002": {"phylogenetic"},
            "GO:0000003": {"orthology_electronic", "phylogenetic"},
        }
    }
    ids = {"GO:0000002": 2, "GO:0000003": 3, "GO:0000004": 4}
    rows = {r[1]: r[3] for r in op.prediction_rows(positives, {}, codes, ids, channels)}
    bits = op.CHANNEL_BITS
    assert rows[2]["channel_mask"] == bits["phylogenetic"]
    assert rows[3]["channel_mask"] == bits["phylogenetic"] | bits["orthology_electronic"]
    # A term no direct closure names (a remap) takes every channel of its target.
    assert rows[4]["channel_mask"] == bits["phylogenetic"] | bits["orthology_electronic"]


# On the small cut of tests/helpers/native_methods.py ------------------------------

_TRUTH_STORE = "protea.core._evaluation_groundtruth._store"

#: The query run_own_annotations read its rows with before channels existed.
_BEFORE = text(
    "SELECT pga.protein_accession, gt.go_id, pga.evidence_code "
    "FROM protein_go_annotation pga JOIN go_term gt ON gt.id = pga.go_term_id "
    "WHERE pga.annotation_set_id = :s AND pga.protein_accession = ANY(:t) "
    "AND NOT (pga.evidence_code = ANY(:skip)) "
    "AND (pga.qualifier IS NULL OR pga.qualifier NOT LIKE '%NOT%')"
)


@pytest.fixture()
def world(pg: Session, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> World:  # noqa: F811
    built = make_world(pg, tmp_path)
    monkeypatch.setattr(_TRUTH_STORE, lambda: built.store)
    monkeypatch.setattr(op, "_store", lambda: built.store)
    monkeypatch.setattr(method_write, "dependency_revisions", lambda: {"protea-x": "c" * 40})
    return built


@pytest.mark.integration
def test_the_loader_reads_each_rows_channel_and_admits_the_asked_ones(
    pg: Session,  # noqa: F811
    world: World,
) -> None:
    every = op.own_rows(pg, world.photo, ["T1", "T2", "T3"])
    assert every.channels == {
        "T1": {
            "GO:0000005": {"orthology_electronic"},
            "GO:0000003": {"signature_electronic"},
            "GO:0000004": {"curated"},
        },
        "T2": {"GO:0000002": {"phylogenetic"}, "GO:0000006": {"reviewed_field_electronic"}},
        "T3": {"GO:0000006": {"signature_electronic"}},
    }
    recipe = op.own_rows(pg, world.photo, ["T1", "T2", "T3"], op.RECIPE_OWN_CHANNELS)
    assert set(recipe.codes) == {"T1", "T2"} and "GO:0000003" not in recipe.codes["T1"]


@pytest.mark.integration
def test_unset_channels_write_the_rows_they_always_wrote(
    pg: Session,  # noqa: F811
    world: World,
) -> None:
    read = op.read_own(pg, world.window, world.photo)
    rows = pg.execute(
        _BEFORE,
        {
            "s": world.photo.annotation_set_id,
            "t": ["T1", "T2", "T3"],
            "skip": [*evaluation._EXP_CODES, *op.NOT_KNOWLEDGE],
        },
    )
    direct: dict[str, dict[str, set[str]]] = {}
    for protein, go_id, code in rows:
        direct.setdefault(protein, {}).setdefault(go_id, set()).add(code)
    positives = op.lafa_positives(pg, world.window, world.photo.ontology_snapshot_id, direct)
    parents = _load_parents_by_go_id(pg, world.photo.ontology_snapshot_id)
    codes = {p: op.term_codes(t, parents) for p, t in direct.items()}
    before = list(
        op.prediction_rows(positives, {}, codes, op.term_ids(pg, world.window.pivot_snapshot_id))
    )
    assert [(a, t, s) for a, t, s, _ in read.rows] == [(a, t, s) for a, t, s, _ in before]
    assert [{k: v for k, v in w.items() if k != "channel_mask"} for *_, w in read.rows] == [
        w for *_, w in before
    ]
    assert all("channel_mask" in w for *_, w in read.rows)


@pytest.mark.integration
def test_the_recipe_arm_leaves_the_mirror_out_and_records_the_channels(
    pg: Session,  # noqa: F811
    world: World,
) -> None:
    payload = {
        "evaluation_set_id": str(world.window.id),
        "evidence_channels": list(op.RECIPE_OWN_CHANNELS),
    }
    result = op.RunOwnAnnotationsOperation().execute(pg, payload, emit=lambda *a, **k: None).result
    pg.commit()
    assert result["method"] == "own-annotations-recipe-channels-lafa-211"
    pset = pg.get(PredictionSet, uuid.UUID(result["prediction_set_id"]))
    go_id_of = {i: g for g, i in world.term_ids.items()}
    written = {
        (r.protein_accession, go_id_of[r.go_term_id])
        for r in pg.execute(
            select(GOPrediction).where(GOPrediction.prediction_set_id == pset.id)
        ).scalars()
    }
    assert ("T1", "GO:0000003") not in written and not any(a == "T3" for a, _ in written)
    assert ("T2", "GO:0000006") in written and ("T1", "GO:0000005") in written
    record = pq.read_table(io.BytesIO(world.store.get(pset.meta["evidence_artifact"]["uri"])))
    masks = dict(
        zip(
            zip(record["accession"].to_pylist(), record["go_id"].to_pylist(), strict=True),
            record["channel_mask"].to_pylist(),
            strict=True,
        )
    )
    assert masks[("T2", "GO:0000006")] == op.CHANNEL_BITS["reviewed_field_electronic"]
    assert masks[("T1", "GO:0000004")] == op.CHANNEL_BITS["curated"]
    assert record.schema.metadata[b"channel_bits"].startswith(b"truth=1,curated=2")
    method_identity(dict(pg.execute(_WHOLE_ROW, {"p": pset.id}).mappings().one()))
