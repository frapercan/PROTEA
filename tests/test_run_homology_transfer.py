"""Homology transfer on a window of LAFA's frame: donors, twins, channels and records.

Against Postgres (``--with-postgres``), on the small cut of
``tests/helpers/native_methods.py``. T1's sequence hits D1 (identity 0.8 over 100
residues, 200 bits), D2 (0.5, 100, 100 bits) and N1 (0.6 over 50, 80 bits), and its
twin TW shares it; T2's hits D1 (0.3 over 200, 50 bits); T3 hits nothing. The donor
states at t0 (truth channel): D1 holds GO:0000001, GO:0000002 (P) and GO:0000003,
GO:0000006 (F); D2 holds GO:0000005; TW holds GO:0000001 and GO:0000002, its NOT
row having taken GO:0000003 out; N1 has only an electronic orthology row
(GO:0000005) and votes only when that channel is admitted. Every expected score
below is worked out from those numbers.
"""

from __future__ import annotations

import io
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import pytest
from pydantic import ValidationError
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

import protea.infrastructure.orm.models  # noqa: F401  (registers every table)
from protea.core import method_write
from protea.core.method_seal import assert_one_axis, method_identity
from protea.core.operations import run_homology_transfer as op
from protea.infrastructure.orm.base import Base
from protea.infrastructure.orm.models.embedding.prediction_score import PredictionScore
from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
from protea.infrastructure.orm.models.query.query_set import QuerySet, QuerySetEntry
from protea.infrastructure.orm.models.sequence.sequence import Sequence
from tests.helpers.cut_photos import make_job
from tests.helpers.native_methods import World, add_self_scores, make_world, whole_row

_TRUTH_STORE = "protea.core._evaluation_groundtruth._store"
_WINDOW = "4228a366-3dc4-4c30-b4a4-287482a59edf"


@pytest.fixture()
def pg(postgres_url: str) -> Iterator[Session]:
    engine = create_engine(postgres_url, future=True)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session
    Base.metadata.drop_all(engine)
    engine.dispose()


@pytest.fixture()
def world(pg: Session, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> World:
    built = make_world(pg, tmp_path)
    add_self_scores(pg, built)
    pg.commit()
    monkeypatch.setattr(_TRUTH_STORE, lambda: built.store)
    monkeypatch.setattr(op, "_store", lambda: built.store)
    monkeypatch.setattr(op, "_scratch_root", lambda: tmp_path)
    monkeypatch.setattr(method_write, "dependency_revisions", lambda: {"protea-x": "c" * 40})
    return built


def _run(pg: Session, world: World, **payload: Any) -> dict[str, Any]:
    payload = {
        "evaluation_set_id": str(world.window.id),
        "_job_id": str(make_job(pg, "run_homology_transfer")),
        **payload,
    }
    result = (
        op.RunHomologyTransferOperation().execute(pg, payload, emit=lambda *a, **k: None).result
    )
    pg.commit()
    return result


def _scores(pg: Session, set_id: str, world: World) -> dict[str, dict[str, float]]:
    go_id_of = {i: g for g, i in world.term_ids.items()}
    rows = pg.execute(
        select(PredictionScore).where(PredictionScore.prediction_set_id == uuid.UUID(set_id))
    ).scalars()
    return {
        r.protein_accession: {go_id_of[t]: s for t, s in zip(r.go_term_ids, r.scores, strict=True)}
        for r in rows
    }


TRUTH = {"evidence_channels": ["truth"]}


def test_the_payload_refuses_what_a_transfer_cannot_mean() -> None:
    base = {"evaluation_set_id": _WINDOW, "rule": "bitscore_vote", "denominator": "any_aspect"}
    assert op.RunHomologyTransferPayload.model_validate(base).self_exclusion == "accession"
    for bad in (
        {"self_exclusion": "accession_after_retrieval"},
        {"donor_policy": {"reviewed_only": True}},
        {"donor_policy": {"admitted_by_release": 211}},
        {"denominator": None},
        {"multiplicity": "twins"},
    ):
        with pytest.raises(ValidationError):
            op.RunHomologyTransferPayload.model_validate({**base, **bad})


@pytest.mark.parametrize(
    ("payload", "code"),
    [
        ({"rule": "top_hit_identity", "identity": "local"}, "homology-top-hit"),
        ({"rule": "top_hit_identity", "identity": "global"}, "homology-top-hit-global"),
        ({"rule": "bitscore_vote", "denominator": "any_aspect"}, "homology-bits"),
        (
            {"rule": "bitscore_identity_vote", "identity": "global", "denominator": "any_aspect"},
            "homology-bits-identity",
        ),
        (
            {
                "rule": "bitscore_identity_vote",
                "identity": "local",
                "denominator": "same_aspect",
                "hit_cap": 299,
                "multiplicity": "sequence",
            },
            "homology-bits-identity-local-same-aspect-top-299-by-sequence",
        ),
    ],
)
def test_every_variant_has_a_code_that_fits_a_short_name(
    payload: dict[str, Any], code: str
) -> None:
    p = op.RunHomologyTransferPayload.model_validate({"evaluation_set_id": _WINDOW, **payload})
    assert op.method_code(p) == code and len(f"{code}-211") <= 64


@pytest.mark.integration
def test_the_top_hit_reads_the_cafa_identity_with_the_twin_at_one(
    pg: Session, world: World
) -> None:
    result = _run(pg, world, rule="top_hit_identity", identity="local", donor_policy=TRUTH)
    scores = _scores(pg, result["prediction_set_id"], world)
    assert scores["T1"] == pytest.approx(
        {
            "GO:0000001": 1.0,  # TW, at identity 1
            "GO:0000002": 1.0,
            "GO:0000003": 0.8,  # D1; TW's NOT took it out of TW's state
            "GO:0000006": 0.8,
            "GO:0000005": 0.5,  # D2
        },
        abs=1e-6,
    )
    assert scores["T2"] == pytest.approx(
        dict.fromkeys(("GO:0000001", "GO:0000002", "GO:0000003", "GO:0000006"), 0.3), abs=1e-6
    )
    assert "T3" not in scores
    pset = pg.get(PredictionSet, uuid.UUID(result["prediction_set_id"]))
    assert (
        pset.meta["self_exclusion"] == "accession" and pset.meta["exclude_self_neighbour"] is True
    )
    assert pset.meta["donor_policy"] == {
        "reviewed_only": False,
        "evidence_codes": None,
        "exclude_reference_prefixes": [],
        "evidence_channels": ["truth"],
    }
    assert pset.meta["targets_with_a_term"] == 2
    assert {p.role for p in pset.method.pins} == {"photo", "homology-table"}
    assert pset.method.family == "explainable-units"
    method_identity(whole_row(pg, pset.id))
    again = _run(pg, world, rule="top_hit_identity", identity="local", donor_policy=TRUTH)
    assert again == {"prediction_set_id": result["prediction_set_id"], "skipped": True}


@pytest.mark.integration
def test_s2_weighs_bits_by_global_identity_and_the_twin_by_its_self_score(
    pg: Session, world: World
) -> None:
    result = _run(
        pg,
        world,
        rule="bitscore_identity_vote",
        identity="global",
        denominator="any_aspect",
        donor_policy=TRUTH,
    )
    d1, d2, twin = 200 * 80 / 120, 100 * 50 / 100, 250.0  # bits x nident / max(qlen, slen)
    total = d1 + d2 + twin
    scores = _scores(pg, result["prediction_set_id"], world)
    assert scores["T1"] == pytest.approx(
        {
            "GO:0000001": (d1 + twin) / total,
            "GO:0000002": (d1 + twin) / total,
            "GO:0000003": d1 / total,
            "GO:0000006": d1 / total,
            "GO:0000005": d2 / total,
        }
    )
    assert scores["T2"] == pytest.approx(
        dict.fromkeys(("GO:0000001", "GO:0000002", "GO:0000003", "GO:0000006"), 1.0)
    )
    pset = pg.get(PredictionSet, uuid.UUID(result["prediction_set_id"]))
    assert {p.role for p in pset.method.pins} == {"photo", "homology-table", "homology-self-scores"}
    counts = pset.meta["written_counts"]
    assert counts["twins"] == {
        "voted": 1,
        "left_out_without_a_self_score": 0,
        "targets_with_twins_left_out": 0,
    }
    assert counts["target_sequences_without_a_self_score"] == 1  # T3's sequence has none
    evidence = pq.read_table(io.BytesIO(world.store.get(pset.meta["evidence_artifact"]["uri"])))
    best = {
        (a, g): (donor, twin_flag)
        for a, g, donor, twin_flag in zip(
            *(
                evidence[c].to_pylist()
                for c in ("accession", "go_id", "best_donor", "best_is_twin")
            ),
            strict=True,
        )
    }
    assert best[("T1", "GO:0000001")] == ("TW", True) and best[("T1", "GO:0000005")] == (
        "D2",
        False,
    )
    method_identity(whole_row(pg, pset.id))


@pytest.mark.integration
def test_the_recipe_channels_admit_a_neighbours_orthology_row(pg: Session, world: World) -> None:
    recipe = [
        "truth",
        "curated",
        "phylogenetic",
        "orthology_electronic",
        "reviewed_field_electronic",
    ]
    truth_only = _run(pg, world, rule="bitscore_vote", denominator="any_aspect", donor_policy=TRUTH)
    channels = _run(
        pg,
        world,
        rule="bitscore_vote",
        denominator="any_aspect",
        donor_policy={"evidence_channels": recipe},
    )
    t1 = _scores(pg, channels["prediction_set_id"], world)["T1"]
    assert t1["GO:0000005"] == pytest.approx((100 + 80) / (200 + 100 + 80 + 250))
    assert _scores(pg, truth_only["prediction_set_id"], world)["T1"]["GO:0000005"] == pytest.approx(
        100 / (200 + 100 + 250)
    )
    # One method, two donor policies: a comparison of the two declares the policy alone.
    a = whole_row(pg, uuid.UUID(channels["prediction_set_id"]))
    b = whole_row(pg, uuid.UUID(truth_only["prediction_set_id"]))
    assert a["method_id"] == b["method_id"]
    assert assert_one_axis(["donor_policy"], a, b) == ("donor_policy",)


@pytest.mark.integration
def test_a_declared_donor_set_admits_only_its_members(pg: Session, world: World) -> None:
    sequence = pg.execute(select(Sequence).limit(1)).scalar_one()
    donors = QuerySet(
        name="donors",
        source_kind="photo_members",
        cut_photo_id=world.photo.id,
        artifact_uri="file:///donors.txt",
        content_sha256="d" * 64,
        description="D1 and the twin.",
        job_id=make_job(pg, "declare_photo_member_set"),
    )
    pg.add(donors)
    pg.flush()
    pg.add_all(
        QuerySetEntry(query_set_id=donors.id, accession=a, sequence_id=sequence.id)
        for a in ("D1", "TW")
    )
    pg.commit()
    result = _run(
        pg,
        world,
        rule="top_hit_identity",
        identity="local",
        donor_policy={"donor_set_id": str(donors.id), "evidence_channels": ["truth"]},
    )
    assert "GO:0000005" not in _scores(pg, result["prediction_set_id"], world)["T1"]
    with pytest.raises(op.HomologyTransferRefused, match="photo_members"):
        _run(
            pg,
            world,
            rule="top_hit_identity",
            identity="local",
            donor_policy={"donor_set_id": str(uuid.uuid4())},
        )


@pytest.mark.integration
def test_sequence_exclusion_takes_the_twins_out(pg: Session, world: World) -> None:
    result = _run(
        pg,
        world,
        rule="top_hit_identity",
        identity="local",
        donor_policy=TRUTH,
        self_exclusion="sequence",
    )
    assert _scores(pg, result["prediction_set_id"], world)["T1"]["GO:0000001"] == pytest.approx(0.8)


@pytest.mark.integration
def test_a_vote_without_self_scores_is_refused_before_anything_is_written(
    pg: Session, world: World
) -> None:
    from protea.infrastructure.orm.models.annotation.homology_self_scores import HomologySelfScores

    pg.query(HomologySelfScores).delete()
    pg.commit()
    with pytest.raises(op.HomologyTransferRefused, match="load_homology_self_scores"):
        _run(pg, world, rule="bitscore_vote", denominator="any_aspect")
    assert pg.execute(select(PredictionSet)).first() is None
