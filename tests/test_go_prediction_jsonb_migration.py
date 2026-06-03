"""Tests for the T3.1 ``GOPrediction.predictions_jsonb`` dual-write scaffolding.

Four tiers:

1. Env-flag helper unit tests
   (:func:`protea.core.jsonb_dual_write.is_jsonb_dual_write_enabled` +
   :func:`protea.core.jsonb_dual_write.maybe_jsonb`): truthy / falsy
   parsing, case-insensitivity, explicit env-dict threading, and the
   compact JSONB shape.
2. Writer-site surface tests confirming ``_row_from_prediction``
   threads the helper output into the row dict it hands the bulk
   insert, and that ``StorePredictionsOperation.execute`` passes the
   column straight through.
3. Migration / ORM pins (no DB): mirrors the T1.6
   ``test_schema_sha_v2_column.py`` pattern. The ORM must declare the
   column with the right type + nullability, and the alembic file
   must keep its revision graph + the column + GIN index DDL. Running
   ``alembic upgrade`` in-process inside pytest cross-contaminates the
   shared session-scoped Postgres container, so we pin via source
   inspection instead.
4. End-to-end DB round-trip (``--with-postgres``): inserts a row with
   the flag off (predictions_jsonb stays NULL), then with the flag
   on (predictions_jsonb populated; content matches tabular columns).

Tier 4 is the load-bearing one: any future writer that bypasses
``_row_from_prediction`` trips it immediately.
"""
from __future__ import annotations

import uuid
from collections.abc import Iterator
from unittest.mock import MagicMock

import pytest

from protea.core.jsonb_dual_write import (
    JSONB_DUAL_WRITE_ENV,
    is_jsonb_dual_write_enabled,
    maybe_jsonb,
)
from protea.core.operations.predict_go_terms import (
    StorePredictionsOperation,
    _row_from_prediction,
)
from protea.infrastructure.orm.models.job import JobStatus

# ---------------------------------------------------------------------------
# Tier 1: env-flag helper
# ---------------------------------------------------------------------------


@pytest.fixture
def _clear_env(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Strip the dual-write env var so each test starts from a known state."""
    monkeypatch.delenv(JSONB_DUAL_WRITE_ENV, raising=False)
    yield


class TestIsJsonbDualWriteEnabled:
    def test_unset_is_false(self, _clear_env: None) -> None:
        assert is_jsonb_dual_write_enabled() is False

    def test_empty_is_false(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(JSONB_DUAL_WRITE_ENV, "")
        assert is_jsonb_dual_write_enabled() is False

    @pytest.mark.parametrize(
        "raw",
        ["1", "true", "TRUE", "True", "yes", "YES", "on", "On", "  on  ", " 1 "],
    )
    def test_truthy_variants(
        self, monkeypatch: pytest.MonkeyPatch, raw: str
    ) -> None:
        monkeypatch.setenv(JSONB_DUAL_WRITE_ENV, raw)
        assert is_jsonb_dual_write_enabled() is True

    @pytest.mark.parametrize(
        "raw", ["0", "false", "FALSE", "no", "off", "maybe", "2", "enabled"]
    )
    def test_falsy_variants(
        self, monkeypatch: pytest.MonkeyPatch, raw: str
    ) -> None:
        monkeypatch.setenv(JSONB_DUAL_WRITE_ENV, raw)
        assert is_jsonb_dual_write_enabled() is False

    def test_explicit_env_dict_overrides_os_environ(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Live env is off; the explicit env dict is on.
        monkeypatch.setenv(JSONB_DUAL_WRITE_ENV, "0")
        assert is_jsonb_dual_write_enabled(env={JSONB_DUAL_WRITE_ENV: "1"}) is True
        # And vice-versa.
        monkeypatch.setenv(JSONB_DUAL_WRITE_ENV, "1")
        assert is_jsonb_dual_write_enabled(env={JSONB_DUAL_WRITE_ENV: "0"}) is False


class TestMaybeJsonb:
    def test_flag_off_returns_none(self, _clear_env: None) -> None:
        assert maybe_jsonb([(42, 0.15, "IDA")]) is None

    def test_flag_off_returns_none_even_for_empty_list(
        self, _clear_env: None
    ) -> None:
        assert maybe_jsonb([]) is None

    def test_flag_on_serialises_single_tuple(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(JSONB_DUAL_WRITE_ENV, "1")
        blob = maybe_jsonb([(42, 0.15, "IDA")])
        assert blob == {
            "predictions": [
                {"go_term_id": 42, "score": 0.15, "evidence": "IDA"},
            ],
            "count": 1,
        }

    def test_flag_on_serialises_multiple_tuples_preserving_order(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(JSONB_DUAL_WRITE_ENV, "yes")
        blob = maybe_jsonb(
            [
                (1, 0.1, "IDA"),
                (2, 0.2, None),
                (3, 0.3, "IPI"),
            ]
        )
        assert blob is not None
        assert blob["count"] == 3
        assert [r["go_term_id"] for r in blob["predictions"]] == [1, 2, 3]
        assert blob["predictions"][1]["evidence"] is None

    def test_flag_on_empty_list_returns_well_formed_dict(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(JSONB_DUAL_WRITE_ENV, "on")
        assert maybe_jsonb([]) == {"predictions": [], "count": 0}

    def test_explicit_env_dict_threaded_through(
        self, _clear_env: None
    ) -> None:
        # OS env is unset, but the explicit dict turns the flag on.
        blob = maybe_jsonb(
            [(7, 0.42, "IBA")],
            env={JSONB_DUAL_WRITE_ENV: "true"},
        )
        assert blob is not None
        assert blob["predictions"][0]["score"] == 0.42

    def test_types_are_coerced(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The caller hands us numpy / Python ints + floats indiscriminately.
        # The helper coerces to canonical builtins so JSONB serialisation
        # is stable across writers.
        monkeypatch.setenv(JSONB_DUAL_WRITE_ENV, "1")
        # bool is a subclass of int; ``int(True)`` -> 1 (acceptable here).
        blob = maybe_jsonb([(True, 0, None)])  # type: ignore[list-item]
        assert blob is not None
        rec = blob["predictions"][0]
        assert isinstance(rec["go_term_id"], int)
        assert isinstance(rec["score"], float)


# ---------------------------------------------------------------------------
# Tier 2: writer-site surface (no DB)
# ---------------------------------------------------------------------------


class TestRowFromPredictionThreadsJsonb:
    def test_flag_off_writes_none(self, _clear_env: None) -> None:
        row = _row_from_prediction(
            {
                "protein_accession": "P12345",
                "go_term_id": 42,
                "ref_protein_accession": "Q99999",
                "distance": 0.15,
                "evidence_code": "IDA",
            },
            uuid.uuid4(),
        )
        assert "predictions_jsonb" in row
        assert row["predictions_jsonb"] is None

    def test_flag_on_writes_compact_blob(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(JSONB_DUAL_WRITE_ENV, "1")
        row = _row_from_prediction(
            {
                "protein_accession": "P12345",
                "go_term_id": 42,
                "ref_protein_accession": "Q99999",
                "distance": 0.15,
                "evidence_code": "IDA",
            },
            uuid.uuid4(),
        )
        blob = row["predictions_jsonb"]
        assert isinstance(blob, dict)
        assert blob["count"] == 1
        record = blob["predictions"][0]
        assert record["go_term_id"] == 42
        assert record["score"] == 0.15
        assert record["evidence"] == "IDA"

    def test_flag_on_handles_missing_evidence(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(JSONB_DUAL_WRITE_ENV, "on")
        row = _row_from_prediction(
            {
                "protein_accession": "P12345",
                "go_term_id": 99,
                "ref_protein_accession": "Q99999",
                "distance": 0.42,
            },
            uuid.uuid4(),
        )
        blob = row["predictions_jsonb"]
        assert blob is not None
        assert blob["predictions"][0]["evidence"] is None


class TestStorePredictionsThreadsJsonb:
    """``StorePredictionsOperation`` must pass the predictions_jsonb
    field straight through to the bulk insert (whether populated or
    NULL).
    """

    def test_bulk_insert_rows_carry_predictions_jsonb_key(
        self, _clear_env: None
    ) -> None:
        op = StorePredictionsOperation()
        session = MagicMock()
        parent = MagicMock()
        parent.status = JobStatus.RUNNING
        session.get.return_value = parent
        row = MagicMock()
        row.progress_current = 1
        row.progress_total = 5
        session.execute.return_value.fetchone.return_value = row

        payload = {
            "parent_job_id": str(uuid.uuid4()),
            "prediction_set_id": str(uuid.uuid4()),
            "predictions": [
                {
                    "protein_accession": "P12345",
                    "go_term_id": 42,
                    "ref_protein_accession": "Q99999",
                    "distance": 0.15,
                    "evidence_code": "IDA",
                },
            ],
        }
        op.execute(session, payload, emit=lambda *_a, **_kw: None)
        rows = session.execute.call_args_list[0].args[1]
        assert len(rows) == 1
        assert "predictions_jsonb" in rows[0]
        # Flag off => NULL placeholder.
        assert rows[0]["predictions_jsonb"] is None

    def test_bulk_insert_populates_when_flag_on(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(JSONB_DUAL_WRITE_ENV, "1")
        op = StorePredictionsOperation()
        session = MagicMock()
        parent = MagicMock()
        parent.status = JobStatus.RUNNING
        session.get.return_value = parent
        row = MagicMock()
        row.progress_current = 1
        row.progress_total = 5
        session.execute.return_value.fetchone.return_value = row

        payload = {
            "parent_job_id": str(uuid.uuid4()),
            "prediction_set_id": str(uuid.uuid4()),
            "predictions": [
                {
                    "protein_accession": "P12345",
                    "go_term_id": 42,
                    "ref_protein_accession": "Q99999",
                    "distance": 0.15,
                    "evidence_code": "IDA",
                },
            ],
        }
        op.execute(session, payload, emit=lambda *_a, **_kw: None)
        rows = session.execute.call_args_list[0].args[1]
        blob = rows[0]["predictions_jsonb"]
        assert isinstance(blob, dict)
        assert blob["predictions"][0]["go_term_id"] == 42


# ---------------------------------------------------------------------------
# Tier 3a: migration / ORM pins (no DB; mirrors T1.6 test pattern)
# ---------------------------------------------------------------------------


def test_orm_declares_predictions_jsonb_nullable_jsonb() -> None:
    """``GOPrediction.predictions_jsonb`` is nullable JSONB on the ORM.

    Catches accidental column-rename / type-drift between this slice
    and the migration that adds it.
    """
    from sqlalchemy.dialects.postgresql import JSONB

    from protea.infrastructure.orm.models.embedding.go_prediction import (
        GOPrediction,
    )

    col = GOPrediction.__table__.c["predictions_jsonb"]
    assert col.nullable is True, "predictions_jsonb must be nullable for backfill"
    assert isinstance(col.type, JSONB), (
        f"expected JSONB, got {type(col.type)!r}"
    )


def test_migration_module_loads_and_pins_revisions() -> None:
    """The T3.1 migration module must declare the expected revision graph
    and emit the column + GIN index DDL in its source.

    Catches accidental rename / deletion of the T3.1 migration file
    during merges (a recurring pre-existing risk per the T1.6 test).
    """
    import importlib.util
    from pathlib import Path

    mod_path = (
        Path(__file__).resolve().parents[1]
        / "alembic"
        / "versions"
        / "ccc0494d22f5_t3_1_goprediction_predictions_jsonb.py"
    )
    assert mod_path.is_file(), f"migration file missing at {mod_path}"

    spec = importlib.util.spec_from_file_location("t3_1_migration", mod_path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)

    assert module.revision == "ccc0494d22f5"
    assert module.down_revision == "e1c4a7b2d8f3"
    assert callable(module.upgrade)
    assert callable(module.downgrade)

    source = mod_path.read_text()
    # Forward migration adds the column and the GIN index.
    assert 'add_column(\n        "go_prediction"' in source
    assert "predictions_jsonb" in source
    assert "ix_go_prediction_jsonb_gin" in source
    assert 'postgresql_using="gin"' in source
    # Downgrade drops both, in reverse order.
    assert (
        'drop_index("ix_go_prediction_jsonb_gin", table_name="go_prediction")'
        in source
    )
    assert 'drop_column("go_prediction", "predictions_jsonb")' in source


# ---------------------------------------------------------------------------
# Tier 3b: end-to-end DB round-trip (--with-postgres)
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_dual_write_roundtrip(
    postgres_url: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """End-to-end: insert a batch with the flag off, then on, and
    verify the persisted ``predictions_jsonb`` matches expectations.
    """
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session

    import protea.infrastructure.orm.models  # noqa: F401
    from protea.core.operations.predict_go_terms import StorePredictionsOperation
    from protea.infrastructure.orm.base import Base
    from protea.infrastructure.orm.models.annotation.annotation_set import (
        AnnotationSet,
    )
    from protea.infrastructure.orm.models.annotation.go_term import GOTerm
    from protea.infrastructure.orm.models.annotation.ontology_snapshot import (
        OntologySnapshot,
    )
    from protea.infrastructure.orm.models.embedding.embedding_config import (
        EmbeddingConfig,
    )
    from protea.infrastructure.orm.models.embedding.go_prediction import (
        GOPrediction,
    )
    from protea.infrastructure.orm.models.embedding.prediction_set import (
        PredictionSet,
    )
    from protea.infrastructure.orm.models.job import Job, JobStatus

    engine = create_engine(postgres_url, future=True)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    with Session(engine, future=True) as session:
        config = EmbeddingConfig(
            model_name="test/model",
            model_backend="esm",
            layer_indices=[0],
            layer_agg="mean",
            pooling="mean",
            normalize_residues=False,
            normalize=True,
            max_length=1022,
            use_chunking=False,
            chunk_size=512,
            chunk_overlap=0,
        )
        session.add(config)
        snap = OntologySnapshot(obo_version="t3-1-v1", obo_url="http://test")
        session.add(snap)
        session.flush()

        go_term = GOTerm(
            go_id="GO:0003674",
            name="molecular_function",
            aspect="F",
            ontology_snapshot_id=snap.id,
        )
        session.add(go_term)

        ann_set = AnnotationSet(
            ontology_snapshot_id=snap.id, source="test", source_version="v1"
        )
        session.add(ann_set)
        session.flush()

        pred_set = PredictionSet(
            embedding_config_id=config.id,
            annotation_set_id=ann_set.id,
            ontology_snapshot_id=snap.id,
            limit_per_entry=5,
            meta={},
        )
        session.add(pred_set)
        parent = Job(
            operation="predict_go_terms",
            queue_name="protea.jobs",
            status=JobStatus.RUNNING,
            progress_current=0,
            progress_total=2,
        )
        session.add(parent)
        session.flush()
        pred_set_id = pred_set.id
        parent_id = parent.id
        go_term_id = go_term.id
        session.commit()

    # --- Flag OFF: row inserts; predictions_jsonb stays NULL. ---
    monkeypatch.delenv(JSONB_DUAL_WRITE_ENV, raising=False)
    payload_off = {
        "parent_job_id": str(parent_id),
        "prediction_set_id": str(pred_set_id),
        "predictions": [
            {
                "protein_accession": "P_OFF_1",
                "go_term_id": go_term_id,
                "ref_protein_accession": "Q_REF_OFF",
                "distance": 0.20,
                "evidence_code": "IEA",
            },
        ],
    }
    op = StorePredictionsOperation()
    with Session(engine, future=True) as session:
        op.execute(session, payload_off, emit=lambda *_a, **_kw: None)
        session.commit()

    with Session(engine, future=True) as session:
        row = (
            session.query(GOPrediction)
            .filter_by(prediction_set_id=pred_set_id, protein_accession="P_OFF_1")
            .one()
        )
        # Tabular columns populated.
        assert row.go_term_id == go_term_id
        assert row.distance == pytest.approx(0.20)
        assert row.evidence_code == "IEA"
        # JSONB stays NULL when the flag is off.
        assert row.predictions_jsonb is None

    # --- Flag ON: row inserts; predictions_jsonb populated; content matches. ---
    monkeypatch.setenv(JSONB_DUAL_WRITE_ENV, "1")
    payload_on = {
        "parent_job_id": str(parent_id),
        "prediction_set_id": str(pred_set_id),
        "predictions": [
            {
                "protein_accession": "P_ON_1",
                "go_term_id": go_term_id,
                "ref_protein_accession": "Q_REF_ON",
                "distance": 0.05,
                "evidence_code": "IDA",
            },
        ],
    }
    with Session(engine, future=True) as session:
        op.execute(session, payload_on, emit=lambda *_a, **_kw: None)
        session.commit()

    with Session(engine, future=True) as session:
        row = (
            session.query(GOPrediction)
            .filter_by(prediction_set_id=pred_set_id, protein_accession="P_ON_1")
            .one()
        )
        # Tabular columns populated.
        assert row.go_term_id == go_term_id
        assert row.distance == pytest.approx(0.05)
        assert row.evidence_code == "IDA"
        # JSONB populated and matches the tabular fields.
        blob = row.predictions_jsonb
        assert blob is not None
        assert blob["count"] == 1
        record = blob["predictions"][0]
        assert record["go_term_id"] == row.go_term_id
        assert record["score"] == pytest.approx(row.distance)
        assert record["evidence"] == row.evidence_code
