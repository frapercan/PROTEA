"""The gates on a method's own predictions, and the order they run in.

A prediction file arriving from another machine is the one input nothing else
in the platform checks. The operation IS its gates, so this covers what each
one refuses and, above all, that a refused file leaves no rows: the counting
pass and the insert pass are separate so that a cap nobody declared is
measured before a ``prediction_set`` row exists, and the gates fire between
the two.

Nothing here needs Postgres. The session is a double that answers the three
questions the operation asks of it and records what it was told to write,
which is what lets the "writes nothing on a refusal" claim be a test instead
of a reading of the code.
"""

from __future__ import annotations

import hashlib
import uuid
from pathlib import Path
from typing import Any

import pytest

from protea.core.operations.ingest_method_predictions import (
    IngestMethodPredictionsOperation,
    IngestMethodPredictionsPayload,
    _read_rows,
    scan,
)
from protea.infrastructure.orm.models.embedding.embedding_config import (
    INGESTED_METHOD_BACKEND,
)

#: aspect letters as go_term stores them, verified on the live pivot:
#: P 30,464 / F 12,397 / C 4,467 under releases/2022-07-01.
TERMS: dict[str, tuple[int, str]] = {
    "GO:0000001": (11, "P"),
    "GO:0000002": (22, "F"),
    "GO:0000003": (33, "C"),
}

HEADER = "protein\tgo_id\taspect\tscore\n"
GOOD = HEADER + (
    "P00001\tGO:0000001\tP\t0.632965147\n"
    "P00001\tGO:0000002\tF\t0.5\n"
    "P00002\tGO:0000003\tC\t0.25\n"
)

METHOD = {"name": "ens_ts44_noresim8", "band": "v211", "page": "criteria/e1s44-211-227.yaml"}


def _write(tmp_path: Path, body: str, name: str = "preds.tsv") -> Path:
    target = tmp_path / name
    target.write_text(body)
    return target


def _payload(path: Path, **over: Any) -> IngestMethodPredictionsPayload:
    base: dict[str, Any] = {
        "predictions_path": str(path),
        "embedding_config_id": str(uuid.uuid4()),
        "annotation_set_id": str(uuid.uuid4()),
        "ontology_snapshot_id": str(uuid.uuid4()),
        "method": METHOD,
    }
    base.update(over)
    return IngestMethodPredictionsPayload(**base)


class _Row:
    """Stands in for the three rows the set points at.

    ``model_backend`` is only read off the config, and the operation refuses
    any tag but ``ingested-method``: that refusal is what lets the leakage
    guard treat the tag as evidence that the arm was fitted elsewhere.
    """

    def __init__(self, backend: str = INGESTED_METHOD_BACKEND) -> None:
        self.id = uuid.uuid4()
        self.model_backend = backend


class _Result:
    def __init__(self, rows: list[Any]) -> None:
        self._rows = rows

    def all(self) -> list[Any]:
        return self._rows

    def fetchall(self) -> list[Any]:
        return self._rows


class _Session:
    """Answers the term query and the three lookups, and records every write."""

    def __init__(
        self,
        terms: dict[str, tuple[int, str]] | None = None,
        backend: str = INGESTED_METHOD_BACKEND,
    ) -> None:
        self._terms = TERMS if terms is None else terms
        self._backend = backend
        self.added: list[Any] = []
        self.inserted: list[int] = []
        self.flushes = 0

    def get(self, _model: type, _ident: uuid.UUID) -> _Row:
        return _Row(self._backend)

    def execute(self, statement: Any) -> _Result:
        # The term map comes back as (go_id, id, aspect); an INSERT comes back
        # as one returned id per row that landed.
        text = str(statement)
        if text.lstrip().upper().startswith("SELECT"):
            return _Result([(gid, tid, asp) for gid, (tid, asp) in self._terms.items()])
        values = getattr(getattr(statement, "_values", None), "__len__", lambda: 0)()
        landed = values or len(getattr(statement, "_multi_values", [[]])[0])
        self.inserted.append(landed)
        return _Result([(i,) for i in range(landed)])

    def add(self, row: Any) -> None:
        self.added.append(row)

    def flush(self) -> None:
        self.flushes += 1


def _emit(*_args: Any, **_kwargs: Any) -> None:
    return None


class TestThePayloadRefusesRatherThanGuessing:
    def test_it_needs_exactly_one_source(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="exactly one"):
            _payload(_write(tmp_path, GOOD), predictions_uri="some/key.tsv")

    def test_neither_source_is_also_refused(self) -> None:
        with pytest.raises(ValueError, match="exactly one"):
            IngestMethodPredictionsPayload(
                embedding_config_id=str(uuid.uuid4()),
                annotation_set_id=str(uuid.uuid4()),
                ontology_snapshot_id=str(uuid.uuid4()),
                method=METHOD,
            )

    def test_the_method_has_to_name_itself(self, tmp_path: Path) -> None:
        # The name is written on every row and identifies the arm; a set that
        # cannot say what produced it is what this operation exists to stop.
        with pytest.raises(ValueError, match="method.name is required"):
            _payload(_write(tmp_path, GOOD), method={"band": "v211"})

    def test_a_blank_name_is_not_a_name(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="method.name is required"):
            _payload(_write(tmp_path, GOOD), method={"name": "   "})


class TestTheCountingPass:
    def test_it_counts_rows_proteins_and_terms_per_protein(self, tmp_path: Path) -> None:
        counters = scan(str(_write(tmp_path, GOOD)), TERMS)
        assert (counters.rows, counters.proteins) == (3, 2)
        assert counters.max_terms_per_protein == 2
        assert counters.unknown_terms == 0
        assert counters.aspect_disagreements == 0

    def test_a_term_the_snapshot_lacks_is_counted_not_resolved(self, tmp_path: Path) -> None:
        body = GOOD + "P00003\tGO:9999999\tP\t0.9\n"
        counters = scan(str(_write(tmp_path, body)), TERMS)
        assert counters.rows == 4
        assert counters.unknown_terms == 1
        assert counters.unknown_examples == ["GO:9999999"]
        # And it does not count towards a protein's term tally, because it
        # cannot be scored at all.
        assert counters.proteins == 2

    def test_an_aspect_disagreement_names_both_sides(self, tmp_path: Path) -> None:
        body = HEADER + "P00001\tGO:0000001\tF\t0.5\n"
        counters = scan(str(_write(tmp_path, body)), TERMS)
        assert counters.aspect_disagreements == 1
        assert counters.aspect_examples == ["GO:0000001 file=F db=P"]

    def test_a_header_that_moved_is_refused(self, tmp_path: Path) -> None:
        body = "protein\tgo_id\tscore\taspect\nP00001\tGO:0000001\t0.5\tP\n"
        with pytest.raises(ValueError, match="declares columns"):
            scan(str(_write(tmp_path, body)), TERMS)

    def test_a_missing_header_is_refused_rather_than_sniffed(self, tmp_path: Path) -> None:
        # Sniffing would cost the first prediction of a headerless file.
        with pytest.raises(ValueError, match="declares columns"):
            scan(str(_write(tmp_path, GOOD.replace(HEADER, ""))), TERMS)


class TestTheRowsItWrites:
    def test_the_score_is_the_methods_and_the_distance_is_null(self, tmp_path: Path) -> None:
        set_id = uuid.uuid4()
        rows = list(_read_rows(str(_write(tmp_path, GOOD)), TERMS, set_id, "ens_ts44_noresim8"))
        assert len(rows) == 3
        first = rows[0]
        assert first["method_score"] == 0.632965147
        assert first["distance"] is None
        assert first["go_term_id"] == 11
        assert first["prediction_set_id"] == set_id

    def test_the_donor_column_carries_the_method(self, tmp_path: Path) -> None:
        # As predict_go_terms_from_interpro puts the IPR accession that voted
        # there: the column says what produced the row.
        rows = list(_read_rows(str(_write(tmp_path, GOOD)), TERMS, uuid.uuid4(), "the-method"))
        assert {r["ref_protein_accession"] for r in rows} == {"the-method"}

    def test_unknown_terms_are_skipped_in_the_insert_pass(self, tmp_path: Path) -> None:
        body = GOOD + "P00003\tGO:9999999\tP\t0.9\n"
        rows = list(_read_rows(str(_write(tmp_path, body)), TERMS, uuid.uuid4(), "m"))
        assert len(rows) == 3


class TestTheGatesFireBeforeAnythingIsWritten:
    def test_an_unknown_term_refuses_and_writes_nothing(self, tmp_path: Path) -> None:
        body = GOOD + "P00003\tGO:9999999\tP\t0.9\n"
        session = _Session()
        op = IngestMethodPredictionsOperation()
        with pytest.raises(ValueError, match="the snapshot does not have"):
            op.execute(session, _payload(_write(tmp_path, body)).model_dump(), emit=_emit)
        assert session.added == []
        assert session.inserted == []

    def test_an_aspect_disagreement_refuses_and_writes_nothing(self, tmp_path: Path) -> None:
        body = HEADER + "P00001\tGO:0000001\tF\t0.5\n"
        session = _Session()
        op = IngestMethodPredictionsOperation()
        with pytest.raises(ValueError, match="declare an aspect"):
            op.execute(session, _payload(_write(tmp_path, body)).model_dump(), emit=_emit)
        assert session.added == []

    def test_the_allowance_lets_a_measured_disagreement_through(self, tmp_path: Path) -> None:
        # One in 893,481 rows was measured on the lab's own knn3 dump
        # (GO:2001317), so the number is settable rather than absolute.
        body = HEADER + "P00001\tGO:0000001\tF\t0.5\n"
        session = _Session()
        op = IngestMethodPredictionsOperation()
        payload = _payload(_write(tmp_path, body), max_aspect_disagreements=1).model_dump()
        result = op.execute(session, payload, emit=_emit)
        assert result.result["aspect_disagreements"] == 1
        assert len(session.added) == 1

    def test_a_cap_the_file_exceeds_refuses(self, tmp_path: Path) -> None:
        session = _Session()
        op = IngestMethodPredictionsOperation()
        payload = _payload(_write(tmp_path, GOOD), limit_per_entry=1).model_dump()
        with pytest.raises(ValueError, match="cap of 1"):
            op.execute(session, payload, emit=_emit)
        assert session.added == []

    def test_an_empty_file_refuses(self, tmp_path: Path) -> None:
        session = _Session()
        op = IngestMethodPredictionsOperation()
        with pytest.raises(ValueError, match="no prediction rows"):
            op.execute(session, _payload(_write(tmp_path, HEADER)).model_dump(), emit=_emit)

    def test_a_wrong_digest_refuses_before_reading_the_terms(self, tmp_path: Path) -> None:
        session = _Session()
        op = IngestMethodPredictionsOperation()
        payload = _payload(_write(tmp_path, GOOD), predictions_sha256="0" * 64).model_dump()
        with pytest.raises(ValueError, match="the file is not the one"):
            op.execute(session, payload, emit=_emit)
        assert session.added == []

    def test_the_declared_digest_passes_when_it_is_the_file(self, tmp_path: Path) -> None:
        path = _write(tmp_path, GOOD)
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        session = _Session()
        op = IngestMethodPredictionsOperation()
        result = op.execute(
            session, _payload(path, predictions_sha256=digest).model_dump(), emit=_emit
        )
        assert result.result["sha256"] == digest


class TestTheConfigHasToBeTheRightKind:
    def test_a_foreign_backend_is_refused(self, tmp_path: Path) -> None:
        # Not a formality. _evaluation_leakage reads this tag as evidence that
        # the rows came from a model fitted elsewhere, and therefore demands a
        # training cut; if any config could be named here that evidence would
        # be a convention rather than a fact about how the rows were made.
        session = _Session(backend="ankh")
        op = IngestMethodPredictionsOperation()
        with pytest.raises(ValueError, match="ingested-method"):
            op.execute(session, _payload(_write(tmp_path, GOOD)).model_dump(), emit=_emit)
        assert session.added == []

    def test_the_tag_the_operation_demands_is_a_fitted_backend(self) -> None:
        # The two halves have to agree: the operation refuses anything but
        # this tag, and the guard only asks for a cut from tags it considers
        # fitted. If they drifted, an ingested arm would be scorable with no
        # cut declared at all.
        from protea.core._evaluation_leakage import FITTED_BACKENDS

        assert INGESTED_METHOD_BACKEND in FITTED_BACKENDS


class TestWhatItReportsWhenItWorks:
    def test_the_cap_it_writes_is_the_one_it_measured(self, tmp_path: Path) -> None:
        session = _Session()
        op = IngestMethodPredictionsOperation()
        result = op.execute(session, _payload(_write(tmp_path, GOOD)).model_dump(), emit=_emit)
        # limit_per_entry is NOT NULL and is a method-identity field in the
        # seal, so when the payload declares no cap the observed one is the
        # only honest value; this is why the operation reads the file twice.
        assert result.result["limit_per_entry"] == 2
        assert session.added[0].limit_per_entry == 2

    def test_the_parent_row_is_flushed_before_the_rows(self, tmp_path: Path) -> None:
        # go_prediction.prediction_set_id is a foreign key, so the first
        # chunk would be refused by the database without this.
        session = _Session()
        op = IngestMethodPredictionsOperation()
        op.execute(session, _payload(_write(tmp_path, GOOD)).model_dump(), emit=_emit)
        assert session.flushes >= 1

    def test_the_provenance_lands_verbatim_under_one_meta_key(self, tmp_path: Path) -> None:
        # One key, not eleven: method_seal._CLASSIFIED_META refuses unknown
        # meta keys by name, which is what already happens to the
        # meta["algorithm"] of predict_go_terms_from_interpro.
        session = _Session()
        op = IngestMethodPredictionsOperation()
        op.execute(session, _payload(_write(tmp_path, GOOD)).model_dump(), emit=_emit)
        meta = session.added[0].meta
        assert meta["method"] == METHOD
        assert meta["source"]["rows"] == 3

    def test_it_says_which_formula_scores_it(self, tmp_path: Path) -> None:
        # Under any other formula a method row has no signal with a weight
        # and the cell comes out zero, which reads as a method performing
        # badly rather than as a configuration error.
        session = _Session()
        op = IngestMethodPredictionsOperation()
        result = op.execute(session, _payload(_write(tmp_path, GOOD)).model_dump(), emit=_emit)
        assert "method_score" in result.result["caveat"]

    def test_every_scorable_row_lands_and_none_is_a_repeat(self, tmp_path: Path) -> None:
        session = _Session()
        op = IngestMethodPredictionsOperation()
        result = op.execute(session, _payload(_write(tmp_path, GOOD)).model_dump(), emit=_emit)
        assert result.result["inserted"] == 3
        assert session.inserted == [3]
        # The difference between the scorable rows and the rows that landed is
        # the only place a dump carrying the same (protein, term) twice shows,
        # because the insert leans on the unique constraint instead of holding
        # five million pairs in memory to look for one.
        assert result.result["repeated_pairs"] == 0

    def test_the_chunk_size_is_what_splits_the_insert(self, tmp_path: Path) -> None:
        session = _Session()
        op = IngestMethodPredictionsOperation()
        payload = _payload(_write(tmp_path, GOOD), chunk_size=2).model_dump()
        result = op.execute(session, payload, emit=_emit)
        assert session.inserted == [2, 1]
        assert result.result["inserted"] == 3

    def test_the_summary_names_the_method(self) -> None:
        op = IngestMethodPredictionsOperation()
        assert "ens_ts44_noresim8" in op.summarize_payload({"method": METHOD})
