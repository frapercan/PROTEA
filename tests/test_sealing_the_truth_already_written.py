"""Sealing an already-uploaded truth, and saying the seal is the weaker one.

WHY IT EXISTS. The generator hashes the local file before the upload, so its
digest certifies what the delta computation produced. Every window written
before that column existed has none: 133 of them, plus the 63 of the (A)
series, which are the training data.

AND WHY THE SEAL IS WEAKER, which these tests pin down because the column
would otherwise be read as stronger than it is: this operation hashes what the
store returns NOW, so it certifies the bytes from the moment of sealing
onwards. An object already replaced before the seal gets recorded as
authentic. It closes the future, not the past.

IT NEVER DESERIALISES, and that is a rule with a name: ``cb46395a`` is the
holdout, its single declared look was spent in the laboratory on 2026-10-26,
and parsing its truth inside the platform would be a second read of the one
artefact the waiver protects. Hashing bytes is not reading a truth.
"""

from __future__ import annotations

import hashlib
import inspect
import uuid

import pytest

from protea.core._artifact_identity import ArtifactMismatch
from protea.core.operations.seal_evaluation_groundtruth import (
    SealEvaluationGroundtruthOperation,
    SealEvaluationGroundtruthPayload,
)

BLOB = b"PAR1-truth-bytes"
SHA = hashlib.sha256(BLOB).hexdigest()


class _Eset:
    def __init__(self, uri: str | None = "s3://p/k", sha: str | None = None) -> None:
        self.id = uuid.uuid4()
        self.groundtruth_uri = uri
        self.groundtruth_sha256 = sha


class _Session:
    def __init__(self, rows: dict[uuid.UUID, _Eset]) -> None:
        self.rows = rows

    def get(self, _model, ident):
        return self.rows.get(ident)


class _Store:
    def __init__(self, blob: bytes = BLOB) -> None:
        self.blob = blob
        self.asked: list[str] = []

    def get(self, key: str) -> bytes:
        self.asked.append(key)
        return self.blob


def _run(session, store, ids, monkeypatch):
    op = SealEvaluationGroundtruthOperation()
    monkeypatch.setattr(
        "protea.core.operations.seal_evaluation_groundtruth.get_artifact_store",
        lambda _s: store,
    )
    monkeypatch.setattr(
        "protea.core.operations.seal_evaluation_groundtruth.load_settings",
        lambda _r: object(),
    )
    eventos: list[tuple[str, str]] = []

    def emit(event, _p, fields, level):
        eventos.append((event, level))
        return fields

    out = op.execute(session, {"evaluation_set_ids": ids}, emit=emit)
    return out.result, eventos


class TestTheScopeIsNamed:
    def test_an_empty_list_is_refused(self) -> None:
        # No "all the unsealed ones" mode: that default would have reached
        # the holdout without anybody typing its id.
        with pytest.raises(ValueError, match="cannot be empty"):
            SealEvaluationGroundtruthPayload.model_validate({"evaluation_set_ids": []})

    def test_an_id_that_is_not_a_window_raises(self, monkeypatch) -> None:
        with pytest.raises(ValueError, match="does not exist"):
            _run(_Session({}), _Store(), [str(uuid.uuid4())], monkeypatch)


class TestWhatItWrites:
    def test_an_unsealed_row_gets_the_digest_of_what_the_store_returns(self, monkeypatch) -> None:
        e = _Eset()
        res, eventos = _run(_Session({e.id: e}), _Store(), [str(e.id)], monkeypatch)
        assert e.groundtruth_sha256 == SHA
        assert res["sealed"] == 1 and res["asked"] == 1
        assert ("seal_evaluation_groundtruth.sealed", "info") in eventos

    def test_a_row_already_sealed_with_the_same_digest_is_left_alone(self, monkeypatch) -> None:
        e = _Eset(sha=SHA)
        res, _ = _run(_Session({e.id: e}), _Store(), [str(e.id)], monkeypatch)
        assert res == {"sealed": 0, "already_sealed": 1, "without_artifact": 0, "asked": 1}

    def test_a_changed_object_is_a_finding_and_not_an_update(self, monkeypatch) -> None:
        # Overwriting the row would erase the evidence that the bytes moved.
        e = _Eset(sha=hashlib.sha256(b"otros").hexdigest())
        with pytest.raises(ArtifactMismatch, match="changed after it was sealed"):
            _run(_Session({e.id: e}), _Store(), [str(e.id)], monkeypatch)
        assert e.groundtruth_sha256 != SHA

    def test_a_row_with_no_artifact_warns_and_the_run_continues(self, monkeypatch) -> None:
        sin, con = _Eset(uri=None), _Eset()
        session = _Session({sin.id: sin, con.id: con})
        res, eventos = _run(session, _Store(), [str(sin.id), str(con.id)], monkeypatch)
        assert res["without_artifact"] == 1 and res["sealed"] == 1
        assert ("seal_evaluation_groundtruth.no_artifact", "warning") in eventos

    def test_the_key_comes_from_the_id_and_not_from_the_stored_uri(self, monkeypatch) -> None:
        # Same rule as the reader: the derived key is the locator, and the
        # stored uri is not trusted to name the object by itself.
        e = _Eset(uri="s3://protea/somewhere/else.parquet")
        store = _Store()
        _run(_Session({e.id: e}), store, [str(e.id)], monkeypatch)
        assert store.asked == [f"eval_groundtruth/{e.id}/groundtruth.parquet"]


class TestItNeverReadsTheTruth:
    def test_the_operation_does_not_deserialise(self) -> None:
        # The holdout rule, as a check on the code rather than a promise in a
        # docstring. Hashing bytes is not reading a truth.
        source = inspect.getsource(SealEvaluationGroundtruthOperation)
        for prohibido in ("deserialize", "load_evaluation_data_for_set", "read_parquet"):
            assert prohibido not in source, prohibido

    def test_the_weaker_seal_is_written_down(self) -> None:
        # If the module does not say it, the column will be read as stronger
        # than it is: this digest certifies the bytes from the moment of
        # sealing, not what the generator produced.
        import protea.core.operations.seal_evaluation_groundtruth as mod

        doc = mod.__doc__ or ""
        assert "WEAKER" in doc and "MOMENT OF SEALING" in doc.upper()
