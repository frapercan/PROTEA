"""Unit tests for the F-EXPORT-MINIJOB.4 assembly stage.

Covers :class:`ExportWriteOperation`:

* Non-terminal delivery records a ``pair_write_received`` ``JobEvent``
  and bumps the parent's ``progress_current`` but does NOT upload or
  insert.
* Terminal delivery (the one that brings ``progress_current ==
  progress_total``) concatenates every per-pair shard into
  ``train.parquet`` + ``eval.parquet``, builds + uploads
  ``manifest.json``, inserts the ``Dataset`` row, emits
  ``dataset.created`` + ``pair_write_done``, and transitions the
  parent to ``SUCCEEDED``.
"""

from __future__ import annotations

import json
import tempfile
import uuid
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from protea.core.operations.export_minijobs._export_write import (
    ExportWriteOperation,
    ExportWritePayload,
    _ShardEntry,
    _stage_concat,
)

# ---------------------------------------------------------------------------
# Stubs
# ---------------------------------------------------------------------------


class _StubStore:
    """In-memory ArtifactStore stub: records ``put``/``get``/``delete`` calls."""

    def __init__(self) -> None:
        self._data: dict[str, bytes] = {}
        self.puts: list[tuple[str, bytes]] = []
        self.deletes: list[str] = []

    def put(self, key: str, src: Path | bytes) -> str:
        if isinstance(src, (str, Path)):
            payload = Path(src).read_bytes()
        else:
            payload = src
        self._data[key] = payload
        self.puts.append((key, payload))
        return f"s3://test-bucket/{key}"

    def get(self, key: str) -> bytes:
        return self._data[key]

    def delete(self, key: str) -> bool:
        self.deletes.append(key)
        self._data.pop(key, None)
        return True

    def exists(self, key: str) -> bool:
        return key in self._data

    def url(self, key: str) -> str:
        return f"s3://test-bucket/{key}"


def _make_shard(n_rows: int, label: int, columns: list[str] | None = None) -> bytes:
    """Build a per-pair feature parquet shard as in-memory bytes."""
    cols = columns or ["protein_accession", "go_term_id", "label", "feat_a", "feat_b"]
    data: dict[str, Any] = {}
    for c in cols:
        if c == "label":
            data[c] = [label] * n_rows
        elif c == "protein_accession":
            data[c] = [f"P{i:05d}" for i in range(n_rows)]
        elif c == "go_term_id":
            data[c] = [f"GO:000{(i % 9) + 1}" for i in range(n_rows)]
        else:
            data[c] = [float(i) for i in range(n_rows)]
    df = pd.DataFrame(data)
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        path = Path(tmp.name)
    try:
        df.to_parquet(str(path), index=False)
        return path.read_bytes()
    finally:
        path.unlink(missing_ok=True)


def _noop_emit(*_args: Any, **_kwargs: Any) -> None:
    pass


def _emit_recorder() -> tuple[list[tuple[str, Any, dict[str, Any], str]], Any]:
    emitted: list[tuple[str, Any, dict[str, Any], str]] = []

    def _emit(event: str, message: Any, fields: dict[str, Any], level: str) -> None:
        emitted.append((event, message, fields, level))

    return emitted, _emit


def _base_payload(
    *,
    pair_id: str,
    is_eval: bool,
    coord_id: str,
    output_name: str = "bench-v1-K5-v226-lineage",
    temp_uri: str = "s3://test-bucket/temp/coordinator/x/features/y.parquet",
    n_rows: int = 10,
) -> dict[str, Any]:
    return {
        "coordinator_job_id": coord_id,
        "pair_id": pair_id,
        "temp_features_uri": temp_uri,
        "n_rows": n_rows,
        "output_name": output_name,
        "embedding_config_id": str(uuid.uuid4()),
        "ontology_snapshot_id": str(uuid.uuid4()),
        "annotation_set_id": str(uuid.uuid4()),
        "annotation_source": "goa",
        "k": 5,
        "is_eval": is_eval,
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestPayloadValidation:
    def test_minimal_payload_validates(self) -> None:
        payload = ExportWritePayload.model_validate(
            {
                "coordinator_job_id": str(uuid.uuid4()),
                "pair_id": "train-220",
                "temp_features_uri": "s3://x/y.parquet",
            }
        )
        assert payload.is_eval is False
        assert payload.k == 5

    def test_required_fields_enforced(self) -> None:
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            ExportWritePayload.model_validate({})


class TestStageConcat:
    """Pure helper test: deterministic concat + train/eval split."""

    def test_concat_preserves_row_count_and_splits(self) -> None:
        store = _StubStore()
        store._data["temp/coordinator/abc/features/train-220.parquet"] = _make_shard(3, label=1)
        store._data["temp/coordinator/abc/features/train-221.parquet"] = _make_shard(5, label=1)
        store._data["temp/coordinator/abc/features/eval-222.parquet"] = _make_shard(2, label=0)

        shards = [
            _ShardEntry(
                pair_id="train-220",
                temp_uri="s3://test-bucket/temp/coordinator/abc/features/train-220.parquet",
                is_eval=False,
                n_rows=3,
            ),
            _ShardEntry(
                pair_id="train-221",
                temp_uri="s3://test-bucket/temp/coordinator/abc/features/train-221.parquet",
                is_eval=False,
                n_rows=5,
            ),
            _ShardEntry(
                pair_id="eval-222",
                temp_uri="s3://test-bucket/temp/coordinator/abc/features/eval-222.parquet",
                is_eval=True,
                n_rows=2,
            ),
        ]

        train_df, eval_df, train_pairs, eval_pair = _stage_concat(store, shards)

        assert len(train_df) == 8
        assert len(eval_df) == 2
        assert train_pairs == ["train-220", "train-221"]
        assert eval_pair == "eval-222"
        # snapshot_pair stamped on every row
        assert set(train_df["snapshot_pair"].unique()) == {"train-220", "train-221"}
        assert set(eval_df["snapshot_pair"].unique()) == {"eval-222"}


class TestNonTerminalDelivery:
    """Earlier deliveries record + increment but do not assemble."""

    def test_records_event_and_increments_progress_only(self) -> None:
        coord_id = str(uuid.uuid4())
        op = ExportWriteOperation()
        session = MagicMock()
        # Increment returns row showing 1 of 3 done.
        row = MagicMock()
        row.progress_current = 1
        row.progress_total = 3
        session.execute.return_value.fetchone.return_value = row

        emitted, emit = _emit_recorder()
        result = op.execute(
            session,
            _base_payload(pair_id="train-220", is_eval=False, coord_id=coord_id),
            emit=emit,
        )

        assert result.result["assembled"] is False
        # session.add was used to record the JobEvent (not the Dataset).
        assert session.add.call_count == 1
        # No dataset.created / pair_write_done emitted yet.
        names = [e[0] for e in emitted]
        assert "dataset.created" not in names
        assert "pair_write_done" not in names
        # No publish_operations follow the write stage.
        assert result.publish_operations == []


class TestTerminalDelivery:
    """The delivery that brings progress to total runs the assembly path."""

    def _build_three_pair_session(
        self, coord_id: str, store: _StubStore, train_pairs: list[str], eval_pair: str
    ) -> tuple[MagicMock, list[MagicMock]]:
        """Wire a session whose ``execute`` walks through the three SQL calls
        the operation makes on the terminal delivery: increment, load event
        rows, finalize parent.
        """
        added: list[Any] = []

        # Stage shards into the stub store first.
        from protea.core.operations.export_minijobs._export_features_batch import _uri_to_key

        ledger_rows: list[tuple[dict[str, Any]]] = []
        for pid in train_pairs:
            key = f"temp/coordinator/{coord_id}/features/{pid}.parquet"
            store._data[key] = _make_shard(4, label=1)
            ledger_rows.append(
                (
                    {
                        "pair_id": pid,
                        "temp_uri": f"s3://test-bucket/{key}",
                        "is_eval": False,
                        "n_rows": 4,
                    },
                )
            )
        eval_key = f"temp/coordinator/{coord_id}/features/{eval_pair}.parquet"
        store._data[eval_key] = _make_shard(3, label=0)
        ledger_rows.append(
            (
                {
                    "pair_id": eval_pair,
                    "temp_uri": f"s3://test-bucket/{eval_key}",
                    "is_eval": True,
                    "n_rows": 3,
                },
            )
        )

        # We do NOT include the terminal-delivery's own pair_id in the ledger
        # to assert dedup logic; the terminal delivery's session.add(JobEvent)
        # is mocked away (session.add is a MagicMock).
        del _uri_to_key  # silence linter

        increment_row = MagicMock()
        increment_row.progress_current = len(train_pairs) + 1
        increment_row.progress_total = len(train_pairs) + 1
        finalize_row = MagicMock()

        call_sequence: list[Any] = [
            increment_row,  # _increment_parent_progress
            ledger_rows,  # _load_all_pair_shards (.all())
            finalize_row,  # _finalize_parent close UPDATE
        ]

        execute_calls: list[MagicMock] = []

        def _execute(*_args: Any, **_kwargs: Any) -> MagicMock:
            stmt_mock = MagicMock()
            value = call_sequence.pop(0)
            if isinstance(value, list):
                stmt_mock.all.return_value = value
            else:
                stmt_mock.fetchone.return_value = value
            execute_calls.append(stmt_mock)
            return stmt_mock

        session = MagicMock()
        session.execute.side_effect = _execute
        session.add.side_effect = added.append  # capture Dataset + JobEvent inserts
        session.flush = MagicMock()
        session._added = added  # for assertions
        return session, execute_calls

    def test_terminal_delivery_assembles_uploads_inserts_emits(self, tmp_path: Path) -> None:
        coord_id = str(uuid.uuid4())
        store = _StubStore()
        session, _calls = self._build_three_pair_session(
            coord_id, store, train_pairs=["train-220", "train-221"], eval_pair="eval-222"
        )

        # Stub settings: storage_backend + load_settings + get_artifact_store.
        fake_settings = MagicMock()
        fake_settings.storage_backend = "local"

        with patch(
            "protea.core.operations.export_minijobs._export_write.load_settings",
            return_value=fake_settings,
        ), patch(
            "protea.core.operations.export_minijobs._export_write.get_artifact_store",
            return_value=store,
        ):
            emitted, emit = _emit_recorder()
            op = ExportWriteOperation()
            result = op.execute(
                session,
                _base_payload(pair_id="eval-222", is_eval=True, coord_id=coord_id),
                emit=emit,
            )

        # Assembly happened: result has dataset metadata.
        assert result.result["n_train_rows"] == 8  # 2 * 4
        assert result.result["n_eval_rows"] == 3
        assert result.result["train_uri"].endswith("/train.parquet")
        assert result.result["eval_uri"].endswith("/eval.parquet")
        assert result.result["manifest_uri"].endswith("/manifest.json")
        assert result.result["storage_backend"] == "local"
        # Terminal write op publishes no follow-on operations.
        assert result.publish_operations == []

        # Store received train + eval + manifest uploads.
        put_keys = [k for k, _ in store.puts]
        assert any(k.endswith("/train.parquet") for k in put_keys)
        assert any(k.endswith("/eval.parquet") for k in put_keys)
        assert any(k.endswith("/manifest.json") for k in put_keys)

        # Manifest content correct.
        manifest_bytes = [v for k, v in store.puts if k.endswith("/manifest.json")][-1]
        manifest = json.loads(manifest_bytes)
        assert manifest["name"] == "bench-v1-K5-v226-lineage"
        assert manifest["k"] == 5
        assert manifest["annotation_source"] == "goa"
        assert manifest["train_snapshot_pairs"] == ["train-220", "train-221"]
        assert manifest["eval_snapshot_pair"] == "eval-222"
        assert manifest["n_train_rows"] == 8
        assert manifest["n_eval_rows"] == 3

        # Dataset row added: walk session.add captures for a Dataset instance.
        from protea.infrastructure.orm.models.embedding.dataset import Dataset

        datasets = [a for a in session._added if isinstance(a, Dataset)]
        assert len(datasets) == 1
        ds = datasets[0]
        assert ds.name == "bench-v1-K5-v226-lineage"
        assert ds.operation == "export_research_dataset"
        assert ds.n_train_rows == 8
        assert ds.n_eval_rows == 3
        assert ds.k == 5
        assert ds.annotation_source == "goa"
        assert ds.train_snapshot_pairs == ["train-220", "train-221"]
        assert ds.eval_snapshot_pair == "eval-222"
        assert ds.storage_backend == "local"
        assert ds.manifest_uri.endswith("/manifest.json")

        # Events: dataset.created + pair_write_done both emitted.
        names = [e[0] for e in emitted]
        assert "dataset.created" in names
        assert "pair_write_done" in names
        # Parent transition event.
        assert "export_write.parent_succeeded" in names

        # Cleanup attempted on per-pair temp shards.
        # 3 pairs in ledger → 3 deletes (best-effort).
        assert len(store.deletes) == 3
