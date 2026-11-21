"""Each figure on a window page names the file it was read from and that file's digest.

An evaluation recorded the keys of the files it wrote and nothing about their
bytes, so a figure on the window page could be traced to its result, its job and
the code that ran, but not checked against the file it came from. The evaluator
now records the sha256 of every file as it uploads it; one route serves a single
recorded file with that digest in a header (tests/test_annotations_router.py,
``TestDownloadOneEvaluationArtifact``), and the window page names the file and
the digest behind each cell.
"""

from __future__ import annotations

import hashlib
import uuid
from pathlib import Path
from typing import Any

from protea.core.operations._run_cafa_helpers import artifacts_record, eval_artifact_key
from protea.core.operations.run_cafa_evaluation import RunCafaEvaluationOperation


class _Store:
    """Keeps what was put, as bytes, under its key."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    def put(self, key: str, src: Path | bytes) -> str:
        self.objects[key] = src.read_bytes() if isinstance(src, Path) else src
        return key


def _quiet(*_a: Any, **_k: Any) -> None:
    return None


def _written(root: Path) -> None:
    (root / "PK").mkdir(parents=True)
    (root / "PK" / "evaluation_best_f_micro_w.tsv").write_text("ns\tf_micro_w\nBPO\t0.0981\n")
    (root / "PK" / "per_protein_grid.parquet").write_bytes(b"PAR1 not really")
    (root / "terms_of_interest.txt").write_text("GO:0008150\n")


class TestTheEvaluatorRecordsWhatItWrote:
    def test_each_key_carries_the_sha256_of_the_bytes_uploaded_under_it(
        self, tmp_path: Path
    ) -> None:
        _written(tmp_path)
        store, result_id = _Store(), uuid.uuid4()

        uploaded = RunCafaEvaluationOperation._upload_artifacts(store, result_id, tmp_path, _quiet)

        assert set(uploaded) == set(store.objects)
        assert eval_artifact_key(result_id, "PK/evaluation_best_f_micro_w.tsv") in uploaded
        for key, digest in uploaded.items():
            assert digest == hashlib.sha256(store.objects[key]).hexdigest()

    def test_the_record_keeps_the_keys_its_readers_walk(self, tmp_path: Path) -> None:
        _written(tmp_path)
        uploaded = RunCafaEvaluationOperation._upload_artifacts(
            _Store(), uuid.uuid4(), tmp_path, _quiet
        )

        record = artifacts_record(uploaded)

        assert record["keys"] == list(uploaded)
        assert record["sha256"] == uploaded
