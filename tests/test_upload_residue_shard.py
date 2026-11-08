"""The residue-code shard upload as a stage of its own on the node (ADR-D51, 2026-11-03).

WHY IT EXISTS. A batch of the corpus pass put its shard (about 25 MB) in the artifact store
from the card's own loop: 35 % of every batch went to that put, with the card idle. With a
spool folder the batch writes the shard to the node's disk and a second worker puts it in
the store while the card encodes the next batch.

WHAT MUST NOT CHANGE. The server indexes a shard only once the store holds it; a batch is
acknowledged only once its shard is safe on disk and its message published; a node with no
spool behaves exactly as before. These tests hold the stages to that, with the real local
artifact store in a temporary folder and codes built by hand.
"""

from __future__ import annotations

import hashlib
import uuid
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import numpy as np
import pytest
from pydantic import ValidationError

from protea.core.operations import _residue_shard_spool as spool
from protea.core.operations import encode_residue_vocabulary as encode
from protea.core.operations import upload_residue_shard as upload
from protea.core.operations.store_residue_codes import WRITE_QUEUE, StoreResidueCodesPayload
from protea.core.residue_vocabulary import ResidueCode
from protea.infrastructure.storage.local import LocalFsArtifactStore

TARGET = uuid.UUID("22166300-a178-599e-835c-afe1c007feb5")
HEADER = {"target_embedding_config_id": str(TARGET), "vocabulary": "test"}


def _codes() -> list[tuple[int, ResidueCode]]:
    """Two sequences of 3 and 2 residues, each residue with two active features."""
    out = []
    for sequence_id, residues in [(100, 3), (101, 2)]:
        features = np.tile(np.array([4, 9], dtype=np.int16), residues)
        levels = np.ones(2 * residues, dtype=np.uint8)
        offsets = np.arange(0, 2 * residues + 1, 2, dtype=np.int64)
        out.append((sequence_id, ResidueCode(features, levels, offsets)))
    return out


class Events:
    def __init__(self) -> None:
        self.seen: list[tuple[str, dict[str, Any]]] = []

    def __call__(self, event: str, message: Any, fields: dict[str, Any], level: str) -> None:
        self.seen.append((event, fields))


# The spool ----------------------------------------------------------------------------------


def test_a_shard_key_names_a_file_inside_the_spool(tmp_path: Path) -> None:
    key = f"residue-codes/{TARGET}/100-101-0123456789abcdef.bin"
    assert spool.spooled_path(tmp_path, key) == tmp_path / "residue-codes" / str(TARGET) / (
        "100-101-0123456789abcdef.bin"
    )


@pytest.mark.parametrize(
    "key",
    [
        "residue-codes/../../home/user/.secrets",
        "/residue-codes/x.bin",
        "model-weights/v.npz",
        "residue-codes",
    ],
)
def test_a_key_that_could_leave_the_spool_is_refused(tmp_path: Path, key: str) -> None:
    with pytest.raises(ValueError, match="not a residue-code shard key"):
        spool.spooled_path(tmp_path, key)


def test_a_spooled_file_is_whole_and_leaves_no_partial_file(tmp_path: Path) -> None:
    path = tmp_path / "residue-codes" / "t" / "a.bin"
    spool.write_spooled(path, b"shard bytes")
    assert path.read_bytes() == b"shard bytes"
    assert [p.name for p in path.parent.iterdir()] == ["a.bin"]
    assert spool.spool_backlog(tmp_path) == (1, len(b"shard bytes"))


def test_a_spool_without_room_refuses_before_the_card_is_spent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(spool.shutil, "disk_usage", lambda p: SimpleNamespace(free=3 * 2**30))
    with pytest.raises(RuntimeError, match="protea.embeddings.upload running"):
        spool.refuse_without_room(tmp_path, 20.0)
    spool.refuse_without_room(tmp_path, 2.0)


def test_the_spool_comes_from_the_node_tuning(monkeypatch: pytest.MonkeyPatch) -> None:
    from protea.config.tuning import get_tuning

    monkeypatch.setenv("PROTEA_TUNING__operation__residue_shard_spool_dir", "/srv/spool")
    get_tuning.cache_clear()
    try:
        assert spool.spool_root() == Path("/srv/spool")
    finally:
        monkeypatch.delenv("PROTEA_TUNING__operation__residue_shard_spool_dir")
        get_tuning.cache_clear()
    assert spool.spool_root() is None


# The batch's hand-off ------------------------------------------------------------------------


def test_with_no_spool_the_card_puts_the_shard_and_messages_the_server(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = LocalFsArtifactStore(tmp_path / "store")
    monkeypatch.setattr(encode, "_artifact_store", lambda: store)
    key, sha, nbytes, (queue, message) = encode.hand_off(None, TARGET, _codes(), HEADER, "job-1")
    assert store.exists(key) and len(store.get(key)) == nbytes
    assert (queue, message["operation"]) == (WRITE_QUEUE, "store_residue_codes")
    assert message["payload"]["shard_sha256"] == sha


def test_with_a_spool_the_shard_waits_on_disk_and_the_message_goes_to_the_upload_stage(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = LocalFsArtifactStore(tmp_path / "store")
    monkeypatch.setattr(encode, "_artifact_store", lambda: store)
    root = tmp_path / "spool"
    codes = _codes()
    key, sha, nbytes, (queue, message) = encode.hand_off(root, TARGET, codes, HEADER, "job-1")
    assert not store.exists(key), "the card put nothing in the store"
    blob = spool.spooled_path(root, key).read_bytes()
    assert len(blob) == nbytes and hashlib.sha256(blob).hexdigest() == sha
    assert (queue, message["operation"], message["job_id"]) == (
        spool.UPLOAD_QUEUE,
        "upload_residue_shard",
        "job-1",
    )
    _q, server = encode.build_store_message("job-1", TARGET, key, sha, codes)
    assert message["payload"] == {"shard_key": key, "shard_sha256": sha, "store": server["payload"]}


def test_both_paths_write_the_same_shard(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The spool changes where the shard waits, not what it is or what the server receives."""
    store = LocalFsArtifactStore(tmp_path / "store")
    monkeypatch.setattr(encode, "_artifact_store", lambda: store)
    inline = encode.hand_off(None, TARGET, _codes(), HEADER, "job-1")
    spooled = encode.hand_off(tmp_path / "spool", TARGET, _codes(), HEADER, "job-1")
    assert inline[:3] == spooled[:3]
    assert spooled[3][1]["payload"]["store"] == inline[3][1]["payload"]


# The upload stage ----------------------------------------------------------------------------


@pytest.fixture
def node(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> SimpleNamespace:
    """A node with a spool holding one shard, and an empty artifact store."""
    state = SimpleNamespace(store=LocalFsArtifactStore(tmp_path / "store"), root=tmp_path / "spool")
    monkeypatch.setattr(encode, "_artifact_store", lambda: state.store)
    monkeypatch.setattr(upload, "_artifact_store", lambda: state.store)
    monkeypatch.setattr(upload, "spool_root", lambda: state.root)
    key, sha, _n, (_q, message) = encode.hand_off(state.root, TARGET, _codes(), HEADER, "job-1")
    state.key, state.sha, state.message = key, sha, message
    state.path = spool.spooled_path(state.root, key)
    return state


def _run(node: SimpleNamespace, payload: dict[str, Any] | None = None) -> Any:
    events = Events()
    result = upload.UploadResidueShardOperation().execute(
        None,  # type: ignore[arg-type]  # the stage reads no database
        payload or node.message["payload"],
        emit=events,
    )
    return result, events


def test_the_upload_puts_the_shard_then_hands_the_server_the_batchs_message(
    node: SimpleNamespace,
) -> None:
    result, events = _run(node)
    assert hashlib.sha256(node.store.get(node.key)).hexdigest() == node.sha
    assert not node.path.exists(), "the spooled file is removed once the store holds it"
    ((queue, message),) = result.publish_operations
    assert (queue, message["operation"], message["job_id"]) == (
        WRITE_QUEUE,
        "store_residue_codes",
        "job-1",
    )
    assert message["payload"] == node.message["payload"]["store"]
    StoreResidueCodesPayload.model_validate(message["payload"])
    ((name, fields),) = events.seen
    assert name == "upload.done" and fields["bytes"] > 0


def test_a_delivery_after_an_earlier_put_publishes_again_without_a_file(
    node: SimpleNamespace,
) -> None:
    """An earlier delivery put the shard, removed the file and died before its acknowledgement."""
    _run(node)
    result, events = _run(node)
    ((queue, message),) = result.publish_operations
    assert queue == WRITE_QUEUE and message["payload"]["shard_key"] == node.key
    assert events.seen[0][1]["bytes"] == 0


def test_a_shard_neither_spooled_nor_stored_fails_and_publishes_nothing(
    node: SimpleNamespace,
) -> None:
    node.path.unlink()
    with pytest.raises(ValueError, match="must be encoded again"):
        _run(node)


def test_a_spooled_file_that_is_not_the_batchs_shard_is_refused(node: SimpleNamespace) -> None:
    node.path.write_bytes(b"something else")
    with pytest.raises(ValueError, match="the batch wrote"):
        _run(node)
    assert not node.store.exists(node.key)


def test_a_node_without_a_spool_refuses_the_upload(
    node: SimpleNamespace, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(upload, "spool_root", lambda: None)
    with pytest.raises(ValueError, match="sets no operation.residue_shard_spool_dir"):
        _run(node)


def test_the_message_to_the_server_must_name_the_uploaded_shard(node: SimpleNamespace) -> None:
    payload = dict(node.message["payload"])
    payload["store"] = {**payload["store"], "shard_key": "residue-codes/other/1-2-ab.bin"}
    with pytest.raises(ValidationError, match="this upload is for"):
        _run(node, payload)
    assert node.path.exists() and not node.store.exists(node.key)


# Wiring --------------------------------------------------------------------------------------


def test_the_stage_is_registered_and_its_queue_carries_operation_messages() -> None:
    from protea.core.operation_catalog import build_operation_registry

    assert build_operation_registry().get("upload_residue_shard").name == "upload_residue_shard"
    worker = (Path(__file__).resolve().parents[1] / "scripts" / "worker.py").read_text()
    queues = worker.split("_OPERATION_QUEUES = {", 1)[1].split("}", 1)[0]
    assert f'"{spool.UPLOAD_QUEUE}"' in queues
