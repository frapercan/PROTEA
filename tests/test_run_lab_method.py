"""run_lab_method: the gates before the card is spent, the run, and the child ingestion job.

The container is replaced by a small Python program that plays the method: it reads the query
file the operation wrote, writes a TSV and the JSON record beside it, and either tells the truth
about them or does not. podman and the card are replaced by functions the tests set.
"""

from __future__ import annotations

import hashlib
import sys
import time
import uuid
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from pydantic import ValidationError

from protea.core.contracts.operation import RetryLaterError
from protea.core.operations import _card_guard
from protea.core.operations import run_lab_method as rlm
from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
from protea.infrastructure.orm.models.embedding.embedding_config import (
    INGESTED_METHOD_BACKEND,
    EmbeddingConfig,
)
from protea.infrastructure.orm.models.job import Job, JobEvent

IMAGE_ID = "d" * 64
FASTA = b">P1\nMKV\n>P2\nMAL\n"
FASTA_SHA = hashlib.sha256(FASTA).hexdigest()
FASTA_KEY = "evaluation-targets/queries-0123456789ab.fasta"

METHOD = r"""
import hashlib, json, sys
from pathlib import Path
work, name, mode = Path(sys.argv[1]), sys.argv[2], sys.argv[3]
query = (work / "in" / "queries.fasta").read_bytes()
tsv = b"protein\tgo_id\taspect\tscore\nP1\tGO:0005515\tF\t0.5\n"
(work / "out" / "predictions.tsv").write_bytes(tsv)
record = {"name": name, "cut": 211, "query_file": ["/app/data/queries.fasta", hashlib.sha256(query).hexdigest()],
          "predictions": {"sha256": hashlib.sha256(tsv).hexdigest()}}
if mode == "lies":
    record["predictions"]["sha256"] = "0" * 64
if mode != "silent":
    (work / "out" / "predictions.json").write_text(json.dumps(record))
print("the method ran on", len(query), "bytes of queries")
"""


class FakeStore:
    def __init__(self, objects: dict[str, bytes]) -> None:
        self.objects = dict(objects)

    def get(self, key: str) -> bytes:
        return self.objects[key]

    def put(self, key: str, src: Path | bytes) -> str:
        self.objects[key] = src.read_bytes() if isinstance(src, Path) else bytes(src)
        return f"mem://{key}"


class FakeSession:
    def __init__(self, rows: dict[tuple[type, uuid.UUID], Any]) -> None:
        self.rows = rows
        self.added: list[Any] = []

    def get(self, model: type, key: uuid.UUID) -> Any:
        return self.rows.get((model, key))

    def add(self, obj: Any) -> None:
        self.added.append(obj)

    def flush(self) -> None:
        pass


def _arm(cut: str | None = "211", start: str = "211", backend: str = INGESTED_METHOD_BACKEND):
    """A session holding one method configuration, its cut and the window's first release."""
    config_id, cut_id, start_id = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    rows: dict[tuple[type, uuid.UUID], Any] = {
        (EmbeddingConfig, config_id): SimpleNamespace(
            model_backend=backend, trained_on_annotation_set_id=cut_id if cut else None
        ),
        (AnnotationSet, start_id): SimpleNamespace(source_version=start),
    }
    if cut:
        rows[(AnnotationSet, cut_id)] = SimpleNamespace(source_version=cut)
    return FakeSession(rows), config_id, start_id


def _payload(config_id: uuid.UUID, start_id: uuid.UUID, **over: Any) -> dict[str, Any]:
    base = {
        "method_name": "lab-reader-cut211",
        "image": "localhost/lab:dev",
        "image_id": IMAGE_ID,
        "bundle_path": "/srv/bundle",
        "bundle_mount": "/opt/bundle",
        "cut": 211,
        "queries_fasta_uri": FASTA_KEY,
        "queries_fasta_sha256": FASTA_SHA,
        "embedding_config_id": str(config_id),
        "annotation_set_id": str(start_id),
        "ontology_snapshot_id": str(uuid.uuid4()),
    }
    return {**base, **over}


def _model(**over: Any) -> rlm.RunLabMethodPayload:
    return rlm.RunLabMethodPayload.model_validate(_payload(uuid.uuid4(), uuid.uuid4(), **over))


class Events:
    def __init__(self) -> None:
        self.seen: list[tuple[str, dict[str, Any]]] = []

    def __call__(self, event: str, message: Any, fields: dict[str, Any], level: str) -> None:
        self.seen.append((event, fields))

    def named(self, event: str) -> list[dict[str, Any]]:
        return [fields for name, fields in self.seen if name == event]


def _play(monkeypatch: pytest.MonkeyPatch, program: str, *args: str) -> None:
    monkeypatch.setattr(
        rlm,
        "container_command",
        lambda p, work, name: [sys.executable, "-c", program, str(work), *args],
    )


# The container call ------------------------------------------------------------------------


def test_the_container_runs_the_pinned_id_with_no_network_and_the_bundle_read_only() -> None:
    p = _model()
    command = rlm.container_command(p, Path("/scratch/run"), "protea-method-x")
    assert command[:2] == ["podman", "run"]
    assert command[command.index("--network") + 1] == "none"
    assert "/srv/bundle:/opt/bundle:ro" in command
    assert "/scratch/run/in:/app/data:ro" in command
    assert command[command.index("--memory") + 1] == "40g"
    # Started from the content id, never from the tag the payload names for the record.
    assert IMAGE_ID in command and "localhost/lab:dev" not in command
    assert command[command.index(IMAGE_ID) + 1 :] == [
        "--cut", "211",
        "--query_file", "/app/data/queries.fasta",
        "--output", "/app/output/predictions.tsv",
    ]  # fmt: skip


def test_no_memory_ceiling_when_the_payload_asks_for_none() -> None:
    command = rlm.container_command(_model(memory_gb=None), Path("/w"), "n")
    assert "--memory" not in command


def test_a_tag_is_not_an_image_id() -> None:
    with pytest.raises(ValueError):
        _model(image_id="localhost/lab:dev")


@pytest.mark.parametrize("field", ["bundle_path", "bundle_mount"])
@pytest.mark.parametrize(
    ("value", "words"),
    [
        ("srv/bundle", "absolute, normalized"),
        ("/srv/../home/user/.secrets", "absolute, normalized"),
        ("/srv/bundle/", "absolute, normalized"),
        ("//srv/bundle", "absolute, normalized"),
        ("/", "root folder"),
        ("/srv/bundle:/opt/other:rw", "colon"),
    ],
)
def test_a_mount_path_is_absolute_normalized_and_has_no_colon(
    field: str, value: str, words: str
) -> None:
    with pytest.raises(ValidationError, match=words):
        _model(**{field: value})


def test_store_keys_name_the_method_the_cut_and_the_content() -> None:
    key = rlm.content_key(_model(method_name="lab reader/211"), "predictions", "ab" * 32, ".tsv")
    assert key == "method-runs/lab-reader-211/cut211/predictions-abababababababab.tsv"


# The gates before the card is spent ------------------------------------------------------


def test_the_arm_passes_when_it_declares_the_cut_and_the_window_starts_at_it() -> None:
    session, config_id, start_id = _arm(cut="211", start="211")
    rlm.check_arm(session, rlm.RunLabMethodPayload.model_validate(_payload(config_id, start_id)))
    session, config_id, start_id = _arm(cut="211", start="227")
    rlm.check_arm(session, rlm.RunLabMethodPayload.model_validate(_payload(config_id, start_id)))


@pytest.mark.parametrize(
    ("arm", "words"),
    [
        ({"backend": "ankh"}, "ingested under"),
        ({"cut": None}, "declares no cut"),
        ({"cut": "227"}, "declares cut 227"),
        ({"cut": "211", "start": "194"}, "before the cut 211"),
    ],
)
def test_the_arm_is_refused_before_the_card_is_spent(arm: dict[str, Any], words: str) -> None:
    session, config_id, start_id = _arm(**arm)
    p = rlm.RunLabMethodPayload.model_validate(_payload(config_id, start_id))
    with pytest.raises(ValueError, match=words):
        rlm.check_arm(session, p)


def test_a_missing_configuration_is_refused() -> None:
    with pytest.raises(ValueError, match="not found"):
        rlm.check_arm(FakeSession({}), _model())


def test_a_bundle_inside_the_root_is_mounted_as_resolved(tmp_path: Path) -> None:
    root = tmp_path / "bundles"
    (root / "reader-v3").mkdir(parents=True)
    (root / "reader").symlink_to(root / "reader-v3")
    assert rlm.checked_bundle(str(root / "reader-v3"), str(root)) == root / "reader-v3"
    assert rlm.checked_bundle(str(root / "reader"), str(root)) == root / "reader-v3"


@pytest.mark.parametrize(
    ("layout", "words"),
    [
        ("outside", "outside the node's bundle root"),
        ("a link out of the root", "outside the node's bundle root"),
        ("a sibling sharing the root's prefix", "outside the node's bundle root"),
        ("missing", "cannot be resolved"),
    ],
)
def test_a_bundle_outside_the_root_is_refused(tmp_path: Path, layout: str, words: str) -> None:
    root = tmp_path / "bundles"
    root.mkdir()
    secrets = tmp_path / "secrets"
    secrets.mkdir()
    bundle = {
        "outside": secrets,
        "a link out of the root": root / "reader",
        "a sibling sharing the root's prefix": tmp_path / "bundles-old",
        "missing": root / "reader",
    }[layout]
    if layout == "a link out of the root":
        bundle.symlink_to(secrets)
    if layout == "a sibling sharing the root's prefix":
        bundle.mkdir()
    with pytest.raises(ValueError, match=words):
        rlm.checked_bundle(str(bundle), str(root))


def test_no_bundle_is_mounted_on_a_node_without_a_root_or_with_the_root_folder(
    tmp_path: Path,
) -> None:
    (tmp_path / "reader").mkdir()
    with pytest.raises(ValueError, match="sets no operation.method_bundle_root"):
        rlm.checked_bundle(str(tmp_path / "reader"), None)
    with pytest.raises(ValueError, match="bounds nothing"):
        rlm.checked_bundle(str(tmp_path / "reader"), "/")


def test_the_bundle_root_comes_from_the_node_tuning(monkeypatch: pytest.MonkeyPatch) -> None:
    from protea.config.tuning import get_tuning

    monkeypatch.setenv("PROTEA_TUNING__operation__method_bundle_root", "/srv/protea-bundles")
    get_tuning.cache_clear()
    try:
        assert rlm.bundle_root() == "/srv/protea-bundles"
    finally:
        monkeypatch.delenv("PROTEA_TUNING__operation__method_bundle_root")
        get_tuning.cache_clear()
    assert rlm.bundle_root() is None


def test_the_query_file_is_refused_when_its_hash_is_not_the_pinned_one(tmp_path: Path) -> None:
    store = FakeStore({FASTA_KEY: FASTA + b">P3\nMMM\n"})
    with pytest.raises(ValueError, match="the payload pins"):
        rlm.fetch_queries(store, _model(), tmp_path)


def test_the_query_file_lands_in_the_input_folder(tmp_path: Path) -> None:
    assert rlm.fetch_queries(FakeStore({FASTA_KEY: FASTA}), _model(), tmp_path) == 2
    assert (tmp_path / "in" / "queries.fasta").read_bytes() == FASTA
    assert (tmp_path / "out").is_dir()


# The run --------------------------------------------------------------------------------


def test_a_long_run_emits_while_it_works(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(rlm, "HEARTBEAT_SECONDS", 0.1)
    _play(monkeypatch, "import time; time.sleep(0.6)")
    events = Events()
    rlm.run_container(_model(), tmp_path, "n", events)
    assert len(events.named("run_lab_method.running")) >= 2


def test_a_failing_container_fails_the_job_with_its_last_words(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _play(monkeypatch, "import sys; print('the bundle has no world model'); sys.exit(3)")
    events = Events()
    with pytest.raises(RuntimeError, match="exited 3"):
        rlm.run_container(_model(), tmp_path, "n", events)
    (failed,) = events.named("run_lab_method.container_failed")
    assert "no world model" in failed["tail"]


def test_a_run_past_its_limit_is_stopped(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    stopped: list[str] = []
    monkeypatch.setattr(rlm, "HEARTBEAT_SECONDS", 0.1)
    monkeypatch.setattr(rlm, "clear_previous_attempt", stopped.append)
    _play(monkeypatch, "import time; time.sleep(30)")
    with pytest.raises(TimeoutError):
        rlm.run_container(_model(timeout_hours=0.0001), tmp_path, "protea-method-x", Events())
    assert stopped == ["protea-method-x"]


def test_the_record_must_state_what_the_platform_gave_and_measured() -> None:
    p = _model()
    honest = {
        "name": p.method_name,
        "cut": 211,
        "query_file": ["q", FASTA_SHA],
        "predictions": {"sha256": "a" * 64},
    }
    rlm.check_provenance(p, rlm.RunOutputs("k", "a" * 64, "j", honest))
    for lie, words in [
        ({"query_file": ["q", "b" * 64]}, "query file sha256"),
        ({"predictions": {"sha256": "b" * 64}}, "predictions sha256"),
        ({"name": "another-arm"}, "name 'another-arm'"),
        ({"cut": 230}, "cut 230"),
    ]:
        with pytest.raises(ValueError, match=words):
            rlm.check_provenance(p, rlm.RunOutputs("k", "a" * 64, "j", {**honest, **lie}))


def test_a_record_that_leaves_a_fact_out_is_refused() -> None:
    p = _model()
    with pytest.raises(ValueError, match="does not state its name, cut, query file sha256"):
        rlm.check_provenance(p, rlm.RunOutputs("k", "a" * 64, "j", {}))
    partial = {"name": p.method_name, "cut": 211, "query_file": ["q", FASTA_SHA]}
    with pytest.raises(ValueError, match="does not state its predictions sha256"):
        rlm.check_provenance(p, rlm.RunOutputs("k", "a" * 64, "j", partial))


def test_a_long_step_beats_until_it_ends() -> None:
    events = Events()
    with rlm.beating(events, "run_lab_method.storing", 0.05):
        time.sleep(0.3)
    beats = len(events.named("run_lab_method.storing"))
    assert beats >= 3
    time.sleep(0.15)
    assert len(events.named("run_lab_method.storing")) == beats  # the thread stopped with the block


def test_the_least_free_card_decides_and_a_node_without_nvidia_smi_is_refused(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        rlm.subprocess, "run", lambda *a, **k: SimpleNamespace(stdout="9000\n3000\n")
    )
    assert rlm.free_card_mib() == 3000

    def missing(*a: Any, **k: Any) -> Any:
        raise FileNotFoundError("nvidia-smi")

    monkeypatch.setattr(rlm.subprocess, "run", missing)
    with pytest.raises(ValueError, match="nvidia-smi"):
        rlm.free_card_mib()


def test_the_scratch_folder_comes_from_the_node_tuning(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from protea.config.tuning import get_tuning

    folder = tmp_path / "disk" / "method-scratch"
    monkeypatch.setenv("PROTEA_TUNING__operation__method_scratch_dir", str(folder))
    get_tuning.cache_clear()
    try:
        assert rlm.scratch_root() == str(folder)
        assert folder.is_dir()
    finally:
        monkeypatch.delenv("PROTEA_TUNING__operation__method_scratch_dir")
        get_tuning.cache_clear()


# The whole job ----------------------------------------------------------------------------


@pytest.fixture
def node(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> SimpleNamespace:
    """A node that holds the pinned image, a bundle under its root, a card no other job holds
    and that has free memory, and a store with the query file."""
    root = tmp_path / "bundles"
    (root / "reader").mkdir(parents=True)
    state = SimpleNamespace(
        store=FakeStore({FASTA_KEY: FASTA}),
        cleared=[],
        free=20_000,
        holder=None,
        card_read=0,
        root=root,
        bundle=root / "reader",
    )

    def free() -> int:
        state.card_read += 1
        return state.free

    monkeypatch.setattr(rlm, "_artifact_store", lambda: state.store)
    monkeypatch.setattr(rlm, "image_id_on_node", lambda image: IMAGE_ID)
    monkeypatch.setattr(rlm, "free_card_mib", free)
    monkeypatch.setattr(rlm, "clear_previous_attempt", state.cleared.append)
    monkeypatch.setattr(rlm, "scratch_root", lambda: None)
    monkeypatch.setattr(rlm, "bundle_root", lambda: str(state.root))
    monkeypatch.setattr(_card_guard, "card_busy", lambda session, own: state.holder)
    return state


def _job_payload(
    node: SimpleNamespace, config_id: uuid.UUID, start_id: uuid.UUID, **over: Any
) -> dict[str, Any]:
    """A payload whose bundle is the node's."""
    return _payload(config_id, start_id, **{"bundle_path": str(node.bundle), **over})


def test_a_run_stores_its_outputs_and_enqueues_the_ingestion_as_a_child_job(
    monkeypatch: pytest.MonkeyPatch, node: SimpleNamespace
) -> None:
    session, config_id, start_id = _arm()
    job_id = str(uuid.uuid4())
    payload = {**_job_payload(node, config_id, start_id, query_set_id="qs"), "_job_id": job_id}
    _play(monkeypatch, METHOD, "lab-reader-cut211", "honest")
    events = Events()

    result = rlm.RunLabMethodOperation().execute(session, payload, emit=events)  # type: ignore[arg-type]

    assert node.cleared == [f"protea-method-{job_id}"]
    (child,) = [o for o in session.added if isinstance(o, Job)]
    assert result.publish_after_commit == [(rlm.INGEST_QUEUE, child.id)]
    assert (child.operation, child.queue_name) == (
        "ingest_method_predictions",
        "protea.methods.write",
    )
    assert child.parent_job_id == uuid.UUID(job_id)
    stored = node.store.objects[child.payload["predictions_uri"]]
    assert hashlib.sha256(stored).hexdigest() == child.payload["predictions_sha256"]
    assert child.payload["query_set_id"] == "qs"
    assert child.payload["method"]["name"] == "lab-reader-cut211"
    run = child.payload["method"]["platform_run"]
    assert (run["image_id"], run["queries_fasta_sha256"], run["job_id"]) == (
        IMAGE_ID,
        FASTA_SHA,
        job_id,
    )
    assert run["bundle_path"] == str(node.bundle)
    assert run["provenance_key"] in node.store.objects
    (created,) = [o for o in session.added if isinstance(o, JobEvent)]
    assert created.job_id == child.id and created.event == "job.created"
    assert [e["queries"] for e in events.named("run_lab_method.queries")] == [2]
    (storing,) = events.named("run_lab_method.storing")
    assert storing["tsv_bytes"] == len(stored)
    assert events.named("run_lab_method.stored")
    log = node.store.objects[run["container_log_key"]]
    assert b"the method ran on" in log


def test_a_busy_card_sends_the_job_back_without_running_anything(
    monkeypatch: pytest.MonkeyPatch, node: SimpleNamespace
) -> None:
    session, config_id, start_id = _arm()
    node.free = 2_000
    monkeypatch.setattr(rlm, "run_in_scratch", lambda *a: pytest.fail("the card was busy"))
    payload = _job_payload(node, config_id, start_id)
    with pytest.raises(RetryLaterError) as busy:
        rlm.RunLabMethodOperation().execute(session, payload, emit=Events())  # type: ignore[arg-type]
    assert busy.value.delay_seconds == rlm.CARD_BUSY_RETRY_SECONDS
    assert session.added == []


def test_a_card_another_job_holds_sends_the_job_back_before_its_memory_is_read(
    monkeypatch: pytest.MonkeyPatch, node: SimpleNamespace
) -> None:
    """A pass between two batches can leave the card's memory free; the job table still says it is held."""
    session, config_id, start_id = _arm()
    node.holder = "encode_residue_vocabulary job 15624da2 still holds the card"
    monkeypatch.setattr(rlm, "run_in_scratch", lambda *a: pytest.fail("the card was held"))
    payload = _job_payload(node, config_id, start_id)
    with pytest.raises(RetryLaterError, match="15624da2 still holds the card") as busy:
        rlm.RunLabMethodOperation().execute(session, payload, emit=Events())  # type: ignore[arg-type]
    assert busy.value.delay_seconds == rlm.CARD_BUSY_RETRY_SECONDS
    assert node.card_read == 0
    assert session.added == []


def test_a_bundle_outside_the_root_is_refused_before_anything_runs(
    monkeypatch: pytest.MonkeyPatch, node: SimpleNamespace, tmp_path: Path
) -> None:
    session, config_id, start_id = _arm()
    (tmp_path / "secrets").mkdir()
    monkeypatch.setattr(rlm, "run_in_scratch", lambda *a: pytest.fail("the bundle was mounted"))
    payload = _job_payload(node, config_id, start_id, bundle_path=str(tmp_path / "secrets"))
    with pytest.raises(ValueError, match="outside the node's bundle root"):
        rlm.RunLabMethodOperation().execute(session, payload, emit=Events())  # type: ignore[arg-type]
    assert (node.cleared, node.card_read, session.added) == ([], 0, [])


def test_the_resolved_bundle_is_the_one_mounted(
    monkeypatch: pytest.MonkeyPatch, node: SimpleNamespace
) -> None:
    session, config_id, start_id = _arm()
    (node.root / "current").symlink_to(node.bundle)
    mounted: list[str] = []

    def command(p: rlm.RunLabMethodPayload, work: Path, name: str) -> list[str]:
        mounted.append(p.bundle_path)
        return [sys.executable, "-c", METHOD, str(work), "lab-reader-cut211", "honest"]

    monkeypatch.setattr(rlm, "container_command", command)
    payload = _job_payload(node, config_id, start_id, bundle_path=str(node.root / "current"))
    rlm.RunLabMethodOperation().execute(session, payload, emit=Events())  # type: ignore[arg-type]
    assert mounted == [str(node.bundle)]


def test_a_tag_that_moved_is_refused(
    monkeypatch: pytest.MonkeyPatch, node: SimpleNamespace
) -> None:
    session, config_id, start_id = _arm()
    monkeypatch.setattr(rlm, "image_id_on_node", lambda image: "e" * 64)
    payload = _job_payload(node, config_id, start_id)
    with pytest.raises(ValueError, match="the payload pins"):
        rlm.RunLabMethodOperation().execute(session, payload, emit=Events())  # type: ignore[arg-type]


def test_a_record_that_lies_about_its_output_enqueues_nothing(
    monkeypatch: pytest.MonkeyPatch, node: SimpleNamespace
) -> None:
    session, config_id, start_id = _arm()
    _play(monkeypatch, METHOD, "lab-reader-cut211", "lies")
    payload = _job_payload(node, config_id, start_id)
    with pytest.raises(ValueError, match="predictions sha256"):
        rlm.RunLabMethodOperation().execute(session, payload, emit=Events())  # type: ignore[arg-type]
    assert session.added == []


def test_a_run_that_leaves_no_record_enqueues_nothing(
    monkeypatch: pytest.MonkeyPatch, node: SimpleNamespace
) -> None:
    session, config_id, start_id = _arm()
    _play(monkeypatch, METHOD, "lab-reader-cut211", "silent")
    payload = _job_payload(node, config_id, start_id)
    with pytest.raises(RuntimeError, match="no predictions.json"):
        rlm.RunLabMethodOperation().execute(session, payload, emit=Events())  # type: ignore[arg-type]
    assert session.added == []
    assert not any(
        key.startswith("method-runs/") and key.endswith(".tsv") for key in node.store.objects
    )


def test_a_failed_run_keeps_its_log(monkeypatch: pytest.MonkeyPatch, node: SimpleNamespace) -> None:
    session, config_id, start_id = _arm()
    _play(monkeypatch, "import sys; print('out of memory in the world model'); sys.exit(1)")
    events = Events()
    payload = _job_payload(node, config_id, start_id)
    with pytest.raises(RuntimeError, match="exited 1"):
        rlm.RunLabMethodOperation().execute(session, payload, emit=events)  # type: ignore[arg-type]
    (kept,) = events.named("run_lab_method.container_log")
    assert b"out of memory in the world model" in node.store.objects[kept["key"]]
    assert session.added == []
