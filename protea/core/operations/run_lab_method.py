"""Run a lab method's container on the node with the card, and hand its predictions to the platform.

The laboratory's state of the art (the reader over the world model, the generator, whatever
comes next) is fitted outside the platform and ships as a container image plus a data bundle,
under one contract: ``--cut C --query_file F --output O.tsv``, with a JSON beside the TSV that
records the run (``predictions.json``: the method's name, the lab commit, the models and their
hashes, the hash of the query file it read and of the TSV it wrote). Running that container by
hand and importing its file makes the prediction a result computed somewhere else, and nothing
in the platform can then say which queries it saw or which image produced it. This operation
makes the run a platform job instead:

* the queries come from the platform's own store, by key and sha256 (what
  ``export_evaluation_targets`` writes for an evaluation set), so the method and the evaluator
  score one population by construction;
* the image is named by its content: the tag the payload names must resolve on the node to the
  ``image_id`` the payload pins, and the container is started from that id, so a tag moved
  under the job cannot change what runs;
* the container runs with no network, the bundle mounted read only, a memory ceiling, and a
  wall-clock limit, under a name derived from the job, so a retried job first removes what an
  interrupted attempt left running instead of putting a second run on the card;
* the TSV and its JSON are stored under keys that carry their own hash. The JSON is required:
  it must state the method's name, the cut, the hash of the query file and the hash of the TSV,
  and each must equal what the platform gave it or measured. A run that leaves no record is
  not evidence of what ran, so it does not reach the ingestion;
* the container's log is stored as well, on success and on failure, because when a run dies
  after hours it is the only account of why;
* the predictions enter through ``ingest_method_predictions``, enqueued as a child job on
  ``protea.methods.write``, so every gate that operation has still applies.

WHY A JOB AND NOT AN OPERATION MESSAGE. A run takes hours (the cut-230 recipe took 2 h 52 min
on 40,531 targets). ``OperationConsumer`` executes on the AMQP thread and acknowledges at the
end, so a message that runs for hours starves the connection's heartbeat, the broker drops the
connection, and the redelivery runs the container again. A job runs through ``QueueConsumer``,
which pumps the connection while the operation works and keeps the job's lease alive; the
operation also emits an event every ten minutes while the container runs and while its output
is hashed and uploaded, so the job's log says it is alive and the reaper's stall rule (30 minutes
without an event once the job is past its hard limit) never reads a long upload as death.

The scratch folder is the node's ``operation.method_scratch_dir`` tuning, because a TSV of
several GB in a tmpfs sits in memory beside the database.

WHY THE CARD IS CHECKED, NOT LOCKED. The node has one card, and another consumer (the embedding
batches) uses it. The operation will not start while the card has less free memory than the
payload asks for: it raises ``RetryLaterError`` and the job comes back later instead of failing.
It does not stop or pause the other consumers, because an operation does not manage workers.

Before the card is spent, the arm is checked as the evaluator will check it: the configuration
must be an ingested method's, it must declare the cut the payload asks the image for, and the
window the predictions go into must start at or after that cut.

It trains nothing, and it reads no bundle it was not given: the bundle path is the node's, named
in the payload and mounted read only where the image expects it.
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
import socket
import subprocess
import tempfile
import threading
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Any

from pydantic import Field
from sqlalchemy.orm import Session

from protea.core._evaluation_leakage import _release
from protea.core.contracts.operation import (
    EmitFn,
    OperationResult,
    ProteaPayload,
    RetryLaterError,
)
from protea.core.utils import contract_payload
from protea.infrastructure.orm.models.embedding.embedding_config import (
    INGESTED_METHOD_BACKEND,
    EmbeddingConfig,
)
from protea.infrastructure.orm.models.job import Job, JobEvent

#: The job queue the node with the card consumes for method runs.
NODE_QUEUE = "protea.methods"
#: Where the server ingests a method's predictions (a job queue).
INGEST_QUEUE = "protea.methods.write"
#: Seconds between the events the job emits while the container runs.
HEARTBEAT_SECONDS = 600
#: Seconds the container gets to stop when the wall-clock limit is reached.
STOP_GRACE_SECONDS = 30
#: Seconds before a job that found the card busy is tried again.
CARD_BUSY_RETRY_SECONDS = 600

Sha256 = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
PositiveInt = Annotated[int, Field(gt=0)]


class RunLabMethodPayload(ProteaPayload, frozen=True):
    """Which method, from which image, at which cut, on which queries, into which window."""

    method_name: str = Field(min_length=1)
    image: str = Field(min_length=1)
    image_id: Sha256
    bundle_path: str = Field(min_length=1)
    bundle_mount: str = Field(min_length=1)
    cut: PositiveInt
    queries_fasta_uri: str = Field(min_length=1)
    queries_fasta_sha256: Sha256
    embedding_config_id: str
    annotation_set_id: str
    ontology_snapshot_id: str
    query_set_id: str | None = None
    limit_per_entry: PositiveInt | None = None
    timeout_hours: float = Field(default=12.0, gt=0, le=72)
    memory_gb: PositiveInt | None = 40
    min_free_card_mib: PositiveInt = 10_000


@dataclass(frozen=True)
class RunOutputs:
    """What a run left in the store, and the record the method wrote about itself."""

    predictions_key: str
    predictions_sha256: str
    provenance_key: str
    provenance: dict[str, Any]
    container_log_key: str | None = None


def container_command(p: RunLabMethodPayload, work: Path, name: str) -> list[str]:
    """The container call under the lab's contract: from the pinned id, with no network."""
    command = [
        "podman", "run", "--rm", "--name", name,
        "--device", "nvidia.com/gpu=all", "--network", "none",
        "-v", f"{p.bundle_path}:{p.bundle_mount}:ro",
        "-v", f"{work / 'in'}:/app/data:ro",
        "-v", f"{work / 'out'}:/app/output",
    ]  # fmt: skip
    if p.memory_gb is not None:
        command += ["--memory", f"{p.memory_gb}g"]
    return command + [
        p.image_id,
        "--cut", str(p.cut),
        "--query_file", "/app/data/queries.fasta",
        "--output", "/app/output/predictions.tsv",
    ]  # fmt: skip


def content_key(p: RunLabMethodPayload, kind: str, digest: str, suffix: str) -> str:
    """Store key of an output: the method, the cut, and the content's own hash."""
    name = "".join(c if c.isalnum() or c in "-_." else "-" for c in p.method_name)
    return f"method-runs/{name}/cut{p.cut}/{kind}-{digest[:16]}{suffix}"


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def check_arm(session: Session, p: RunLabMethodPayload) -> None:
    """Refuse a run the evaluator could not score where the payload sends it, before the card is spent."""
    config = session.get(EmbeddingConfig, uuid.UUID(p.embedding_config_id))
    if config is None:
        raise ValueError(f"EmbeddingConfig {p.embedding_config_id} not found")
    if config.model_backend != INGESTED_METHOD_BACKEND:
        raise ValueError(
            f"config {p.embedding_config_id} has backend {config.model_backend!r}; a method's "
            f"predictions are ingested under {INGESTED_METHOD_BACKEND!r}"
        )
    declared = _release(session, config.trained_on_annotation_set_id)
    if declared is None:
        raise ValueError(
            f"config {p.embedding_config_id} declares no cut, so nothing gates its run"
        )
    if declared != p.cut:
        raise ValueError(
            f"the arm declares cut {declared}; the payload asks the image for cut {p.cut}"
        )
    start = _release(session, uuid.UUID(p.annotation_set_id))
    if start is None or start < p.cut:
        raise ValueError(
            f"the window starts at release {start}, before the cut {p.cut}: the method was "
            "fitted on annotations the window would score as new"
        )


def image_id_on_node(image: str) -> str:
    """The id the node's podman holds under ``image`` (a tag or an id)."""
    proc = subprocess.run(
        ["podman", "image", "inspect", "--format", "{{.Id}}", image],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    if proc.returncode != 0:
        raise ValueError(f"the node holds no image {image}: {proc.stderr.strip()[-300:]}")
    return proc.stdout.strip()


def free_card_mib() -> int:
    """The least free memory over the node's cards, in MiB: the container is given all of them."""
    try:
        proc = subprocess.run(
            ["nvidia-smi", "--query-gpu=memory.free", "--format=csv,noheader,nounits"],
            capture_output=True,
            text=True,
            timeout=60,
            check=True,
        )
    except (FileNotFoundError, subprocess.CalledProcessError) as exc:
        raise ValueError(f"this node cannot read a card with nvidia-smi: {exc}") from exc
    return min(int(line) for line in proc.stdout.split())


def scratch_root() -> str | None:
    """The folder runs work in: the node's tuning, else the system's temporary folder.

    A method's TSV can be several GB, and on a node whose temporary folder is a tmpfs it
    would sit in memory beside the database, so a node sets
    ``PROTEA_TUNING__operation__method_scratch_dir`` to a folder on disk.
    """
    from protea.config.tuning import get_tuning

    root = get_tuning().operation.method_scratch_dir
    if root:
        Path(root).mkdir(parents=True, exist_ok=True)
    return root


@contextmanager
def beating(emit: EmitFn, event: str, every: float) -> Iterator[None]:
    """Emit ``event`` every ``every`` seconds while the block runs, so a long step shows it is alive."""
    stop = threading.Event()
    started = time.monotonic()

    def beat() -> None:
        while not stop.wait(every):
            emit(event, None, {"elapsed_seconds": round(time.monotonic() - started)}, "info")

    thread = threading.Thread(target=beat, name=f"beat-{event}", daemon=True)
    thread.start()
    try:
        yield
    finally:
        stop.set()
        thread.join()


def clear_previous_attempt(name: str) -> None:
    """Remove a container an interrupted attempt of this job left behind, if any."""
    subprocess.run(
        ["podman", "rm", "--force", "--ignore", "--time", str(STOP_GRACE_SECONDS), name],
        capture_output=True,
        timeout=STOP_GRACE_SECONDS + 120,
        check=False,
    )


def fetch_queries(store: Any, p: RunLabMethodPayload, work: Path) -> int:
    """Write the query FASTA into the run's input folder after checking its hash."""
    raw = store.get(p.queries_fasta_uri)
    got = hashlib.sha256(raw).hexdigest()
    if got != p.queries_fasta_sha256:
        raise ValueError(
            f"{p.queries_fasta_uri} has sha256 {got}; the payload pins {p.queries_fasta_sha256}"
        )
    (work / "in").mkdir()
    (work / "out").mkdir()
    (work / "in" / "queries.fasta").write_bytes(raw)
    return raw.count(b">")


def run_container(p: RunLabMethodPayload, work: Path, name: str, emit: EmitFn) -> None:
    """Run the container to its end, with an event every HEARTBEAT_SECONDS; stop it at the limit."""
    log = work / "container.log"
    started = time.monotonic()
    with log.open("wb") as out:
        proc = subprocess.Popen(
            container_command(p, work, name), stdout=out, stderr=subprocess.STDOUT
        )
        while True:
            try:
                code = proc.wait(timeout=HEARTBEAT_SECONDS)
                break
            except subprocess.TimeoutExpired:
                elapsed = time.monotonic() - started
                if elapsed > p.timeout_hours * 3600:
                    clear_previous_attempt(name)
                    proc.kill()
                    proc.wait()
                    raise TimeoutError(
                        f"the method ran past {p.timeout_hours} h and was stopped"
                    ) from None
                emit("run_lab_method.running", None, {"elapsed_seconds": round(elapsed)}, "info")
    if code != 0:
        tail = log.read_bytes()[-2000:].decode("utf-8", "replace")
        emit("run_lab_method.container_failed", None, {"returncode": code, "tail": tail}, "error")
        raise RuntimeError(f"the method's container exited {code}: {tail[-300:]}")


def store_outputs(store: Any, p: RunLabMethodPayload, work: Path) -> RunOutputs:
    """Put the TSV and its JSON in the store under keys that carry their hash."""
    tsv = work / "out" / "predictions.tsv"
    if not tsv.is_file():
        raise RuntimeError("the container exited 0 and wrote no predictions.tsv")
    sha = sha256_of(tsv)
    key = content_key(p, "predictions", sha, ".tsv")
    side = work / "out" / "predictions.json"
    if not side.is_file():
        raise RuntimeError(
            "the container wrote no predictions.json beside its TSV: the record of what ran is "
            "part of the contract, and a run without it is not evidence"
        )
    record = json.loads(side.read_text())
    if not isinstance(record, dict):
        raise RuntimeError("predictions.json is not a JSON object")
    store.put(key, tsv)
    side_key = content_key(p, "provenance", sha256_of(side), ".json")
    store.put(side_key, side)
    return RunOutputs(key, sha, side_key, record)


def keep_log(store: Any, p: RunLabMethodPayload, log: Path, emit: EmitFn) -> str | None:
    """Store the container's log, the only evidence of why a long run died. Never raises."""
    if not log.is_file():
        return None
    try:
        key = content_key(p, "container-log", sha256_of(log), ".txt")
        store.put(key, log)
    except Exception as exc:  # a lost log must not hide the run's own error
        emit("run_lab_method.container_log_lost", str(exc)[-300:], {}, "warning")
        return None
    emit("run_lab_method.container_log", None, {"key": key, "bytes": log.stat().st_size}, "info")
    return key


def stated(record: dict[str, Any]) -> dict[str, Any]:
    """The four facts a method's record must state, read from the lab's contract layout."""
    query = record.get("query_file")
    written = record.get("predictions")
    return {
        "name": record.get("name"),
        "cut": record.get("cut"),
        "query file sha256": query[1] if isinstance(query, list) and len(query) == 2 else None,
        "predictions sha256": written.get("sha256") if isinstance(written, dict) else None,
    }


def check_provenance(p: RunLabMethodPayload, out: RunOutputs) -> None:
    """The method's record must state what it ran on and wrote, and agree with the platform."""
    facts = stated(out.provenance)
    missing = [fact for fact, value in facts.items() if value is None]
    if missing:
        raise ValueError(
            f"the method's record does not state its {', '.join(missing)}, and the record is "
            "the evidence of what ran"
        )
    measured = {
        "name": p.method_name,
        "cut": p.cut,
        "query file sha256": p.queries_fasta_sha256,
        "predictions sha256": out.predictions_sha256,
    }
    wrong = [f"{fact} {facts[fact]!r}, the platform has {want!r}"
             for fact, want in measured.items() if facts[fact] != want]  # fmt: skip
    if wrong:
        raise ValueError(f"the method's record disagrees with the platform: {'; '.join(wrong)}")


def ingest_payload(p: RunLabMethodPayload, out: RunOutputs, run: dict[str, Any]) -> dict[str, Any]:
    """The child job's payload: the stored TSV, the window, and the run as the method's record."""
    payload: dict[str, Any] = {
        "predictions_uri": out.predictions_key,
        "predictions_sha256": out.predictions_sha256,
        "embedding_config_id": p.embedding_config_id,
        "annotation_set_id": p.annotation_set_id,
        "ontology_snapshot_id": p.ontology_snapshot_id,
        "method": {**out.provenance, "name": p.method_name, "platform_run": run},
    }
    if p.query_set_id is not None:
        payload["query_set_id"] = p.query_set_id
    if p.limit_per_entry is not None:
        payload["limit_per_entry"] = p.limit_per_entry
    return payload


def enqueue_ingest(
    session: Session, payload: dict[str, Any], parent: uuid.UUID | None
) -> uuid.UUID:
    """Create the ingestion job as a child of the run; the worker publishes it after the commit."""
    child = Job(
        id=uuid.uuid4(),
        operation="ingest_method_predictions",
        queue_name=INGEST_QUEUE,
        payload=payload,
        parent_job_id=parent,
        description=f"ingest {payload['method']['name']} from run_lab_method",
    )
    session.add(child)
    session.flush()
    fields = {"operation": "ingest_method_predictions", "queue": INGEST_QUEUE}
    fields |= {"trigger": "run_lab_method", "parent_job_id": str(parent) if parent else None}
    session.add(JobEvent(job_id=child.id, event="job.created", fields=fields))
    return child.id


def _artifact_store() -> Any:
    from protea.infrastructure.settings import load_settings
    from protea.infrastructure.storage import get_artifact_store

    return get_artifact_store(load_settings(Path(__file__).resolve().parents[3]))


def run_in_scratch(p: RunLabMethodPayload, name: str, emit: EmitFn) -> tuple[RunOutputs, int]:
    """Fetch, run and store inside a scratch folder that is gone when this returns."""
    store = _artifact_store()
    with tempfile.TemporaryDirectory(prefix="protea-method-", dir=scratch_root()) as scratch:
        work = Path(scratch)
        queries = fetch_queries(store, p, work)
        fields = {"queries": queries, "scratch": scratch}
        emit("run_lab_method.queries", f"{queries} queries", fields, "info")
        try:
            run_container(p, work, name, emit)
        finally:
            log_key = keep_log(store, p, work / "container.log", emit)
        tsv = work / "out" / "predictions.tsv"
        size = tsv.stat().st_size if tsv.is_file() else 0
        emit("run_lab_method.storing", None, {"tsv_bytes": size}, "info")
        with beating(emit, "run_lab_method.storing", HEARTBEAT_SECONDS):
            out = store_outputs(store, p, work)
        emit("run_lab_method.stored", None, {"predictions_uri": out.predictions_key}, "info")
        return dataclasses.replace(out, container_log_key=log_key), queries


class RunLabMethodOperation:
    """Run a lab method's container on the node with the card and enqueue its ingestion."""

    name = "run_lab_method"
    description = (
        "Run a lab method's container on the node with the card, on queries taken from the "
        "store by hash and from an image pinned by id, store its predictions and their record, "
        "and enqueue their ingestion."
    )

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        pl = payload or {}
        return f"{pl.get('method_name', '?')} at cut {pl.get('cut', '?')}"

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        job_id = payload.get("_job_id")
        p = RunLabMethodPayload.model_validate(contract_payload(payload))
        check_arm(session, p)
        held = image_id_on_node(p.image)
        if held != p.image_id:
            raise ValueError(
                f"{p.image} is image {held} on this node; the payload pins {p.image_id}"
            )
        name = f"protea-method-{job_id or uuid.uuid4().hex[:12]}"
        clear_previous_attempt(name)
        free = free_card_mib()
        if free < p.min_free_card_mib:
            raise RetryLaterError(
                f"the card has {free} MiB free and the method asks for {p.min_free_card_mib}",
                delay_seconds=CARD_BUSY_RETRY_SECONDS,
            )
        started = time.monotonic()
        out, queries = run_in_scratch(p, name, emit)
        check_provenance(p, out)
        run = {
            "job_id": job_id,
            "node": socket.gethostname(),
            "image": p.image,
            "image_id": p.image_id,
            "queries_fasta_uri": p.queries_fasta_uri,
            "queries_fasta_sha256": p.queries_fasta_sha256,
            "provenance_key": out.provenance_key,
            "container_log_key": out.container_log_key,
            "seconds": round(time.monotonic() - started),
        }
        parent = uuid.UUID(job_id) if job_id else None
        child = enqueue_ingest(session, ingest_payload(p, out, run), parent)
        result = {**run, "predictions_uri": out.predictions_key, "queries": queries}
        result |= {"predictions_sha256": out.predictions_sha256, "ingest_job_id": str(child)}
        emit("run_lab_method.done", f"ingestion enqueued as {child}", result, "info")
        return OperationResult(result=result, publish_after_commit=[(INGEST_QUEUE, child)])
