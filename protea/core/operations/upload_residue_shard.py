"""Put a spooled residue-code shard in the artifact store, then hand it to the server.

ADR-D51, amended on 2026-11-03. A batch of ``encode_residue_vocabulary`` spent 58 % of its
time on the card and 35 % putting its shard (about 25 MB) in the artifact store, one after
the other: measured over 305 batches of the corpus pass, the card was idle in 124 of the 125
samples taken while a shard went out. On a node that sets
``operation.residue_shard_spool_dir`` the batch writes the shard to that folder instead,
and this operation, consumed by a second worker of the same node with no card, puts it in
the store while the card encodes the next batch.

Every step keeps an order the inline put guaranteed:

* the batch acknowledges its message only once the shard is on the node's disk and this
  message is published, so a batch lost in between is delivered and encoded again;
* this operation publishes the message to ``store_residue_codes`` only after the put, so
  the server never indexes a shard the store does not hold;
* the spooled file is removed after the put and before that message goes out. A delivery
  that finds no file belongs to an earlier one that put the shard and died before its
  acknowledgement: the store must then hold bytes with the shard's sha256, and the message
  to the server is published again; otherwise the shard is lost and the delivery fails.

The message to the server travels inside this one, exactly as the batch built it, and is
checked against the server's own payload model before anything is put.
"""

from __future__ import annotations

import hashlib
import time
from pathlib import Path
from typing import Annotated, Any

from pydantic import Field, model_validator
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, Operation, OperationResult, ProteaPayload
from protea.core.operations._residue_shard_spool import spool_root, spooled_path
from protea.core.operations.encode_residue_vocabulary import put_with_retries
from protea.core.operations.store_residue_codes import WRITE_QUEUE, StoreResidueCodesPayload
from protea.core.utils import contract_payload

Sha256 = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]


class UploadResidueShardPayload(ProteaPayload, frozen=True):
    """One spooled shard, and the message the server receives once it is in the store."""

    shard_key: str = Field(min_length=1)
    shard_sha256: Sha256
    store: dict[str, Any]

    @model_validator(mode="after")
    def _the_message_names_this_shard(self) -> UploadResidueShardPayload:
        server = StoreResidueCodesPayload.model_validate(self.store)
        if (server.shard_key, server.shard_sha256) != (self.shard_key, self.shard_sha256):
            raise ValueError(
                f"the message to the server names shard {server.shard_key}, "
                f"this upload is for {self.shard_key}"
            )
        return self


def stored_sha256(store: Any, key: str) -> str | None:
    """The sha256 of what the store holds under ``key``, or None when it holds nothing."""
    if not store.exists(key):
        return None
    digest = hashlib.sha256()
    for chunk in store.iter_chunks(key):
        digest.update(chunk)
    return digest.hexdigest()


def _artifact_store() -> Any:
    from protea.infrastructure.settings import load_settings
    from protea.infrastructure.storage import get_artifact_store

    return get_artifact_store(load_settings(Path(__file__).resolve().parents[3]))


class UploadResidueShardOperation(Operation):
    """Put one spooled shard in the store and publish its message to the server."""

    name = "upload_residue_shard"
    description = (
        "Node child job: put one residue-code shard that a card batch spooled on this node's "
        "disk in the artifact store, then publish its message to store_residue_codes, which "
        "indexes it on the server."
    )

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = UploadResidueShardPayload.model_validate(contract_payload(payload))
        root = spool_root()
        if root is None:
            raise ValueError(
                "this node sets no operation.residue_shard_spool_dir, so it holds no spooled "
                "shard: upload_residue_shard runs on the node whose batches spool"
            )
        path = spooled_path(root, p.shard_key)
        store = _artifact_store()
        started = time.monotonic()
        if path.is_file():
            blob = path.read_bytes()
            got = hashlib.sha256(blob).hexdigest()
            if got != p.shard_sha256:
                raise ValueError(
                    f"the spooled file {path} has sha256 {got}; the batch wrote {p.shard_sha256}"
                )
            put_with_retries(store, p.shard_key, blob)
            path.unlink()
            put = len(blob)
        else:
            held = stored_sha256(store, p.shard_key)
            if held != p.shard_sha256:
                found = "nothing" if held is None else f"sha256 {held}"
                raise ValueError(
                    f"shard {p.shard_key} is not in the spool and the store holds {found} under "
                    "its key: its batch must be encoded again"
                )
            put = 0
        fields = {
            "shard_key": p.shard_key,
            "bytes": put,
            "seconds": round(time.monotonic() - started, 2),
        }
        said = f"put {put} bytes" if put else "already in the store from an earlier delivery"
        emit("upload.done", f"shard {said}", fields, "info")
        message = {
            "operation": "store_residue_codes",
            "job_id": p.store["parent_job_id"],
            "payload": p.store,
        }
        return OperationResult(result=fields, publish_operations=[(WRITE_QUEUE, message)])

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        return str((payload or {}).get("shard_key", ""))
