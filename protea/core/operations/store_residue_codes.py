"""The write stage of ``encode_residue_vocabulary``: index a shard of per-residue codes (ADR-D51).

Runs on ``protea.embeddings.write``, on the host that owns the database, as
``store_embeddings`` does for ``compute_embeddings``. The card computed the codes and put
ONE compressed shard in the artifact store; its message names the shard, its sha256 and
where every sequence sits in it. This stage fetches the shard, checks that the bytes are
the ones named and that they hold what the message says, inserts one ``residue_code`` row
per sequence in one transaction, and advances the parent job, closing it on the last batch.

So the database is written only here, never across the link from the card, and a batch
whose message never arrives leaves no row and no partial state: relaunching the
coordinator dispatches exactly the sequences that have no row.

WHY THE CODES DO NOT TRAVEL IN THE MESSAGE

They are about 87 GB for the corpus. Through the broker they would be written to its
message store on the server's disk and read back, base64 in JSON, before being written a
second time to the artifact store. The card puts them in the store once, with retries,
under a key that names their content, and the queue carries the small part.
"""

from __future__ import annotations

import uuid
from typing import Any

from pydantic import Field
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, OperationResult, ProteaPayload
from protea.core.contracts.parent_progress import update_parent_progress
from protea.core.operations._parent_guard import parent_cancelled
from protea.core.residue_code_shards import Shard, unpack_shard
from protea.core.utils import contract_payload
from protea.infrastructure.orm.models.embedding.residue_code import ResidueCode as ResidueCodeRow

#: The queue the server's writer consumes, shared with ``store_embeddings``.
WRITE_QUEUE = "protea.embeddings.write"


class StoredSequence(ProteaPayload, frozen=True):
    """Where one sequence sits in the shard, and how big its code is."""

    sequence_id: int
    position: int = Field(ge=0)
    residue_count: int = Field(ge=0)
    entry_count: int = Field(ge=0)


class StoreResidueCodesPayload(ProteaPayload, frozen=True):
    """One shard written by one card batch."""

    parent_job_id: str
    target_embedding_config_id: str
    shard_key: str = Field(min_length=1)
    shard_sha256: str = Field(min_length=64, max_length=64)
    sequences: list[StoredSequence] = Field(min_length=1)


def check_shard(shard: Shard, p: StoreResidueCodesPayload) -> None:
    """Refuse a shard that does not hold what the message says it holds.

    The sha256 already proved the bytes are the ones the card wrote; this proves the card
    and the message agree about them: the configuration, and every sequence at its
    position with its sizes.
    """
    target = shard.header.get("target_embedding_config_id")
    if target != p.target_embedding_config_id:
        raise ValueError(
            f"shard {p.shard_key} was written for config {target}, "
            f"the message says {p.target_embedding_config_id}"
        )
    if len(shard) != len(p.sequences):
        raise ValueError(
            f"shard {p.shard_key} holds {len(shard)} sequences, the message lists "
            f"{len(p.sequences)}"
        )
    for s in p.sequences:
        code = shard.code(s.position)
        if int(shard.sequence_ids[s.position]) != s.sequence_id or (
            code.residues,
            code.entries,
        ) != (s.residue_count, s.entry_count):
            raise ValueError(
                f"shard {p.shard_key} at position {s.position} holds sequence "
                f"{int(shard.sequence_ids[s.position])} with {code.residues} residues and "
                f"{code.entries} entries, the message says {s.sequence_id}, "
                f"{s.residue_count} and {s.entry_count}"
            )


def index_rows_of(p: StoreResidueCodesPayload) -> list[dict[str, Any]]:
    """The ``residue_code`` rows a checked message stands for."""
    target = uuid.UUID(p.target_embedding_config_id)
    return [
        {
            "sequence_id": s.sequence_id,
            "embedding_config_id": target,
            "shard_key": p.shard_key,
            "shard_sha256": p.shard_sha256,
            "position": s.position,
            "residue_count": s.residue_count,
            "entry_count": s.entry_count,
        }
        for s in p.sequences
    ]


def _artifact_store() -> Any:
    from pathlib import Path

    from protea.infrastructure.settings import load_settings
    from protea.infrastructure.storage import get_artifact_store

    return get_artifact_store(load_settings(Path(__file__).resolve().parents[3]))


class StoreResidueCodesOperation:
    """Index one shard of per-residue codes and advance the parent job."""

    name = "store_residue_codes"
    description = (
        "Write stage of encode_residue_vocabulary, on the host that owns the database: "
        "check one shard of per-residue codes in the artifact store against its message, "
        "insert one residue_code row per sequence, and advance the parent job."
    )

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = StoreResidueCodesPayload.model_validate(contract_payload(payload))
        parent_job_id = uuid.UUID(p.parent_job_id)
        if parent_cancelled(session, p.parent_job_id, emit, "store_residue_codes"):
            return OperationResult(result={"skipped": True})
        shard = unpack_shard(_artifact_store().get(p.shard_key), p.shard_sha256)
        check_shard(shard, p)
        session.execute(pg_insert(ResidueCodeRow).on_conflict_do_nothing(), index_rows_of(p))
        counts = {
            "sequences": len(p.sequences),
            "residues": sum(s.residue_count for s in p.sequences),
            "entries": sum(s.entry_count for s in p.sequences),
            "shard_key": p.shard_key,
        }
        emit("store_residue_codes.done", None, counts, "info")
        update_parent_progress(
            session, parent_job_id, emit, event_name="store_residue_codes.parent_succeeded"
        )
        return OperationResult(result=counts)

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        n = len((payload or {}).get("sequences") or [])
        return f"n={n}" if n else ""
