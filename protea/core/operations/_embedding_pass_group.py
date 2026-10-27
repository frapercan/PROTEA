"""Which embedding configs may share one forward pass, and the refusal when they may not.

A forward pass already computes every hidden layer, unconditionally, in every
backend PROTEA ships: ``EsmModel.from_pretrained(..., output_hidden_states=True)``
and ``model(**tokens, output_hidden_states=True)`` for ESM / T5 / Ankh,
``LogitsConfig(return_hidden_states=True)`` for ESM-C. ``layer_indices`` never
asks the model for anything. It SELECTS, after the fact, from tensors that
already exist: ``protea_backends._chunk_helpers.validate_layers`` reduces it to
``sorted({int(li) for li in layer_indices})`` and indexes into the hidden
states it was handed.

``ComputeEmbeddingsPayload`` nonetheless carried one config id, so a layer grid
was run as one job per layer. Eleven configs pending a bank match are eleven
LAYERS of four models (ankh-base [16] [32]; esmc_600m [12] [24] [35];
esm2_t33_650M [11] [22] [33]; prot_t5_xl [8] [16] [24]), which at the rates the
GPU node measured is 113.6 h of inference where 42.3 h of it is distinct work.
Ten of the eleven passes recompute tensors the first one already held.

The saving is only real if the group really does share a pass, and "really" has
to be settled field by field rather than by eye, which is what this module is
for. Grouping configs that do NOT share a pass would write vectors under config
ids whose recipes never produced them: the rows would be well formed, the job
would report success, and nothing downstream re-derives an embedding, so the
mislabelling would never surface. That is why the check below RAISES rather
than warning and carrying on with a default.
"""

from __future__ import annotations

import time
import uuid
from typing import TYPE_CHECKING, Any

from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn
from protea.core.operations._compute_embeddings_helpers import serialize_inferred_chunks
from protea.infrastructure.orm.models.embedding.embedding_config import EmbeddingConfig

if TYPE_CHECKING:
    from collections.abc import Callable

    from protea_backends._chunk_helpers import ChunkEmbedding

    from protea.infrastructure.orm.models.sequence.sequence import Sequence

__all__ = [
    "FORWARD_PASS_FIELDS",
    "REDUCTION_ONLY_FIELDS",
    "SharedForwardPassError",
    "WRITE_ONLY_FIELDS",
    "assert_layers_available",
    "assert_shared_forward_pass",
    "load_pass_group",
    "run_group_inference",
    "shared_pass_emitter",
    "warn_if_pass_not_shared",
]

#: The substrate fields that decide what the model is fed and which weights
#: read it. Two configs differing in any of them are two different forward
#: passes, and no amount of sharing makes them one.
#:
#: ``max_length`` belongs here even though it reads like a post-processing
#: bound: every backend truncates BEFORE the model sees the sequence
#: (``truncation=True, max_length=config.max_length`` in the ESM tokenizer call
#: and in the T5 / Ankh ``batch_encode_plus``; ``seq_str[: config.max_length]``
#: in ESM-C). A shorter bound is a shorter input, so it is a different pass.
#:
#: ``model_name`` belongs here for the weights and for a second reason that is
#: easy to miss: the T5 backend auto-detects ProstT5 from the model name and
#: prepends ``<AA2fold>``. Two names can therefore differ in the TOKENS as well
#: as in the parameters.
FORWARD_PASS_FIELDS: tuple[str, ...] = ("max_length", "model_backend", "model_name")

#: The substrate fields that are read strictly after ``hidden_states`` exists,
#: and may therefore differ inside one group. Each one was traced to its use
#: rather than assumed:
#:
#: * ``layer_indices``  -- pure selection; see the module docstring. This is the
#:   axis the whole change exists to fan out over.
#: * ``layer_agg``      -- ``aggregate_layers`` reduces across the tensors that
#:   selection already returned.
#: * ``pooling``        -- collapses residues to a vector. The ``cls`` branch
#:   looks like a different pass but is not: it reads position 0 of the same
#:   hidden states.
#: * ``normalize_residues`` / ``normalize`` -- L2 applied to the residue matrix
#:   and to the pooled vector respectively, both after the pass.
#: * ``use_chunking`` / ``chunk_size`` / ``chunk_overlap`` -- ``chunk_and_pool``
#:   slices spans out of the POST-pass residue tensor. Chunking here is not a
#:   windowed re-inference; the model is never run again per chunk.
#:
REDUCTION_ONLY_FIELDS: tuple[str, ...] = (
    "chunk_overlap",
    "chunk_size",
    "layer_agg",
    "layer_indices",
    "normalize",
    "normalize_residues",
    "pooling",
    "use_chunking",
)

#: Fields that are neither fed to the model nor read during the reduction: they
#: are applied when the vector is written. ``embedding_scale`` is a property of
#: the config that owns the rows, resolved per config inside ``store_group_rows``.
WRITE_ONLY_FIELDS: tuple[str, ...] = ("embedding_scale",)


class SharedForwardPassError(ValueError):
    """Raised when a config group would not, in fact, share one forward pass.

    A ``ValueError`` subclass so a worker treats it as a hard, non-retryable
    failure like every other bad-payload refusal on this path, while callers
    that want to distinguish it can.
    """


def assert_shared_forward_pass(configs: list[Any]) -> None:
    """Refuse a config group whose members do not share one forward pass.

    Every field in :data:`FORWARD_PASS_FIELDS` must be identical across the
    group; every field in :data:`REDUCTION_ONLY_FIELDS` may vary.

    The message names EVERY diverging field with its distinct values, not the
    first one found. A group differing in two fields reported as differing in
    one invites the caller to fix that one, re-dispatch, and be refused again
    for a reason that was already known: the same defect as describing a level
    by one of the fields that vary in it.
    """
    if not configs:
        raise SharedForwardPassError("a forward-pass group needs at least one embedding config")

    _refuse_unclassified_fields()
    _refuse_duplicate_ids(configs)
    _refuse_grouped_learned_codes(configs)

    diverging = {
        field: sorted({repr(getattr(cfg, field)) for cfg in configs})
        for field in FORWARD_PASS_FIELDS
    }
    diverging = {field: values for field, values in diverging.items() if len(values) > 1}
    if not diverging:
        return

    detail = "; ".join(f"{field}={{{', '.join(values)}}}" for field, values in diverging.items())
    raise SharedForwardPassError(
        f"embedding configs {_id_list(configs)} do not share a forward pass: "
        f"{len(diverging)} of {len(FORWARD_PASS_FIELDS)} forward-pass field(s) diverge "
        f"({detail}). Only {', '.join(REDUCTION_ONLY_FIELDS)} may differ inside a group; "
        f"everything else selects a different input or different weights."
    )


def load_pass_group(session: Session, config_ids: list[str]) -> list[EmbeddingConfig]:
    """Load a config group in payload order, refusing a missing id or a bad group.

    Both the coordinator and the batch worker call this. The coordinator so a
    bad group dies before any GPU time is spent on it; the batch worker because
    it is handed config ids on a queue and is the process that would actually
    write the mislabelled rows, and a guard that only runs upstream is a guard
    the message that skipped the coordinator never meets.
    """
    configs: list[EmbeddingConfig] = []
    for raw_id in config_ids:
        config = session.get(EmbeddingConfig, uuid.UUID(str(raw_id)))
        if config is None:
            raise ValueError(f"EmbeddingConfig {raw_id} not found")
        configs.append(config)
    assert_shared_forward_pass(configs)
    return configs


def assert_layers_available(configs: list[Any], model: Any) -> None:
    """Refuse a layer index the loaded model does not have, BEFORE the first pass.

    ``protea_backends._chunk_helpers.validate_layers`` is handed ``hidden_states``,
    so it can only raise once the forward pass has already run. With one config
    that cost a batch; with a group it costs the whole pass for every config in
    it, and the third config asking for a layer the model does not have kills a
    job that has already paid the card. Checking against the depth the loaded
    model DECLARES moves that failure to just after the load, where it is free.

    A model whose depth cannot be read is left to ``validate_layers``: this is a
    pre-flight that makes the existing check earlier, never one that replaces it,
    so an unreadable depth degrades to today's behaviour rather than to silence.
    """
    depth = _declared_hidden_state_count(model)
    if depth is None:
        return
    bad = {
        str(cfg.id): sorted({int(li) for li in cfg.layer_indices if not 0 <= int(li) < depth})
        for cfg in configs
    }
    bad = {cfg_id: indices for cfg_id, indices in bad.items() if indices}
    if not bad:
        return
    detail = "; ".join(f"{cfg_id} asks {indices}" for cfg_id, indices in sorted(bad.items()))
    raise SharedForwardPassError(
        f"the loaded model exposes {depth} hidden states (valid reverse indices "
        f"0..{depth - 1}, 0 = last layer), but {detail}. Refused before the forward "
        f"pass so the group costs nothing."
    )


def _declared_hidden_state_count(model: Any) -> int | None:
    """How many hidden states a forward pass will return, or ``None`` if unreadable.

    ``num_hidden_layers`` plus one, because the backends index a tuple that
    carries the embedding output ahead of the per-layer outputs. T5 and Ankh
    spell the depth ``num_layers``; the HF config's attribute map answers to
    either, and ESM-C's SDK model carries no ``config`` at all, which is the case
    the ``None`` is for.
    """
    config = getattr(model, "config", None)
    for attr in ("num_hidden_layers", "num_layers"):
        value = getattr(config, attr, None)
        if isinstance(value, int) and value > 0:
            return value + 1
    return None


def _refuse_unclassified_fields() -> None:
    """Refuse every group while any identity field is classified nowhere.

    The guard compares an INCLUSION list, which is the safe direction to be
    wrong in only as long as the list is known to be complete. A field added to
    ``IDENTITY_FIELDS`` and to no tuple here would otherwise be free to vary
    inside a group, silently, because nothing would be comparing it: exactly the
    failure mode of naming a level by a subset of the fields that vary in it.
    Refusing until somebody classifies it costs one line of thought and prevents
    a class of mislabelled vectors.
    """
    from protea.core.embedding_identity import IDENTITY_FIELDS

    classified = set(FORWARD_PASS_FIELDS) | set(REDUCTION_ONLY_FIELDS) | set(WRITE_ONLY_FIELDS)
    unclassified = sorted(set(IDENTITY_FIELDS) - classified)
    if unclassified:
        raise SharedForwardPassError(
            f"embedding identity field(s) {', '.join(unclassified)} are classified neither "
            f"as forward-pass, reduction-only nor write-only. Until they are, no group can "
            f"be shown to share a pass: see _embedding_pass_group."
        )


def _refuse_duplicate_ids(configs: list[Any]) -> None:
    """Refuse a group naming the same config twice.

    Harmless to the store (the insert is ``on_conflict_do_nothing``) and
    therefore invisible: the job would report N configs written and have
    written N-1, with the duplicate's rows silently deduplicated by the
    database rather than by anything that could report it.
    """
    ids = [str(cfg.id) for cfg in configs]
    duplicated = sorted({cfg_id for cfg_id in ids if ids.count(cfg_id) > 1})
    if duplicated:
        raise SharedForwardPassError(
            f"embedding config id(s) {', '.join(duplicated)} appear more than once in a "
            f"forward-pass group of {len(ids)}; each config must be named once."
        )


def _refuse_grouped_learned_codes(configs: list[Any]) -> None:
    """Refuse a group of more than one that contains a learned-code config.

    A learned-code config has no forward pass of its own: its vectors come from
    a BASE config's pass plus a head, resolved per config at inference time in
    ``_infer_all_learned_code``. Whatever this module concludes about the
    learned config's own substrate fields says nothing about the pass that
    actually runs, so the sharing claim would be unfounded rather than wrong,
    which is worse. One learned config per job keeps that path exactly as it is.
    """
    if len(configs) < 2:
        return
    from protea.core.operations._learned_code_embed import is_learned_code_config

    learned = sorted(str(cfg.id) for cfg in configs if is_learned_code_config(cfg))
    if learned:
        raise SharedForwardPassError(
            f"learned-code config(s) {', '.join(learned)} cannot be grouped: a learned code's "
            f"forward pass is its base config's, which this group does not describe. "
            f"Dispatch learned-code configs one per job."
        )


def _id_list(configs: list[Any]) -> str:
    return ", ".join(str(cfg.id) for cfg in configs)


def shared_pass_emitter(config: EmbeddingConfig) -> Any | None:
    """The backend's ``embed_chunks_multi``, or ``None`` when it has none.

    The reduction side of this fan-out is PROTEA's; the pass side is the
    plugin's, because only the plugin holds the hidden states and
    ``protea-backends`` is a separately pinned dependency. Resolved by
    ``getattr`` rather than declared on the plugin protocol, so PROTEA does not
    need a lockstep bump of that pin to land the contract and a fleet part-way
    through the bump degrades to correct N-pass work rather than raising
    ``AttributeError`` on every batch.

    The signature it must satisfy is ``embed_chunks`` with the config
    pluralised: ``(model, tokenizer, sequences, configs, device)``, returning one
    ``list[list[ChunkEmbedding]]`` per config, in the order the configs arrived.
    """
    from protea.core.operations.compute_embeddings import _resolve_backend

    return getattr(_resolve_backend(config.model_backend), "embed_chunks_multi", None)


def warn_if_pass_not_shared(configs: list[EmbeddingConfig], emit: EmitFn) -> None:
    """WARN when a group of more than one is about to pay for a pass per config.

    The group is still correct without the plugin-side emitter, which is exactly
    why it has to say so: a job that fanned out three configs and quietly ran
    three passes produces the same rows as one that ran one, and the only
    difference is the number this change exists to move. Silence there would let
    the saving be reported from the contract rather than from the clock.
    """
    if len(configs) < 2 or shared_pass_emitter(configs[0]) is not None:
        return
    emit(
        "compute_embeddings_batch.pass_not_shared",
        None,
        {
            "backend": configs[0].model_backend,
            "configs": len(configs),
            "reason": "installed protea-backends plugin exposes no embed_chunks_multi; "
            "each config costs its own forward pass, so the group is correct "
            "but no GPU time is saved",
        },
        "warning",
    )


def run_group_inference(
    embed_group: Callable[[list[str]], list[list[ChunkEmbedding]]],
    configs: list[EmbeddingConfig],
    sequences: list[Sequence],
    batch_size: int,
    load_s: float,
) -> tuple[list[tuple[str, list[dict]]], dict[str, float]]:
    """Batch ``sequences``, embed each batch for the whole group, and pair up the rows.

    ``embed_group`` takes the batch's sequence strings and returns one
    chunk-list-per-sequence for each config, in the group's order; the operation
    supplies it so the model, device and dispatch seam stay where they are.

    Returns ``([(config_id, write_sequences), ...], phase_timings)``. The pairing
    is positional on both axes and ``strict=True`` on both, because both are
    silent when they go wrong: a short outer list would attribute one config's
    vectors to the next config in the group, and a short inner one would
    attribute one sequence's vectors to the next sequence (the failure
    ``serialize_inferred_chunks`` already refuses).

    ``inference_s`` is the number to watch as configs are added to a group: the
    forward pass is 99.51% of wall clock, so it is what has to stay flat for the
    saving to be real, and it is measured here rather than projected.
    """
    per_config: list[list[dict]] = [[] for _ in configs]
    infer_s = 0.0
    serialize_s = 0.0
    for i in range(0, len(sequences), batch_size):
        batch = sequences[i : i + batch_size]
        t = time.perf_counter()
        grouped = embed_group([s.sequence for s in batch])
        infer_s += time.perf_counter() - t
        t = time.perf_counter()
        for rows, batch_chunks in zip(per_config, grouped, strict=True):
            rows.extend(serialize_inferred_chunks(batch, batch_chunks))
        serialize_s += time.perf_counter() - t

    timings = {
        "model_load_s": round(load_s, 3),
        "inference_s": round(infer_s, 3),
        "serialize_s": round(serialize_s, 3),
    }
    return [
        (str(cfg.id), rows) for cfg, rows in zip(configs, per_config, strict=True)
    ], timings
