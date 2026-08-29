# protea/core/operations/count_backend_parameters.py
"""Record how many parameters each embedding backend actually executes.

``embedding_config.param_count`` is nullable and has been null for the whole T5
family. That is not a cosmetic gap. Any analysis that sorts models by size drops
the null rows, and because the nulls are a family rather than a scatter, the
surviving sample is selected on a variable correlated with the outcome. A
monotone ordering measured that way says nothing about capacity; it says
something about which family happened to have the column filled.

The number this writes is deliberately not the published one, because two of the
deployed checkpoints run something smaller than the model they are named after:

* the deployed ProtT5 is an encoder-only checkpoint, so the paper's figure counts
  a decoder that is never instantiated and lands it at the wrong end of a size
  axis by roughly a factor of two and a half
* ProtST loads a protein tower and a text tower together, and the embedding
  forward path calls the protein tower alone

So each distinct checkpoint is loaded through the same backend plugin that
computes the embeddings, and counted over the module that plugin's forward path
actually calls. Loading it any other way would measure a model nobody runs.

Counting is grouped by ``(model_backend, model_name)`` rather than by config,
because configurations differ in layer selection and pooling while sharing a
checkpoint; the eight canonical configurations do not imply eight loads.

It loads on CPU by default. ``numel`` does not depend on dtype or device, and the
card on the compute node is for inference, not for arithmetic that does not need
it.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pydantic import field_validator
from sqlalchemy import select
from sqlalchemy.orm import Session

from protea.core.contracts.operation import (
    EmitFn,
    Operation,
    OperationResult,
    ProteaPayload,
)
from protea.core.plugins import discover_plugins
from protea.core.utils import contract_payload
from protea.infrastructure.orm.models.embedding.embedding_config import EmbeddingConfig

BACKEND_PLUGIN_GROUP = "protea.backends"

#: Backends whose forward path executes a strict submodule of what they load.
#: Everything absent from this map executes the whole loaded model, which is the
#: ordinary case and needs no entry.
#:
#: This map duplicates knowledge that lives in the backend, so
#: ``tests/test_count_backend_parameters.py`` reads each backend's source and
#: fails when the named attribute stops appearing in its forward path. Without
#: that test the map would drift silently and would keep reporting a number that
#: was true once.
EXECUTED_SUBMODULE: dict[str, str] = {
    "protst": "protein_model",
}


class CountBackendParametersPayload(ProteaPayload, frozen=True):
    """Which configurations to count for.

    The default is every configuration still missing a count, so the ordinary
    invocation needs no arguments and is idempotent: a rerun after a new backend
    lands fills only the new rows.
    """

    embedding_config_ids: list[str] | None = None
    only_missing: bool = True
    device: str = "cpu"
    dry_run: bool = False

    @field_validator("embedding_config_ids")
    @classmethod
    def _reject_an_empty_selection(cls, value: list[str] | None) -> list[str] | None:
        """An empty list is refused because it reads as the opposite of itself.

        ``None`` means every configuration and an empty list looks like it means
        none, but an empty list is falsy, so a selection that narrowed to nothing
        upstream would silently widen to everything here and load every
        checkpoint in the registry. Omit the field to mean all.
        """
        if value is not None and not value:
            raise ValueError(
                "embedding_config_ids cannot be an empty list; omit it to count "
                "every configuration"
            )
        return value


@dataclass(frozen=True)
class _Checkpoint:
    """One distinct model to load, and the configurations that share it."""

    backend: str
    model_name: str
    config_ids: tuple[str, ...]


@dataclass(frozen=True)
class _Count:
    """What one load produced. ``loaded`` and ``executed`` differ for ProtST."""

    loaded: int
    executed: int
    module: str


def _select_configs(
    session: Session, payload: CountBackendParametersPayload
) -> list[EmbeddingConfig]:
    stmt = select(EmbeddingConfig)
    if payload.embedding_config_ids:
        stmt = stmt.where(EmbeddingConfig.id.in_(payload.embedding_config_ids))
    if payload.only_missing:
        stmt = stmt.where(EmbeddingConfig.param_count.is_(None))
    # Ordered so a partial run is resumable in a stable order rather than
    # whatever order the planner happens to return.
    return list(session.execute(stmt.order_by(EmbeddingConfig.model_name)).scalars())


def group_by_checkpoint(configs: list[EmbeddingConfig]) -> list[_Checkpoint]:
    """One entry per distinct ``(backend, model_name)``, carrying its configs.

    Grouping is the whole reason this is cheap. Eight canonical configurations
    resolve to five distinct checkpoints, and ProstT5 alone is 22 GB on disk.
    """
    grouped: dict[tuple[str, str], list[str]] = {}
    for config in configs:
        grouped.setdefault((config.model_backend, config.model_name), []).append(
            str(config.id)
        )
    return [
        _Checkpoint(backend, model_name, tuple(ids))
        for (backend, model_name), ids in grouped.items()
    ]


def _count_parameters(module: Any) -> int:
    return sum(int(p.numel()) for p in module.parameters())


def count_checkpoint(plugin: Any, checkpoint: _Checkpoint, device: str, emit: EmitFn) -> _Count:
    """Load one checkpoint the way its backend loads it and count what runs.

    The plugin is asked for the model rather than the checkpoint being opened
    here, so a backend that changes how it instantiates (encoder-only, a dtype, a
    pinned revision) changes this count with it instead of drifting away from it.
    """
    model, _tokenizer = plugin.load_model(checkpoint.model_name, device, emit)
    loaded = _count_parameters(model)

    attribute = EXECUTED_SUBMODULE.get(checkpoint.backend)
    if attribute is None:
        return _Count(loaded=loaded, executed=loaded, module="the whole loaded model")

    submodule = getattr(model, attribute, None)
    if submodule is None:
        # Loud rather than silent: falling back to the loaded total here would
        # publish a number that is wrong in the exact direction this operation
        # exists to correct.
        raise ValueError(
            f"backend {checkpoint.backend!r} declares that it executes "
            f"{attribute!r}, but the loaded model has no such attribute; the "
            "backend's forward path changed and EXECUTED_SUBMODULE is stale"
        )
    return _Count(loaded=loaded, executed=_count_parameters(submodule), module=attribute)


def _resolve_plugin(plugins: dict[str, Any], backend: str) -> Any:
    """The named backend, or a refusal that says which name was missing.

    A configuration can outlive the installation that produced it, so a missing
    plugin is a real state rather than a programming error, and the message has
    to name the backend or the reader is left guessing which of eight it was.
    """
    plugin = plugins.get(backend)
    if plugin is None:
        raise ValueError(
            f"no backend plugin named {backend!r} in group "
            f"{BACKEND_PLUGIN_GROUP!r}; the configuration names a backend "
            "this installation does not carry"
        )
    return plugin


def _report(checkpoint: _Checkpoint, count: _Count, emit: EmitFn) -> dict[str, Any]:
    """Emit one checkpoint's result and return the record for the job result.

    The loaded total travels beside the executed one everywhere. Reporting only
    the executed number would leave a reader unable to tell a small model from a
    large model whose second tower never runs.
    """
    gap = count.loaded - count.executed
    record = {
        "backend": checkpoint.backend,
        "model_name": checkpoint.model_name,
        "params_executed": count.executed,
        "params_loaded": count.loaded,
        "executed_module": count.module,
        "config_ids": list(checkpoint.config_ids),
    }
    emit(
        "params.counted",
        f"{checkpoint.model_name}: {count.executed:,} executed"
        + (f", {gap:,} loaded but never run" if gap else ""),
        {**record, "configs": len(checkpoint.config_ids)},
        "info",
    )
    return record


class CountBackendParametersOperation(Operation):
    name = "count_backend_parameters"
    description = (
        "Load each distinct embedding checkpoint through its own backend plugin "
        "and record the parameters its forward path executes on the matching "
        "embedding_config rows. Counts the executed module rather than the "
        "published total, because the deployed ProtT5 is encoder-only and ProtST "
        "runs its protein tower alone. Writes embedding_config.param_count."
    )

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = CountBackendParametersPayload.model_validate(contract_payload(payload))
        configs = _select_configs(session, p)
        checkpoints = group_by_checkpoint(configs)

        emit(
            "params.start",
            f"{len(configs)} configurations resolve to {len(checkpoints)} checkpoints",
            {"configs": len(configs), "checkpoints": len(checkpoints)},
            "info",
        )
        if not checkpoints:
            return OperationResult(result={"checkpoints": 0, "configs_updated": 0, "counts": []})

        plugins = discover_plugins(BACKEND_PLUGIN_GROUP)
        by_id = {str(c.id): c for c in configs}
        counts: list[dict[str, Any]] = []
        updated = 0

        for checkpoint in checkpoints:
            count = count_checkpoint(
                _resolve_plugin(plugins, checkpoint.backend), checkpoint, p.device, emit
            )
            counts.append(_report(checkpoint, count, emit))

            if not p.dry_run:
                for config_id in checkpoint.config_ids:
                    by_id[config_id].param_count = count.executed
                    updated += 1

        emit(
            "params.done",
            f"{updated} configuration rows updated" + (" (dry run: none)" if p.dry_run else ""),
            {"configs_updated": updated, "dry_run": p.dry_run},
            "info",
        )
        return OperationResult(
            result={
                "checkpoints": len(checkpoints),
                "configs_updated": updated,
                "counts": counts,
                "dry_run": p.dry_run,
                # Said out loud because a reader comparing these against a paper
                # will otherwise conclude the smaller numbers are a bug.
                "caveat": (
                    "param_count records the parameters the backend's forward path "
                    "executes, not the checkpoint total; the deployed ProtT5 is "
                    "encoder-only and ProtST executes its protein tower alone, so "
                    "both sit below their published figures"
                ),
            }
        )

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        ids = payload.get("embedding_config_ids")
        scope = f"{len(ids)} configurations" if ids else "every configuration missing a count"
        return f"count executed parameters for {scope}"
