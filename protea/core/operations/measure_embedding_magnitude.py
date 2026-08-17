# protea/core/operations/measure_embedding_magnitude.py
"""Measure how large an un-normalised pooled vector actually gets, before storing one.

Every embedding this platform has written is L2-normalised, so every stored
component sits in [-1, 1] and the question below has never come up. It comes up
now because the ablation programme needs the un-normalised vector: normalisation
is applied inside the chunk loop, which makes it a per-window length-equalising
reweighting rather than a formatting step, and an arm that does not apply it
cannot be recovered from anything on disk.

The hazard is quiet. Embeddings are stored as ``halfvec``, whose range tops out
near 65504, and ``scale_and_clip_embedding`` is a pure passthrough when
``embedding_scale`` is 1.0, which every live config uses. So the guard that would
clip an out-of-range component is correct, present, and not on the path. An
un-normalised component above the ceiling would reach the column, become an inf,
and produce a bank of plausible and worthless vectors with nothing raising.

This operation answers the one question that decides the scale: what is the
largest absolute component a pooled vector of this configuration actually
produces. It writes nothing. It exists as a registered operation rather than as a
script because the scale it recommends will be baked into a corpus-wide pass, and
a number chosen from a procedure that lives on one disk is a number nobody can
re-derive.

Sampling is stratified by length and the reason is not tidiness. Pooling divides
by the residue count, so magnitude and length are not independent, and a sample
drawn without regard to length would under-represent exactly the regime where the
extremes live. The bands are the campaign's own.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated, Any

from pydantic import Field
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from protea.core.contracts.operation import (
    EmitFn,
    Operation,
    OperationResult,
    ProteaPayload,
)
from protea.infrastructure.orm.models.embedding.embedding_config import EmbeddingConfig
from protea.infrastructure.orm.models.sequence.sequence import Sequence

#: The fp16 ceiling ``halfvec`` inherits. A component above this is an inf once
#: written, and nothing downstream distinguishes an inf from a real coordinate.
HALFVEC_MAX = 65504.0

#: How much room to leave between the largest component observed on a SAMPLE and
#: the ceiling. Four, because a sample maximum understates a population maximum
#: and the cost of being wrong is a silent corpus of infinities, while the cost
#: of a scale two doublings too small is nothing at all: cosine retrieval is
#: scale invariant and the per-dimension standardisation absorbs a uniform
#: divisor, so the arm is the raw geometry either way.
HEADROOM = 4.0

#: The campaign's length bands, used everywhere a result is stratified.
LENGTH_BANDS: tuple[tuple[str, int, int], ...] = (
    ("short", 1, 512),
    ("chunkable", 513, 2048),
    ("long", 2049, 1_000_000),
)


PositiveInt = Annotated[int, Field(gt=0)]


class MeasureEmbeddingMagnitudePayload(ProteaPayload, frozen=True):
    """What to measure, and how widely.

    ``per_band`` is per length band rather than a total, so the extremes of the
    long band are not crowded out by the short one. Magnitude and length are not
    independent, since pooling divides by the residue count.
    """

    embedding_config_id: str
    per_band: PositiveInt = 200
    seed: int = 42
    device: str = "cuda"
    batch_size: PositiveInt = 1


class _UnnormalisedConfig:
    """A read-only view of a config with ``normalize`` forced off.

    The backend reads its recipe off the config object, so measuring the
    un-normalised vector means handing it a config that says so. Mutating the
    session-attached row would risk flushing a recipe change to a table whose
    identifiers are derived from that recipe, so this forwards every attribute
    except the one under test.
    """

    def __init__(self, config: EmbeddingConfig) -> None:
        self._config = config

    def __getattr__(self, name: str) -> Any:
        return getattr(self._config, name)

    @property
    def normalize(self) -> bool:
        return False


def _sample_sequence_ids(
    session: Session, per_band: int, seed: int
) -> dict[str, list[int]]:
    """Sequence ids per length band, ordered and then sampled deterministically.

    ``ORDER BY id`` before sampling rather than relying on scan order, because a
    seeded shuffle over an unordered result selects a different sample on every
    run and this project has already paid for that once.
    """
    import random

    rng = random.Random(seed)
    sampled: dict[str, list[int]] = {}
    length = func.length(Sequence.sequence)
    for name, low, high in LENGTH_BANDS:
        rows = session.execute(
            select(Sequence.id)
            .where(length >= low, length <= high)
            .order_by(Sequence.id)
        ).scalars().all()
        ids = list(rows)
        if len(ids) > per_band:
            ids = rng.sample(ids, per_band)
        sampled[name] = sorted(ids)
    return sampled


def _recommend_scale(observed_max: float) -> float:
    """Smallest power of two keeping ``observed_max`` inside the ceiling with headroom.

    Powers of two so the divisor is exact in binary floating point and a rescaled
    value round-trips without introducing error of its own.
    """
    if observed_max <= 0.0:
        return 1.0
    limit = HALFVEC_MAX / HEADROOM
    scale = 1.0
    while observed_max / scale > limit:
        scale *= 2.0
    return scale


def _summarise(values: list[float]) -> dict[str, float]:
    """Maximum and a few quantiles, on a list of per-vector maxima."""
    if not values:
        return {}
    ordered = sorted(values)
    n = len(ordered)

    def at(q: float) -> float:
        return ordered[min(n - 1, int(q * n))]

    return {
        "max": ordered[-1],
        "p99": at(0.99),
        "p50": at(0.50),
        "min": ordered[0],
        "n": float(n),
    }


@dataclass(frozen=True)
class _Inference:
    """Everything the forward pass needs, bundled so it travels as one thing."""

    model: Any
    tokenizer: Any
    recipe: Any
    device: str
    batch_size: int


def _measure_bands(
    session: Session,
    inference: _Inference,
    sampled: dict[str, list[int]],
    emit: EmitFn,
) -> dict[str, list[float]]:
    """Largest absolute component of every pooled chunk, kept per length band.

    One value per chunk rather than per sequence, because a chunked recipe writes
    one row per chunk and the ceiling applies to each of them independently.
    """
    from protea.core.operations.compute_embeddings import _dispatch_embed

    per_band: dict[str, list[float]] = {}
    for band, ids in sampled.items():
        if not ids:
            continue
        maxima: list[float] = []
        for start in range(0, len(ids), inference.batch_size):
            window = ids[start : start + inference.batch_size]
            seqs = session.execute(
                select(Sequence.sequence).where(Sequence.id.in_(window))
            ).scalars().all()
            for chunks in _dispatch_embed(
                inference.model, inference.tokenizer, list(seqs),
                inference.recipe, inference.device,
            ):
                maxima.extend(float(abs(chunk.vector).max()) for chunk in chunks)
        per_band[band] = maxima
        summary = _summarise(maxima)
        emit(
            "magnitude.band",
            f"{band}: max |component| {summary.get('max', 0.0):.2f}",
            {"band": band, **summary},
            "info",
        )
    return per_band


def _verdict(per_band_maxima: dict[str, list[float]]) -> dict[str, Any]:
    """The number that decides the scale, with the caveat it must be quoted beside.

    ``observed_max`` is a sample maximum and will be read as a population one, so
    the caveat travels in the payload rather than in a docstring nobody opens.
    """
    overall = [v for values in per_band_maxima.values() for v in values]
    observed_max = max(overall) if overall else 0.0
    scale = _recommend_scale(observed_max)
    fits = observed_max * HEADROOM <= HALFVEC_MAX
    tail = (
        "fits fp16 with headroom at scale 1.0" if fits
        else f"needs embedding_scale {scale:g}"
    )
    return {
        "message": f"largest component {observed_max:.2f}; {tail}",
        "observed_max": observed_max,
        "halfvec_max": HALFVEC_MAX,
        "headroom": HEADROOM,
        "recommended_embedding_scale": scale,
        "fits_at_scale_one": fits,
        "caveat": (
            "observed_max is the maximum over a stratified sample, not over the "
            f"corpus; the recommended scale carries a factor of {HEADROOM:g} of "
            "headroom for that reason"
        ),
    }


class MeasureEmbeddingMagnitudeOperation(Operation):
    name = "measure_embedding_magnitude"
    description = (
        "Forward-pass a length-stratified sample under an existing embedding config "
        "with normalisation disabled, and report the largest absolute component a "
        "pooled vector produces together with the embedding_scale that keeps it "
        "inside the halfvec range. Writes nothing."
    )

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = MeasureEmbeddingMagnitudePayload.model_validate(payload)
        config_id, per_band, seed = p.embedding_config_id, p.per_band, p.seed
        device, batch_size = p.device, p.batch_size

        config = session.get(EmbeddingConfig, config_id)
        if config is None:
            raise ValueError(f"no embedding config {config_id!r}")

        emit(
            "magnitude.start",
            f"measuring {config.model_name} with normalisation disabled",
            {"embedding_config_id": str(config_id), "per_band": per_band, "seed": seed},
            "info",
        )

        sampled = _sample_sequence_ids(session, per_band, seed)
        for band, ids in sampled.items():
            emit("magnitude.sample", f"{band}: {len(ids)} sequences", {"band": band,
                 "n": len(ids)}, "info")

        # Imported here rather than at module scope: the backend pulls in torch
        # and the model stack, and this module is imported by the catalogue on
        # every worker including those that will never run it.
        from protea.core.operations.compute_embeddings import _get_or_load_model

        model, tokenizer = _get_or_load_model(config, device, emit)
        inference = _Inference(
            model=model, tokenizer=tokenizer, recipe=_UnnormalisedConfig(config),
            device=device, batch_size=batch_size,
        )
        per_band_maxima = _measure_bands(session, inference, sampled, emit)

        verdict = _verdict(per_band_maxima)
        emit("magnitude.verdict", verdict.pop("message"), dict(verdict),
             "info" if verdict["fits_at_scale_one"] else "warn")

        return OperationResult(
            result={
                "embedding_config_id": str(config_id),
                "model_name": config.model_name,
                "seed": seed,
                "per_band": per_band,
                "bands": {b: _summarise(v) for b, v in per_band_maxima.items()},
                **verdict,
            }
        )

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        return (
            "measure un-normalised pooled magnitude for config "
            f"{payload.get('embedding_config_id')}"
        )
