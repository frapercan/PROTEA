"""One forward pass, several layer depths.

``model(**tokens, output_hidden_states=True)`` returns every layer and the
caller keeps one, so embedding a model at four depths ran the same forward pass
four times and discarded the rest. ``compute_embeddings`` already carried a note
saying a multi-configuration emitter "is only worth building if that fraction
dominates, because what it saves is exactly the redundant passes". The fraction
is now measured: ``inference_fraction`` 0.999. It dominates.

WHY THIS IS EXACT, and not an approximation traded for speed. The backend
aggregates the selected layers before pooling, and ``layer_agg="concat"``
aggregates by concatenation. Mean pooling runs over the residue axis and is
linear and per-dimension, so pooling a concatenation equals concatenating the
poolings. Slicing the pooled vector therefore recovers exactly what a
single-layer pass produces; only the final L2 normalisation has to be redone per
slice, because it was applied to the whole concatenation.

Verified before the plumbing was written, against separate passes in the same
process and on the same device: cosine 1.00000000, maximum absolute difference
2e-8. Comparing against the STORED vectors instead gives 0.99997, which measures
halfvec quantisation and GPU-versus-CPU kernels rather than this algebra -- the
kind of reference mismatch that would have read as a failure of the thing under
test.

The whole change lives in PROTEA. ``protea-backends`` is pinned by commit and
already offers ``concat``, so nothing here needs a dependency bump.
"""

from __future__ import annotations

from typing import Any

__all__ = ["MultiLayerPlan", "plan_for", "split_pooled", "split_write_sequences"]

#: Identity fields that every configuration in one pass must agree on. It is
#: every field the recipe has except the layer, because the layer is the only
#: thing a shared forward pass can vary after the fact.
_MUST_MATCH: tuple[str, ...] = (
    "chunk_overlap",
    "chunk_size",
    "embedding_scale",
    "max_length",
    "model_backend",
    "model_name",
    "normalize",
    "normalize_residues",
    "pooling",
    "use_chunking",
)


class MultiLayerPlan:
    """Which layers one pass must request, and which slice each config takes.

    ``layer_indices`` is what the shared pass asks for, ascending, because the
    backend sorts its request and concatenates in that order. ``slot_of`` maps a
    config id to its position in that concatenation.
    """

    def __init__(self, layer_indices: list[int], slot_of: dict[str, int]) -> None:
        self.layer_indices = layer_indices
        self.slot_of = slot_of

    def __len__(self) -> int:
        return len(self.layer_indices)


def plan_for(configs: list[Any]) -> MultiLayerPlan:
    """Group configurations that can share one forward pass.

    Raises when they cannot. A mismatch means the shared pass would produce
    vectors for a recipe nobody asked for, and it would do so silently: the
    stored rows would carry the right config id and the wrong contents, which
    is the failure this campaign keeps meeting under other names.

    Every configuration must select exactly one layer. A config already
    aggregating several is not a depth on the layer axis, and folding it in
    here would put two different quantities in one bar.
    """
    if not configs:
        raise ValueError("no configurations to plan for")

    head = configs[0]
    for cfg in configs[1:]:
        differing = [f for f in _MUST_MATCH if getattr(cfg, f, None) != getattr(head, f, None)]
        if differing:
            raise ValueError(
                f"{getattr(cfg, 'display_name', cfg.id)} cannot share a forward pass "
                f"with {getattr(head, 'display_name', head.id)}: they differ in "
                f"{', '.join(differing)}. Only the layer may vary."
            )

    slot_of: dict[str, int] = {}
    layers: list[int] = []
    for cfg in configs:
        got = list(cfg.layer_indices or [])
        if len(got) != 1:
            raise ValueError(
                f"{getattr(cfg, 'display_name', cfg.id)} selects {len(got)} layers; "
                "a shared pass splits one layer per configuration, and a config that "
                "already aggregates several is not a depth on this axis."
            )
        if got[0] not in layers:
            layers.append(got[0])

    layers.sort()  # the backend sorts its request; the concat follows that order
    for cfg in configs:
        slot_of[str(cfg.id)] = layers.index(cfg.layer_indices[0])
    return MultiLayerPlan(layers, slot_of)


def split_pooled(vector: list[float], slot: int, n_slots: int, normalize: bool) -> list[float]:
    """Take one configuration's slice out of a concatenated pooled vector.

    ``normalize`` re-applies the unit norm the concatenation consumed. Skipping
    it would leave every slice shorter than one, by a factor that depends on how
    much of the concatenation's length the other layers held -- a per-layer
    scaling that no downstream reader could see or undo.
    """
    width, extra = divmod(len(vector), n_slots)
    if extra:
        raise ValueError(
            f"a {len(vector)}-wide vector does not divide into {n_slots} equal slices; "
            "the pass and the plan disagree about how many layers were requested"
        )
    piece = vector[slot * width : (slot + 1) * width]
    if not normalize:
        return piece
    norm = sum(x * x for x in piece) ** 0.5
    if norm == 0.0:
        return piece
    return [x / norm for x in piece]


def split_write_sequences(
    write_sequences: list[dict], slot: int, n_slots: int, normalize: bool
) -> list[dict]:
    """Rebuild one configuration's serialized batch out of a shared pass.

    The dicts the write worker consumes carry the vector and its width, so both
    have to be narrowed together: leaving ``embedding_dim`` at the
    concatenation's width would store a correct vector under a declared size it
    does not have, and the column would take it.
    """
    return [
        {
            "sequence_id": seq["sequence_id"],
            "chunks": [
                {
                    "chunk_index_s": c["chunk_index_s"],
                    "chunk_index_e": c["chunk_index_e"],
                    "vector": (piece := split_pooled(c["vector"], slot, n_slots, normalize)),
                    "embedding_dim": len(piece),
                }
                for c in seq["chunks"]
            ],
        }
        for seq in write_sequences
    ]
