"""What the substrate node stands on: the representations the record holds.\n\nKept apart from the node builders because a representation is a thing that\nexists whether or not any node has an opinion about it, and the two answer\ndifferent questions. The node says how firmly a choice is held; this says\nwhat there was to choose from and which of it was ever built."""

from __future__ import annotations

from typing import Any

#
# The Substrate node reports a ratio, and a ratio is where this record is least
# readable. "1 of 13" says that twelve alternatives were passed over and names
# neither the one that was used nor the twelve that were not, so a reader cannot
# tell a deliberate choice from an accident, cannot see that two of the thirteen
# are the same backbone read at two different layers, and cannot see that three
# of them were FITTED against an annotation release rather than shipped
# pretrained. Those three are not another setting of the same knob.
#
# Nothing below decides anything. It expands the denominator of a ratio the node
# already publishes, using the rows the node already counted.

#: What a representation is, and the three states are exhaustive and ordered.
#:
#: ``retrieved``  a prediction set was computed in it.
#: ``built``      it holds stored embeddings and nothing was ever retrieved in
#:                it. A real alternative, passed over.
#: ``unbuilt``    it holds no stored embedding. NOT an untried alternative: an
#:                unbuilt one. Counting it as available would overstate what was
#:                passed over, which is why the node's own denominator excludes
#:                it and why it is still listed rather than dropped.
RETRIEVED = "retrieved"
BUILT = "built"
UNBUILT = "unbuilt"

_REPRESENTATION_ORDER: dict[str, int] = {RETRIEVED: 0, BUILT: 1, UNBUILT: 2}


def _is_true(value: Any) -> bool:
    """Read a boolean the reads hand back as text.

    Every flag on the substrate read is cast to text in SQL so the node can put
    it in front of a reader unchanged. Anything that is not the word true is
    false, including None, which is what an absent column means here.
    """
    return str(value).lower() == "true"


def _coverage(stored: int, corpus: int | None, chunked: bool) -> float | None:
    """Stored rows as a fraction of the corpus, or nothing.

    Null in two cases, and both are refusals rather than gaps. With chunking on,
    a stored row is one chunk of a sequence and not a sequence, so the ratio
    would count a long protein several times and could exceed one; with no
    corpus count there is nothing to divide by. A plausible wrong coverage is
    indistinguishable from a right one, so neither is guessed at.
    """
    if chunked or not corpus:
        return None
    return stored / corpus


def build_representations(record: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    """Every registered representation, with what was ever done in it.

    The counts at the top are the Substrate node's own ratio restated, built
    from the same rows, so the section cannot disagree with the node it expands.

    ``param_count`` is passed through exactly as stored, null included. It is
    null for several of these and nothing here orders by it or fills it in: a
    column missing for half the rows cannot rank them, and a size nobody
    recorded is a fact about the record.
    """
    rows = record.get("substrates") or []
    corpus_rows = record.get("corpus") or []
    corpus = corpus_rows[0].get("sequences") if corpus_rows else None

    # Predictions and scored results per representation, counted off the rows
    # the rest of the graph already read. A representation that produced neither
    # is the whole point of the section, so both are reported at zero rather
    # than omitted.
    sets_by_config: dict[str, int] = {}
    for ps in record.get("prediction_sets") or []:
        key = ps.get("embedding_config_id")
        if key:
            sets_by_config[key] = sets_by_config.get(key, 0) + 1
    results_by_config: dict[str, int] = {}
    for r in record.get("results") or []:
        key = r.get("embedding_config_id")
        if key:
            results_by_config[key] = results_by_config.get(key, 0) + 1

    out: list[dict[str, Any]] = []
    for row in rows:
        state = RETRIEVED if row.get("in_use") else BUILT if row.get("producible") else UNBUILT
        stored = int(row.get("stored") or 0)
        chunked = _is_true(row.get("use_chunking"))
        trained_on_id = row.get("trained_on_id")
        out.append(
            {
                "id": row.get("id"),
                "label": row.get("label"),
                "display_name": row.get("display_name"),
                "model_name": row.get("model_name"),
                "model_backend": row.get("model_backend"),
                "family": row.get("family"),
                "param_count": row.get("param_count"),
                "layer_indices": row.get("layer_indices"),
                "layer_agg": row.get("layer_agg"),
                "pooling": row.get("pooling"),
                "normalize": _is_true(row.get("normalize")),
                "normalize_residues": _is_true(row.get("normalize_residues")),
                "max_length": row.get("max_length"),
                "use_chunking": chunked,
                "embeddings_stored": stored,
                "coverage": _coverage(stored, corpus, chunked),
                "state": state,
                # Fitted against a release, or shipped pretrained. Null here is
                # "not fitted", which is a positive statement about a pretrained
                # backbone and not a missing value.
                "trained_on": (
                    {
                        "annotation_set_id": trained_on_id,
                        "source": row.get("trained_on_source"),
                        "version": row.get("trained_on_version"),
                        "published_at": row.get("trained_on_date"),
                    }
                    if trained_on_id
                    else None
                ),
                "prediction_sets": sets_by_config.get(row.get("id") or "", 0),
                "results": results_by_config.get(row.get("id") or "", 0),
            }
        )

    # Retrieved first, then built, then unbuilt, alphabetical inside each. Never
    # by parameter count, which is absent for several of them, and never by
    # coverage, which would read as a ranking of representations by a number
    # that says only how much of the corpus was encoded.
    out.sort(key=lambda r: (_REPRESENTATION_ORDER[r["state"]], (r["label"] or "").lower()))
    return {
        "corpus_sequences": corpus,
        "total": len(out),
        "built": sum(1 for r in out if r["state"] != UNBUILT),
        "retrieved": sum(1 for r in out if r["state"] == RETRIEVED),
        "rows": out,
    }
