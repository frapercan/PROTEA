"""Fitting the projector, and the negatives that keep it honest.

The loss is a hinge on a contrast, as in the ontology encoder, but the failure
mode it guards against is different and much easier to fall into. A protein's
point can be pushed out until every term subsumes it, at which point every
positive costs zero and the model has learned to predict the whole ontology.
Only the negatives stop that, so the sampler is the design.
"""

from __future__ import annotations

import random
from collections.abc import Callable, Sequence

import torch
from torch import Tensor

from protea.core.ontology.dag import Dag
from protea.core.ontology.protein_projector import ProjectorConfig, ProteinProjector


class StructuralNegatives:
    """Siblings first, denials second, random last.

    A SIBLING of a term the protein has is the hardest negative available: one
    step away in the graph, sharing a parent, and still false. A CURATED
    DENIAL is the only certain negative in the corpus, and there are only 5,603
    of them. A RANDOM term is a weak negative and is used only to fill, because
    a protein whose terms all sit in one corner of the ontology would otherwise
    never be told about the rest of it.

    Nothing the protein actually has is ever emitted, whatever route produced
    it. That check is not paranoia: siblings in a DAG can be related through a
    second path, and the propagated term set is much larger than the direct one.
    """

    def __init__(self, dag: Dag, denials: dict[str, set[str]], seed: int) -> None:
        self.dag = dag
        self.denials = denials
        self.rng = random.Random(seed)

    def for_protein(self, accession: str, has: set[str], n: int) -> list[str]:
        pool: list[str] = []
        for term in self.rng.sample(sorted(has), min(len(has), 16)):
            pool.extend(self.dag.siblings_of(term) - has)
        pool.extend(self.denials.get(accession, set()) - has)
        self.rng.shuffle(pool)
        out = [t for t in dict.fromkeys(pool) if t not in has][:n]
        while len(out) < n:
            cand = self.rng.choice(self.dag.terms)
            if cand not in has:
                out.append(cand)
        return out


def _pad(rows: Sequence[Sequence[int]], width: int) -> Tensor:
    """A ragged term list as a rectangle, repeating rather than padding.

    A pad id would be a real term and would be scored as one. Repeating an
    entry the protein genuinely has only reweights it, which is harmless.
    """
    out = []
    for r in rows:
        r = list(r) or [0]
        out.append([r[i % len(r)] for i in range(width)])
    return torch.tensor(out, dtype=torch.long)


def fit_projector(
    model: ProteinProjector,
    batches: Callable[[], object],
    config: ProjectorConfig,
    *,
    log: Callable[[str], None] | None = None,
) -> ProteinProjector:
    """Train on (embedding, positive ids, negative ids) batches.

    ``batches`` is a callable returning a fresh iterable each epoch, so the
    caller owns how the data is drawn and this function owns only the step.
    """
    opt = torch.optim.Adam(model.parameters(), lr=config.lr)
    model.train()
    for epoch in range(config.epochs):
        total, seen = 0.0, 0
        for x, pos, neg in batches():  # type: ignore[attr-defined]
            point = model(x.to(config.device))
            loss = model.penalty(point, pos.to(config.device)).mean() + torch.clamp(
                config.margin - model.penalty(point, neg.to(config.device)), min=0.0
            ).mean()
            opt.zero_grad()
            loss.backward()
            opt.step()
            total += float(loss.detach()) * len(x)
            seen += len(x)
        if log is not None:
            log(f"    epoch {epoch + 1:3d}/{config.epochs}  loss {total / max(seen, 1):.4f}")
    model.eval()
    return model
