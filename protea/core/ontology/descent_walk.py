"""Running the descent, and fitting it.

Kept apart from the model because the walk is where the method's behaviour
lives: what it emits, when it stops, and what it refuses to consider. That is
worth reading without a neural network in the way.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable

import torch
from torch import Tensor

from protea.core.ontology.dag import Dag
from protea.core.ontology.descent import DescentConfig, DescentModel, frontier


def _extra(dag: Dag, terms: list[str], step: int, accepted: set[str]) -> Tensor:
    """Per-decision state the term's own embedding cannot carry.

    How deep the walk is, because a term reached after six steps is being
    asserted about a protein on much thinner grounds than one reached at the
    first. And what share of the term's parents were accepted, which is always
    one at inference but is informative during training, where the boundary is
    computed against a target set rather than against a walk.
    """
    rows = []
    for t in terms:
        parents = dag.parents_of(t)
        share = sum(p in accepted for p in parents) / max(len(parents), 1)
        rows.append([step / 10.0, share])
    return torch.tensor(rows, dtype=torch.float32)


def walk(
    model: DescentModel,
    context: Tensor,
    *,
    tau: float | None = None,
    seeds: Iterable[str] | None = None,
) -> dict[str, float]:
    """Generate one protein's annotation set. Returns term to probability.

    Starts from the ontology's roots, or from ``seeds`` when the protein
    already has annotations, which is what makes this usable for a protein with
    history as well as for one with none. Stops when a step accepts nothing,
    so the length of the output is the model's decision and not a parameter.
    """
    dag = model.dag
    cut = model.config.tau if tau is None else tau
    accepted = set(seeds) if seeds is not None else set(dag.roots())
    scores: dict[str, float] = {}
    model.eval()
    with torch.no_grad():
        for step in range(model.config.max_steps):
            candidates = sorted(frontier(dag, accepted))
            if not candidates:
                break
            ids = torch.tensor([dag.index[t] for t in candidates], dtype=torch.long)
            ctx = context.unsqueeze(0).expand(len(candidates), -1)
            p = torch.sigmoid(model(ctx, ids, _extra(dag, candidates, step, accepted)))
            taken = [t for t, v in zip(candidates, p.tolist(), strict=True) if v >= cut]
            for t, v in zip(candidates, p.tolist(), strict=True):
                scores[t] = max(scores.get(t, 0.0), v)
            if not taken:
                break
            accepted.update(taken)
    return {t: v for t, v in scores.items() if t in accepted}


def fit_descent(
    model: DescentModel,
    batches: Callable[[], Iterable[tuple[Tensor, Tensor, Tensor, Tensor]]],
    config: DescentConfig,
    *,
    log: Callable[[str], None] | None = None,
) -> DescentModel:
    """Train on (context, term ids, extra, label) batches.

    Plain binary cross-entropy on the boundary decisions. The positives are the
    terms a walk would have had to enter and the negatives are the terms one
    step outside, which is the only comparison the walk ever actually makes.
    Nothing here is weighted by information accretion: that belongs to the
    threshold and to the metric, and putting it in the loss as well would
    count it twice.
    """
    opt = torch.optim.Adam(model.parameters(), lr=config.lr)
    loss_fn = torch.nn.BCEWithLogitsLoss()
    for epoch in range(config.epochs):
        model.train()
        total, seen = 0.0, 0
        for ctx, ids, extra, y in batches():
            logits = model(ctx.to(config.device), ids.to(config.device), extra.to(config.device))
            loss = loss_fn(logits, y.to(config.device))
            opt.zero_grad()
            loss.backward()
            opt.step()
            total += float(loss.detach()) * len(y)
            seen += len(y)
        if log is not None:
            log(f"    epoch {epoch + 1:3d}/{config.epochs}  loss {total / max(seen, 1):.4f}")
    model.eval()
    return model
