"""The ontology as a graph, with nothing in it that is specific to GO.

A Dag is built from an edge list of (parent, child) string pairs. Where those
came from, whether a database, an OBO file or another ontology entirely, is the
caller's problem. That is the whole point of building this ourselves.
"""

from __future__ import annotations

import random
from collections import deque
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field

#: How deep a transitive closure is allowed to walk. GO's longest is_a chain is
#: well inside this; the bound exists so a cyclic edge list cannot hang.
_MAX_DEPTH = 32


@dataclass(frozen=True)
class EdgeSplit:
    """Direct edges held out for evaluation, and what is left to train on.

    Splitting on EDGES rather than on terms is deliberate. Holding out terms
    would ask the encoder to place something it has never seen, which is a
    different and much harder question than the one this encoder is for. What
    is asked here is whether the geometry it learned from the rest of the
    ontology puts a known pair in the right relation.
    """

    train: tuple[tuple[str, str], ...]
    test: tuple[tuple[str, str], ...]

    def __post_init__(self) -> None:
        if set(self.train) & set(self.test):
            raise ValueError("an edge cannot be in both halves of the split")


@dataclass
class Dag:
    """A directed acyclic graph over term identifiers.

    Edges point parent to child, which is the direction subsumption reads in:
    the child is a kind of, or a part of, the parent.
    """

    edges: tuple[tuple[str, str], ...]
    terms: tuple[str, ...] = field(init=False)
    index: dict[str, int] = field(init=False, repr=False)
    _children: dict[str, list[str]] = field(init=False, repr=False)
    _parents: dict[str, list[str]] = field(init=False, repr=False)
    #: Memoised traversals. Every one of these is asked for the same term
    #: thousands of times: the closure walks up from each term, the evaluation
    #: excludes a child's ancestors for every held-out edge, and the negative
    #: sampler asks for siblings once per term per protein per epoch. Without
    #: the cache that last one alone is millions of breadth-first searches, and
    #: it was measured taking longer than the training it feeds.
    _up: dict[str, set[str]] = field(init=False, repr=False, default_factory=dict)
    _down: dict[str, set[str]] = field(init=False, repr=False, default_factory=dict)
    _sib: dict[str, set[str]] = field(init=False, repr=False, default_factory=dict)

    def __post_init__(self) -> None:
        children: dict[str, list[str]] = {}
        parents: dict[str, list[str]] = {}
        for parent, child in self.edges:
            children.setdefault(parent, []).append(child)
            parents.setdefault(child, []).append(parent)
            children.setdefault(child, [])
            parents.setdefault(parent, [])
        self._children = children
        self._parents = parents
        self.terms = tuple(sorted(children))
        self.index = {t: i for i, t in enumerate(self.terms)}

    def children_of(self, term: str) -> list[str]:
        return self._children.get(term, [])

    def parents_of(self, term: str) -> list[str]:
        return self._parents.get(term, [])

    def roots(self) -> list[str]:
        """Terms nothing subsumes. GO has three; another ontology may have one."""
        return [t for t in self.terms if not self._parents.get(t)]

    def ancestors(self, term: str) -> set[str]:
        """Everything above ``term``, itself excluded."""
        hit = self._up.get(term)
        if hit is not None:
            return hit
        seen: set[str] = set()
        queue = deque(self._parents.get(term, []))
        depth = 0
        while queue and depth < _MAX_DEPTH * len(self.terms):
            node = queue.popleft()
            depth += 1
            if node in seen:
                continue
            seen.add(node)
            queue.extend(self._parents.get(node, []))
        self._up[term] = seen
        return seen

    def descendants(self, term: str) -> set[str]:
        """Everything below ``term``, itself excluded."""
        hit = self._down.get(term)
        if hit is not None:
            return hit
        seen: set[str] = set()
        queue = deque(self._children.get(term, []))
        while queue:
            node = queue.popleft()
            if node in seen:
                continue
            seen.add(node)
            queue.extend(self._children.get(node, []))
        self._down[term] = seen
        return seen

    def siblings_of(self, term: str) -> set[str]:
        """Terms sharing a parent with ``term`` and standing in no relation to it.

        These are the negatives worth training on. A protein that has a parent
        and one of its children usually does not have the other children, and
        that is a far larger and more informative set than the 5,603 curated
        denials. A sibling is also the hardest possible negative: adjacent in
        the graph, one step from a term the protein really has, and still
        false.

        Anything that subsumes or is subsumed by ``term`` is removed. In a DAG
        two children of one parent can still be related through another path,
        and calling such a pair a negative would be teaching a falsehood.
        """
        hit = self._sib.get(term)
        if hit is not None:
            return hit
        out: set[str] = set()
        for parent in self._parents.get(term, []):
            out.update(self._children.get(parent, []))
        out = out - {term} - self.ancestors(term) - self.descendants(term)
        self._sib[term] = out
        return out

    def closure(self) -> Iterator[tuple[str, str]]:
        """Every (ancestor, descendant) pair the direct edges imply.

        Yielded rather than returned: on GO this is 783,477 pairs, which is
        fine to hold but there is no reason to make every caller hold it.
        """
        for term in self.terms:
            for up in self.ancestors(term):
                yield up, term

    def split_edges(self, *, held_out: float, seed: int) -> EdgeSplit:
        """Hold out a fraction of the direct edges.

        An edge whose child would be left with no other parent is never held
        out. Removing it would disconnect the child from the ontology entirely,
        and then the test measures whether the encoder can guess an isolated
        term's position, which is not what is being asked.
        """
        rng = random.Random(seed)
        pool = [e for e in self.edges if len(self._parents.get(e[1], [])) > 1]
        rng.shuffle(pool)
        n = int(len(self.edges) * held_out)
        test = tuple(pool[:n])
        held = set(test)
        return EdgeSplit(train=tuple(e for e in self.edges if e not in held), test=test)

    @classmethod
    def from_pairs(cls, pairs: Iterable[tuple[str, str]]) -> Dag:
        return cls(edges=tuple(dict.fromkeys(pairs)))
