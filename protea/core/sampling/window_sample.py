"""The stratified sample of a window's targets, by the rule ``equal_cells_proportional_strata``.

The units are the window's benchmark pairs: a target in one aspect where it gained
terms, with its category in that aspect (its cell) and its place on the window's two
sequence axes, the identity band and the length band of the window's placement (its
stratum). The rule gives each of the nine cells ``units_per_cell`` units, or every
unit it has when it has fewer, so the small no-knowledge cells keep a presence beside
the large partial-knowledge ones. Within a cell the quota is spread over the nonempty
strata: one unit each first (the largest strata first when the quota is smaller than
their number), then the rest in proportion to what each stratum holds beyond that
unit, by largest remainder (exact arithmetic; a tie goes to the larger stratum, then
to the earlier one in the canonical order), so no stratum is asked for more than it
holds. Within a stratum the units are taken in hash order
(:mod:`protea.core.sampling`), with the window's id as the scope.

The targets are the union of the selected units' proteins, and a target keeps every
cell it has, so a cell's realised count (its units whose protein is a target) is at
or above its quota. Everything here is pure: the operation reads the units from the
window and writes what :func:`draw_window_sample` returns.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from fractions import Fraction
from typing import Any

from protea.core.domain.aspect import Aspect
from protea.core.domain.category import Category
from protea.core.sampling import hash_order_key
from protea.core.strata import HomologyBand, LengthBand

__all__ = [
    "WINDOW_SAMPLE_RULES",
    "CellDraw",
    "StratumDraw",
    "Unit",
    "WindowSampleDraw",
    "allocate_cell",
    "draw_window_sample",
    "largest_remainder",
]

#: The rules a window sample can be drawn by.
WINDOW_SAMPLE_RULES: tuple[str, ...] = ("equal_cells_proportional_strata",)

#: The canonical orders: the cells as the platform reports them, the strata by band.
CATEGORY_ORDER: tuple[str, ...] = tuple(category.code for category in Category)
ASPECT_ORDER: tuple[str, ...] = tuple(aspect.code for aspect in Aspect)
IDENTITY_BAND_ORDER: tuple[str, ...] = tuple(band.value for band in HomologyBand)
LENGTH_BAND_ORDER: tuple[str, ...] = tuple(band.value for band in LengthBand)


@dataclass(frozen=True)
class Unit:
    """One benchmark pair of a window: a target in one aspect, its cell and its stratum."""

    accession: str
    aspect: str
    category: str
    identity_band: str
    length_band: str

    def __post_init__(self) -> None:
        for field, value, allowed in (
            ("aspect", self.aspect, ASPECT_ORDER),
            ("category", self.category, CATEGORY_ORDER),
            ("identity_band", self.identity_band, IDENTITY_BAND_ORDER),
            ("length_band", self.length_band, LENGTH_BAND_ORDER),
        ):
            if value not in allowed:
                raise ValueError(f"a unit's {field} is one of {allowed}, not {value!r}")

    @property
    def cell(self) -> tuple[str, str]:
        return (self.category, self.aspect)

    @property
    def stratum(self) -> tuple[str, str]:
        return (self.identity_band, self.length_band)


def largest_remainder(seats: int, weights: Sequence[int]) -> list[int]:
    """``seats`` shared in proportion to ``weights`` by largest remainder, exactly.

    Each weight gets the floor of its exact share, and the seats left go one each to
    the largest remainders; a tie goes to the larger weight, then to the earlier
    position. Zero weights get nothing unless every weight is zero, which only zero
    seats can share.
    """
    if seats < 0 or any(weight < 0 for weight in weights):
        raise ValueError(f"cannot share {seats} seats over weights {list(weights)}")
    total = sum(weights)
    if total == 0:
        if seats:
            raise ValueError(f"{seats} seats cannot be shared over weights that are all zero")
        return [0] * len(weights)
    shares = [Fraction(seats * weight, total) for weight in weights]
    given = [int(share) for share in shares]
    order = sorted(range(len(weights)), key=lambda i: (-(shares[i] - given[i]), -weights[i], i))
    for i in order[: seats - sum(given)]:
        given[i] += 1
    return given


def allocate_cell(quota: int, sizes: Sequence[int]) -> list[int]:
    """A cell's quota over its nonempty strata, ``sizes`` in the canonical order.

    One unit per stratum first, then the rest in proportion to what each stratum
    holds beyond that unit, by :func:`largest_remainder`, so no stratum is asked for
    more than it holds. A quota smaller than the number of strata gives one unit each
    to as many strata as it can, the largest first (a tie to the earlier stratum).
    """
    if any(size <= 0 for size in sizes):
        raise ValueError(f"a cell is allocated over its nonempty strata, not {list(sizes)}")
    if not 0 <= quota <= sum(sizes):
        raise ValueError(f"a quota of {quota} does not fit strata holding {sum(sizes)} units")
    if quota < len(sizes):
        largest_first = sorted(range(len(sizes)), key=lambda i: (-sizes[i], i))[:quota]
        return [int(i in largest_first) for i in range(len(sizes))]
    rest = largest_remainder(quota - len(sizes), [size - 1 for size in sizes])
    return [1 + extra for extra in rest]


@dataclass(frozen=True)
class StratumDraw:
    """One stratum of a cell: its units, its quota, how many were taken and how many count."""

    identity_band: str
    length_band: str
    population: int
    quota: int
    selected: int
    realised: int


@dataclass(frozen=True)
class CellDraw:
    """One of the nine cells: its units, its quota, the units taken and the units that count."""

    category: str
    aspect: str
    population: int
    quota: int
    selected: int
    realised: int
    strata: tuple[StratumDraw, ...]


@dataclass(frozen=True)
class WindowSampleDraw:
    """What a draw selected: the targets, and the allocation table that says how."""

    rule: str
    seed: str
    window_id: str
    units_per_cell: int
    targets: tuple[str, ...]
    cells: tuple[CellDraw, ...]

    def allocation(self) -> dict[str, Any]:
        """The allocation table, as the job result and the store record it."""
        return {
            "rule": self.rule,
            "seed": self.seed,
            "evaluation_set_id": self.window_id,
            "units_per_cell": self.units_per_cell,
            "targets": len(self.targets),
            "cells": [
                {
                    "category": cell.category,
                    "aspect": cell.aspect,
                    "population": cell.population,
                    "quota": cell.quota,
                    "selected": cell.selected,
                    "realised": cell.realised,
                    "strata": [
                        {
                            "identity_band": stratum.identity_band,
                            "length_band": stratum.length_band,
                            "population": stratum.population,
                            "quota": stratum.quota,
                            "selected": stratum.selected,
                            "realised": stratum.realised,
                        }
                        for stratum in cell.strata
                    ],
                }
                for cell in self.cells
            ],
        }


def _grouped(units: Iterable[Unit]) -> dict[tuple[str, str], dict[tuple[str, str], list[str]]]:
    """Cell to stratum to accessions, refusing a benchmark pair that appears twice."""
    seen: set[tuple[str, str]] = set()
    cells: dict[tuple[str, str], dict[tuple[str, str], list[str]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for unit in units:
        pair = (unit.accession, unit.aspect)
        if pair in seen:
            raise ValueError(f"target {unit.accession} appears twice in aspect {unit.aspect}")
        seen.add(pair)
        cells[unit.cell][unit.stratum].append(unit.accession)
    return cells


def _strata_in_order(strata: dict[tuple[str, str], list[str]]) -> list[tuple[str, str]]:
    return sorted(
        strata,
        key=lambda s: (IDENTITY_BAND_ORDER.index(s[0]), LENGTH_BAND_ORDER.index(s[1])),
    )


@dataclass(frozen=True)
class _CellTake:
    """One cell's strata in the canonical order, their quotas and the accessions taken."""

    strata: tuple[tuple[str, str], ...]
    populations: tuple[int, ...]
    quotas: tuple[int, ...]
    taken: tuple[tuple[str, ...], ...]


def _take(
    cell: dict[tuple[str, str], list[str]], units_per_cell: int, seed: str, window_id: str
) -> _CellTake:
    """What each stratum of a cell gives: its quota of accessions, in hash order."""
    order = tuple(_strata_in_order(cell))
    populations = tuple(len(cell[stratum]) for stratum in order)
    quotas = tuple(allocate_cell(min(units_per_cell, sum(populations)), populations))
    taken = tuple(
        tuple(sorted(cell[stratum], key=lambda a: (hash_order_key(seed, window_id, a), a))[:quota])
        for stratum, quota in zip(order, quotas, strict=True)
    )
    return _CellTake(order, populations, quotas, taken)


def _cell_draw(
    key: tuple[str, str],
    cell: dict[tuple[str, str], list[str]],
    take: _CellTake,
    targets: frozenset[str],
) -> CellDraw:
    """The allocation table's row of one cell, with the units that count once the union is known."""
    strata = tuple(
        StratumDraw(
            identity_band=stratum[0],
            length_band=stratum[1],
            population=population,
            quota=quota,
            selected=len(chosen),
            realised=sum(accession in targets for accession in cell[stratum]),
        )
        for stratum, population, quota, chosen in zip(
            take.strata, take.populations, take.quotas, take.taken, strict=True
        )
    )
    return CellDraw(
        category=key[0],
        aspect=key[1],
        population=sum(take.populations),
        quota=sum(take.quotas),
        selected=sum(stratum.selected for stratum in strata),
        realised=sum(stratum.realised for stratum in strata),
        strata=strata,
    )


def draw_window_sample(
    units: Iterable[Unit],
    *,
    units_per_cell: int,
    seed: str,
    window_id: str,
    rule: str = "equal_cells_proportional_strata",
) -> WindowSampleDraw:
    """The sample of a window's targets the rule draws from its benchmark pairs."""
    if rule not in WINDOW_SAMPLE_RULES:
        raise ValueError(f"unknown rule {rule!r}; expected one of {WINDOW_SAMPLE_RULES}")
    if units_per_cell < 1:
        raise ValueError(f"units_per_cell is at least 1, not {units_per_cell}")
    cells = _grouped(units)
    if not cells:
        raise ValueError(f"window {window_id} has no benchmark pair to sample")
    keys = [(category, aspect) for category in CATEGORY_ORDER for aspect in ASPECT_ORDER]
    takes = {key: _take(cells.get(key, {}), units_per_cell, seed, window_id) for key in keys}
    targets = frozenset(
        accession for take in takes.values() for chosen in take.taken for accession in chosen
    )
    return WindowSampleDraw(
        rule=rule,
        seed=seed,
        window_id=window_id,
        units_per_cell=units_per_cell,
        targets=tuple(sorted(targets)),
        cells=tuple(_cell_draw(key, cells.get(key, {}), takes[key], targets) for key in keys),
    )
