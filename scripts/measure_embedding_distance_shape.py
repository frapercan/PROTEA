"""The SHAPE of an embedding's distance distribution, by length band.

Read-only. Both machines run this same file, which is the point: a measurement
that exists only as a script on one host is a capability that dies with that
host, and one wrapped as a registered operation pays for a payload class, a
catalog entry, a queue and a slot in every worker's memory without collecting
retry, progress or distribution in return.

WHY SHAPE AND NOT THE MEAN. For unit vectors the mean cosine distance is not
independent of the centroid's norm: ``|c|^2 = 1/n + (n-1)/n * cos_mean``, so
mean distance and anisotropy are one quantity in two dresses (verified across
five configs, maximum discrepancy 4e-5). Reporting both as corroborating
evidence counts one fact twice. What the mean does NOT determine is the
dispersion -- ``p99/p50`` -- and that is the column that says whether a ranking
survives a collapsed representation. Measured on ankh_base: ``@d79`` sits at
mean distance 0.0004 with a near-total directional collapse, yet its p99/p50 is
2.6x against 1.7x at ``@d100``. Cosine ranking is scale-free, so a cone a
thousand times narrower still orders its members.

WHY BY BAND, AND WHY RATIOS. esm2 declares 1026 positions and was trained at
1024; ankh and prot_t5 use relative position bias and saturate instead. Nothing
errors past that, and ``residues_processed`` is identical across lineages
because tokenisation cuts at the same place -- so no residue counter can see
whether what a model reads beyond its window means anything.

Dispersion can, but not in absolute terms: the lineages sit between 1.5x and
5.4x at their own baseline, so a fallen esm2 can still read higher than a
healthy prot_t5. Each lineage is therefore measured AGAINST ITSELF -- the ratio
of its dispersion in a band to its dispersion in the shortest band -- and only
those ratios are compared. Difference in differences; the baseline cancels.

THE CONTROL. The three esm2 sizes share one 1024 window and differ 375-fold in
capacity, which separates three hypotheses that are otherwise stuck together:

    extrapolation  all THREE esm2 degrade alike regardless of size, and the
                   four non-esm2 do not
    the band       all seven degrade together, so it is the long protein and
                   not the model
    capacity       degradation scales with size, and 3B degrades LESS than 8M

8M and 3B cannot coincide by capacity, so a joint fall of the three against a
flat four rules capacity out by construction rather than by argument.

THE PREDICTION, registered before running. Under extrapolation the fall must
appear between band 2 and band 3, because esm2's trained window (1024) is
almost exactly the band edge. A fall inside band 2, or a gradual slide from
band 1, is not the window.

Design by the compute node.

Usage:
    PGURL=... python scripts/measure_embedding_distance_shape.py [--sample 400]
"""

from __future__ import annotations

import argparse
import os
import sys

import numpy as np
import psycopg

#: The campaign's own length bands (``protea.core.strata``), so a number here
#: and a number in a stratum table mean the same population.
BANDS: tuple[tuple[str, int, int], ...] = (
    ("<=512", 0, 512),
    ("512-1024", 512, 1024),
    ("1024-2048", 1024, 2048),
    (">2048", 2048, 10**9),
)


def _fetch(cur, config_id: str, lo: int, hi: int, limit: int) -> np.ndarray | None:
    """A deterministic slice of one band's vectors: ordered, never random.

    Two machines reporting different numbers then means the vectors differ, not
    the draw.
    """
    cur.execute(
        "SELECT e.embedding::text FROM sequence_embedding e "
        "JOIN sequence s ON s.id = e.sequence_id "
        "WHERE e.embedding_config_id = %s AND length(s.sequence) > %s "
        "AND length(s.sequence) <= %s ORDER BY e.sequence_id LIMIT %s",
        (config_id, lo, hi, limit),
    )
    rows = cur.fetchall()
    if len(rows) < 32:
        return None
    return np.array([np.fromstring(r[0].strip("[]"), sep=",") for r in rows], dtype=np.float64)


def _dispersion(v: np.ndarray) -> dict[str, float]:
    """p99/p50 of the pairwise cosine distances, plus the mean for context."""
    unit = v / np.linalg.norm(v, axis=1)[:, None]
    sims = unit @ unit.T
    d = 1.0 - sims[np.triu_indices(len(v), k=1)]
    p50, p99 = float(np.percentile(d, 50)), float(np.percentile(d, 99))
    p1 = float(np.percentile(d, 1))
    return {
        "n": len(v),
        "mean": float(d.mean()),
        "p99_p50": p99 / p50 if p50 > 0 else float("nan"),
        "p99_p1": p99 / p1 if p1 > 0 else float("nan"),
    }


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--sample", type=int, default=400, help="vectors per band")
    a = ap.parse_args()

    url = os.environ.get("PGURL")
    if not url:
        sys.exit("PGURL no definido")
    url = url.replace("postgresql+psycopg://", "postgresql://")

    with psycopg.connect(url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT c.display_name, c.id, c.family FROM embedding_config c "
            "WHERE EXISTS (SELECT 1 FROM sequence_embedding e "
            "WHERE e.embedding_config_id = c.id) ORDER BY c.family, c.display_name"
        )
        configs = cur.fetchall()
        if not configs:
            sys.exit("ninguna configuracion tiene vectores almacenados")

        print(f"{'configuracion':<30}{'banda':>12}{'n':>6}{'media':>10}{'p99/p50':>9}{'ratio':>8}")
        print("-" * 75)
        for name, cid, _family in configs:
            base: float | None = None
            for label, lo, hi in BANDS:
                v = _fetch(cur, str(cid), lo, hi, a.sample)
                if v is None:
                    print(f"{name:<30}{label:>12}{'-':>6}{'sin vectores suficientes':>28}")
                    continue
                s = _dispersion(v)
                if base is None:
                    base = s["p99_p50"]
                # Each lineage against ITSELF. Comparing absolute dispersion
                # across lineages would compare their baselines, which differ
                # by more than the effect being looked for.
                ratio = s["p99_p50"] / base if base else float("nan")
                print(
                    f"{name:<30}{label:>12}{s['n']:>6}{s['mean']:>10.4f}"
                    f"{s['p99_p50']:>9.2f}{ratio:>8.2f}"
                )
            print()


if __name__ == "__main__":
    sys.exit(main())
