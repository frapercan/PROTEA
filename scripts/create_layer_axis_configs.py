"""ONE-TIME SETUP: turn the layer axis from an anecdote into a measured axis.

THIS SCRIPT IS A ONE-TIME SETUP AND MUST NOT BE RE-RUN OR SCHEDULED IN NORMAL OPERATION.
It is safe to leave in the repository for provenance and historical reference only.

The registry held the layer axis for exactly one model: a single pair on
ankh_base. One pair on one model is an observation, not an axis. This registers
the other twelve cells of a four-lineage by four-depth grid, so "where in the
network the signal lives" becomes a question with an answer that compares
across families.

DEPTH IS WRITTEN RELATIVE, AND THAT IS THE POINT. ``layer_indices`` counts
backwards -- ``[0]`` is the last layer, ``[1]`` the penultimate -- so the same
integer means a different place in nets of different size. ``@d67`` is ``[11]``
on a 33-layer esm2_650m and ``[16]`` on a 48-layer ankh_base; putting 11 and 16
on the same bar compares two different things, while ``@d67`` compares one.

Each new config copies every identity field from its lineage's existing
``@d100`` row and changes exactly one: the layer. Copying rather than
re-specifying is deliberate -- a hand-written recipe that drifts in pooling or
max_length would produce a level that differs in two fields while its name
claims one, which is the defect this campaign keeps meeting.

Ids are not assigned here. The ``before_insert`` listener derives them from the
recipe, so the compute node building the same grid lands on the same rows.

Usage:
    PROTEA_ALLOW_BACKFILL=1 python scripts/create_layer_axis_configs.py [--dry-run]
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from protea.core.embedding_identity import IDENTITY_FIELDS  # noqa: E402
from protea.infrastructure.orm.models.embedding.embedding_config import (  # noqa: E402
    EmbeddingConfig,
)
from protea.infrastructure.session import build_session_factory, session_scope  # noqa: E402
from protea.infrastructure.settings import load_settings  # noqa: E402

#: Encoder depth per lineage, in transformer layers. Used ONLY to turn a
#: relative depth into the backwards index the column stores. Nothing reads it
#: at extraction time: the backends resolve depth from the model itself, and
#: esmc_600m does not even declare it in config.json (36 counted from the
#: weights). A second place holding the truth is a second place for it to be
#: wrong, so this one exists for the length of this script and no longer.
_LINEAGES = {
    "esm2_650m@d100:mean": 33,
    "ankh_base@d100:mean": 48,
    "prot_t5@d100:mean": 24,
    "esmc_600m@d100:mean": 36,
}

#: Depth as a percentage FROM THE INPUT. 100 is the final layer and already
#: exists for all four; 0 is the embedding layer's own output.
_DEPTHS = (0, 33, 67)


def _layer_index(total: int, depth_pct: int) -> int:
    """The backwards index that lands ``depth_pct`` of the way in from the input."""
    return round(total * (100 - depth_pct) / 100)


def _new_name(reference: str, depth_pct: int) -> str:
    return reference.replace("@d100", f"@d{depth_pct}")


def main() -> None:
    if not os.getenv("PROTEA_ALLOW_BACKFILL"):
        print(
            "ERROR: one-time setup scripts are disabled by default.",
            "Set PROTEA_ALLOW_BACKFILL=1 to enable.",
            sep="\n",
            file=sys.stderr,
        )
        sys.exit(1)

    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--dry-run", action="store_true", help="Report, do not commit")
    a = ap.parse_args()

    settings = load_settings(PROJECT_ROOT)
    with session_scope(build_session_factory(settings.db_url)) as session:
        existing = {
            c.display_name: c for c in session.query(EmbeddingConfig).all() if c.display_name
        }
        missing = [n for n in _LINEAGES if n not in existing]
        if missing:
            raise SystemExit(
                f"refusing: no reference row for {', '.join(missing)}. Each new depth "
                "copies its lineage's @d100 recipe, so that row has to exist first."
            )

        created = 0
        for reference, total in _LINEAGES.items():
            base = existing[reference]
            recipe = {f: getattr(base, f) for f in IDENTITY_FIELDS}
            for depth in _DEPTHS:
                name = _new_name(reference, depth)
                index = _layer_index(total, depth)
                if name in existing:
                    print(f"  {name:<26} ya existe, se deja")
                    continue
                cfg = EmbeddingConfig(
                    **{**recipe, "layer_indices": [index]},
                    display_name=name,
                    family=base.family,
                    kind=base.kind,
                    param_count=base.param_count,
                    description=(
                        f"{base.model_name} read at {depth}% depth from the input "
                        f"(layer_indices [{index}] of {total}); the layer axis for "
                        "this lineage, comparable across families by relative depth"
                    ),
                )
                session.add(cfg)
                session.flush()
                print(
                    f"  {name:<26} capas={total:<3} layer_indices=[{index:<2}] "
                    f"-> {str(cfg.id)[:8]} (v{cfg.id.version})"
                )
                created += 1

        assigned = [
            c.display_name
            for c in session.new
            if isinstance(c, EmbeddingConfig) and c.id.version != 5
        ]
        if assigned:
            raise SystemExit(
                f"VERIFICATION FAILED, rolling back: {len(assigned)} config(s) took an "
                f"assigned id instead of a derived one: {', '.join(assigned)}"
            )

        if a.dry_run:
            session.rollback()
            print(f"  [dry-run] {created} configuracion(es) calculadas, nada confirmado")
            return
        print(f"  {created} creadas; la rejilla de capas queda en 4 linajes x 4 profundidades")


if __name__ == "__main__":
    sys.exit(main())
