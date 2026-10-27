"""ONE-TIME FIX: rename the embedding registry to the declared scheme.

THIS SCRIPT IS A ONE-TIME FIX AND MUST NOT BE RE-RUN OR SCHEDULED IN NORMAL OPERATION.
It is safe to leave in the repository for provenance and historical reference only.

The registry named its levels by fewer fields than they varied in, which is the
defect this campaign keeps meeting. Three instances, all in one table:

``ankh_base`` sat at ``layer_indices [0]`` and ``ankh_base@L10`` at ``[10]``.
Since ``[0]`` means the LAST layer and ``[1]`` the penultimate — see
``compute_embeddings.py`` — the first name hid the layer axis entirely and the
second inverted it: ``@L10`` reads as the tenth layer and means the tenth from
the end, layer 38 of 48.

``family`` answered two questions, so grouping by it mixed model lineages with
architectures. The migration ``e1c7a94f2b30`` splits ``kind`` out; this script
carries the naming half.

The scheme, applied here:

    <kind>/<family>/<model>@d<depth>:<pooling>

``d`` is depth as a PERCENTAGE FROM THE INPUT, so ``@d100`` is the final layer
and ``@d0`` the embedding output. It is written relative because that is what
compares across nets of different size: ``@d67`` is ``[11]`` on a 33-layer
esm2_650m and ``[16]`` on a 48-layer ankh_base, and putting ``11`` and ``16`` on
the same bar compares two different things while ``@d67`` compares one.

Depth is resolved against each model's own layer count, read here from the
recorded ``layer_indices`` and the model's known depth rather than stored: a
second place holding the truth is a second place for it to be wrong.

Usage:
    PROTEA_ALLOW_BACKFILL=1 python scripts/rename_embedding_configs.py [--dry-run]
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet  # noqa: E402
from protea.infrastructure.orm.models.embedding.embedding_config import (  # noqa: E402
    EmbeddingConfig,
)
from protea.infrastructure.session import build_session_factory, session_scope  # noqa: E402
from protea.infrastructure.settings import load_settings  # noqa: E402

#: Encoder depth per model, in transformer layers. Only used to turn a stored
#: backwards index into a relative one; nothing downstream reads it, and
#: extraction resolves depth from the model itself.
_DEPTH = {
    "ElnaggarLab/ankh-base": 48,
    "ElnaggarLab/ankh-large": 48,
    "facebook/esm2_t36_3B_UR50D": 36,
    "facebook/esm2_t33_650M_UR50D": 33,
    "facebook/esm2_t6_8M_UR50D": 6,
    "Rostlab/ProstT5": 24,
    "Rostlab/prot_t5_xl_half_uniref50-enc": 24,
}

#: Short model tags. The HF id is kept in ``model_name``; the display name uses
#: the tag the campaign talks in.
_TAG = {
    "ElnaggarLab/ankh-base": "ankh_base",
    "ElnaggarLab/ankh-large": "ankh_large",
    "facebook/esm2_t36_3B_UR50D": "esm2_3b",
    "facebook/esm2_t33_650M_UR50D": "esm2_650m",
    "facebook/esm2_t6_8M_UR50D": "esm2_8m",
    "esmc_600m": "esmc_600m",
    "Rostlab/ProstT5": "prostt5",
    "Rostlab/prot_t5_xl_half_uniref50-enc": "prot_t5",
    "mila-intel/ProtST-esm1b": "protst",
}


def _depth_label(cfg: EmbeddingConfig) -> str:
    """``@dNN`` for a config, or ``@final`` when the depth is not resolvable.

    A config whose model depth is unknown is labelled by what is certain — that
    it sits at the output — instead of being given a percentage nobody measured.
    """
    idx = (cfg.layer_indices or [0])[0]
    if idx == 0:
        return "d100"
    total = _DEPTH.get(cfg.model_name)
    if total is None:
        return f"dminus{idx}"
    return f"d{round(100 * (total - idx) / total)}"


def _learned_name(
    cfg: EmbeddingConfig, parent: EmbeddingConfig | None, bank_version: str | None
) -> str:
    """Drop only the segments a column now holds, and only after checking.

    The old name carried five facts. Three have homes: the bank is
    ``trained_on_annotation_set_id``, the parent is
    ``derived_from_embedding_config_id``, and the parent's pooling is that
    parent's ``pooling``. Each is removed *because it matched* the column, not
    because a pattern looked like it — so a segment that fails to match is kept
    rather than guessed away. What is left over is what has nowhere else to
    live: the training objective, and the sparse encoder's hyperparameters.
    """
    drops = {
        str(parent.id)[:8] if parent else None,
        parent.pooling if parent else None,
    }
    kept = []
    for seg in (cfg.display_name or cfg.model_name).split(":"):
        if bank_version and seg.endswith(f"-{bank_version}"):
            seg = seg[: -len(bank_version) - 1]
        if seg and seg not in drops:
            kept.append(seg)
    return ":".join(kept)


def _new_name(
    cfg: EmbeddingConfig, parent: EmbeddingConfig | None, bank_version: str | None
) -> str:
    """The level's name: only what varies independently.

    ``kind`` and ``family`` are functions of the model, so repeating them here
    would be redundancy rather than information — they are columns, and the
    render groups by them. What varies independently is the model, the depth it
    was read at, and how residues were pooled.
    """
    if cfg.kind == "learned":
        return _learned_name(cfg, parent, bank_version)
    tag = _TAG.get(cfg.model_name, cfg.display_name or cfg.model_name)
    return f"{tag}@{_depth_label(cfg)}:{cfg.pooling}"


def main() -> None:
    if not os.getenv("PROTEA_ALLOW_BACKFILL"):
        print(
            "ERROR: one-time fix scripts are disabled by default.",
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
        rows = session.query(EmbeddingConfig).order_by(EmbeddingConfig.model_name).all()
        seen: dict[str, str] = {}
        for cfg in rows:
            parent = (
                session.get(EmbeddingConfig, cfg.derived_from_embedding_config_id)
                if cfg.derived_from_embedding_config_id
                else None
            )
            bank = (
                session.get(AnnotationSet, cfg.trained_on_annotation_set_id)
                if cfg.trained_on_annotation_set_id
                else None
            )
            new = _new_name(cfg, parent, bank.source_version if bank else None)
            if new in seen:
                raise SystemExit(
                    f"the scheme would give two configs the same name: {new!r} for "
                    f"{seen[new]} and {cfg.id}. That is the defect this rename removes, "
                    "so it refuses rather than producing it."
                )
            seen[new] = str(cfg.id)
            print(f"  {cfg.display_name or '(unnamed)':<46} -> {new}")
            cfg.display_name = new

        if a.dry_run:
            session.rollback()
            print(f"  [dry-run] {len(rows)} name(s) computed, nothing committed")
            return
        print(f"  {len(rows)} renamed")


if __name__ == "__main__":
    sys.exit(main())
