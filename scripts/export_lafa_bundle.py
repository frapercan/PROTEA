"""Materialise a LAFA-compatible frozen-data bundle from PROTEA's database.

Reads from the live PROTEA database and writes the 7-artifact bundle that
``apps/lafa_container/protea_main.py`` expects (schema pinned in
``tests/test_lafa_container_loaders.py``):

::

    <output_dir>/
    ├── manifest.json
    ├── reference_embeddings.parquet
    ├── reference_annotations.parquet
    ├── go_term_metadata.parquet
    ├── pca_state.npz
    ├── anc2vec.npz                     (copy of artifacts/anc2vec/anc2vec_2020-10.npz)
    └── reranker.txt                    (booster bytes from RerankerModel)

Usage::

    poetry run python scripts/export_lafa_bundle.py \\
        --embedding_config_id <uuid> \\
        --annotation_set_id   <uuid> \\
        --reranker_model_name <name> \\
        --output_dir          /path/to/protea-frozen-v226-2025-05-03 \\
        --cutoff_version      v226 \\
        --cutoff_date         2025-05-03

The script reuses the live PROTEA artifact store to materialise the
reranker booster (``model_data`` inline, ``artifact_uri`` external,
or both). Anc2Vec is a static asset shared with every cutoff so the
exporter just copies the on-disk file.

Run F-LAFA.1 once per training cutoff. Per-cutoff bundles live in
their own directories / repositories named
``protea-frozen-v<N>-<YYYY-MM-DD>`` per the LAFA contract.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import uuid
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from protea_method.pca_cache import load_or_fit_pca_state
from sqlalchemy import select
from sqlalchemy.orm import Session

from protea.infrastructure.orm.models.annotation.go_term import GOTerm
from protea.infrastructure.orm.models.annotation.protein_go_annotation import (
    ProteinGOAnnotation,
)
from protea.infrastructure.orm.models.embedding.reranker_model import RerankerModel
from protea.infrastructure.orm.models.embedding.sequence_embedding import (
    SequenceEmbedding,
)
from protea.infrastructure.orm.models.protein.protein import Protein
from protea.infrastructure.orm.models.sequence.sequence import Sequence
from protea.infrastructure.session import build_session_factory, session_scope
from protea.infrastructure.settings import load_settings
from protea.infrastructure.storage import get_artifact_store

_ANC2VEC_SOURCE = (
    Path(__file__).resolve().parents[1]
    / "protea"
    / "artifacts"
    / "anc2vec"
    / "anc2vec_2020-10.npz"
)


def _export_reference_embeddings(
    session: Session,
    embedding_config_id: uuid.UUID,
    output_path: Path,
    *,
    max_refs: int | None = None,
) -> tuple[list[str], np.ndarray]:
    """Write ``reference_embeddings.parquet`` and return the in-memory pool.

    One row per ``(canonical_accession, embedding)`` pair using the
    ``chunk_index_s == 0`` slice (full-sequence embedding). Proteins
    sharing a sequence inherit the same embedding via the Protein →
    Sequence → SequenceEmbedding join; we deduplicate by canonical
    accession at write time.
    """
    stmt = (
        select(
            Protein.canonical_accession,
            SequenceEmbedding.embedding,
        )
        .join(Sequence, Sequence.id == Protein.sequence_id)
        .join(SequenceEmbedding, SequenceEmbedding.sequence_id == Sequence.id)
        .where(SequenceEmbedding.embedding_config_id == embedding_config_id)
        .where(SequenceEmbedding.chunk_index_s == 0)
    )
    if max_refs is not None:
        stmt = stmt.limit(max_refs)

    seen: set[str] = set()
    accessions: list[str] = []
    rows: list[np.ndarray] = []
    for canonical, embedding in session.execute(stmt):
        if canonical in seen:
            continue
        seen.add(canonical)
        accessions.append(canonical)
        rows.append(np.asarray(embedding, dtype=np.float32))

    if not accessions:
        raise ValueError(
            f"No SequenceEmbeddings found for embedding_config_id={embedding_config_id}",
        )

    embeddings = np.stack(rows).astype(np.float32, copy=False)
    print(f"[export_lafa_bundle] reference_embeddings: {embeddings.shape}")

    table = pa.table({"accession": accessions, "embedding": list(rows)})
    pq.write_table(table, output_path, compression="snappy")
    return accessions, embeddings


def _export_reference_annotations(
    session: Session,
    annotation_set_id: uuid.UUID,
    accessions: list[str],
    output_path: Path,
) -> int:
    """Write ``reference_annotations.parquet`` filtered to the export refs."""
    accession_set = set(accessions)
    # ProteinGOAnnotation.protein_accession is at isoform granularity
    # (FK to Protein.accession); we join Protein to roll the rows up
    # to the canonical accession that matches the reference embeddings.
    stmt = (
        select(
            Protein.canonical_accession,
            ProteinGOAnnotation.go_term_id,
            ProteinGOAnnotation.qualifier,
            ProteinGOAnnotation.evidence_code,
        )
        .join(Protein, Protein.accession == ProteinGOAnnotation.protein_accession)
        .where(ProteinGOAnnotation.annotation_set_id == annotation_set_id)
    )
    accs: list[str] = []
    go_ids: list[int] = []
    qualifiers: list[str | None] = []
    evidence: list[str | None] = []
    for canonical, gt_id, qualifier, ev in session.execute(stmt):
        if canonical not in accession_set:
            continue
        accs.append(canonical)
        go_ids.append(int(gt_id))
        qualifiers.append(qualifier)
        evidence.append(ev)

    table = pa.table(
        {
            "accession": accs,
            "go_term_id": go_ids,
            "qualifier": qualifiers,
            "evidence_code": evidence,
        }
    )
    pq.write_table(table, output_path, compression="snappy")
    print(f"[export_lafa_bundle] reference_annotations: {len(accs)} rows")
    return len(accs)


def _export_go_metadata(
    session: Session,
    ontology_snapshot_id: uuid.UUID,
    output_path: Path,
) -> int:
    stmt = select(
        GOTerm.id,
        GOTerm.go_id,
        GOTerm.aspect,
        GOTerm.name,
    ).where(GOTerm.ontology_snapshot_id == ontology_snapshot_id)
    rows = list(session.execute(stmt))
    if not rows:
        raise ValueError(
            f"No GOTerm rows found for ontology_snapshot_id={ontology_snapshot_id}",
        )
    table = pa.table(
        {
            "go_term_id": [int(r[0]) for r in rows],
            "go_id": [str(r[1]) for r in rows],
            "aspect": [str(r[2] or "") for r in rows],
            "name": [str(r[3] or "") for r in rows],
        }
    )
    pq.write_table(table, output_path, compression="snappy")
    print(f"[export_lafa_bundle] go_term_metadata: {len(rows)} rows")
    return len(rows)


def _export_pca_state(
    embedding_config_id: uuid.UUID,
    embeddings: np.ndarray,
    output_path: Path,
) -> tuple[np.ndarray, np.ndarray]:
    """Fit PCA via the same helper used by predict_go_terms_batch.

    Reuses the on-disk cache under ``protea/artifacts/pca/<config>.npz``;
    if the cache file already exists for this ``embedding_config_id``,
    fitting is skipped.
    """
    state = load_or_fit_pca_state(embedding_config_id, embeddings)
    if state is None:
        raise ValueError("PCA state could not be computed (empty pool).")
    mean, components = state
    np.savez(output_path, mean=mean, components=components)
    print(f"[export_lafa_bundle] pca_state: mean={mean.shape} components={components.shape}")
    return mean, components


def _copy_anc2vec(output_path: Path) -> None:
    if not _ANC2VEC_SOURCE.exists():
        raise FileNotFoundError(
            f"Anc2Vec artifact missing at {_ANC2VEC_SOURCE}; run the embedding bootstrap first.",
        )
    shutil.copyfile(_ANC2VEC_SOURCE, output_path)
    print(f"[export_lafa_bundle] anc2vec: copied from {_ANC2VEC_SOURCE.name}")


def _fetch_reranker_text(
    session: Session,
    name: str | None,
    model_id: uuid.UUID | None,
) -> tuple[RerankerModel, str]:
    """Resolve a RerankerModel row + load its booster text.

    Source priority: ``model_id`` > ``name``. Booster text comes from
    the inline ``model_data`` column if present, else from the
    ``artifact_uri`` via the live PROTEA artifact store.
    """
    if model_id is None and name is None:
        raise ValueError("Provide either model_id or name.")

    stmt = select(RerankerModel)
    if model_id is not None:
        stmt = stmt.where(RerankerModel.id == model_id)
    else:
        stmt = stmt.where(RerankerModel.name == name)
    row = session.execute(stmt).scalar_one_or_none()
    if row is None:
        raise ValueError(f"RerankerModel not found (name={name!r} id={model_id})")

    if row.model_data:
        return row, row.model_data
    if row.artifact_uri:
        store = get_artifact_store(load_settings(Path.cwd()))
        from protea.core.reranker import _uri_to_key

        key = _uri_to_key(row.artifact_uri, store)
        return row, store.get(key).decode("utf-8")
    raise ValueError(f"RerankerModel {row.id} has neither model_data nor artifact_uri.")


def _export_reranker(
    session: Session,
    name: str | None,
    model_id: uuid.UUID | None,
    output_path: Path,
) -> dict[str, Any]:
    """Legacy single-booster export. Writes ``output_path`` (typically
    ``<bundle>/reranker.txt``) and returns provenance metadata for the
    manifest.
    """
    row, text = _fetch_reranker_text(session, name, model_id)
    output_path.write_bytes(text.encode("utf-8"))
    print(
        f"[export_lafa_bundle] reranker: {row.name} "
        f"(schema_sha={row.feature_schema_sha[:12] if row.feature_schema_sha else '?'}, "
        f"{len(text)} chars)",
    )
    return {
        "reranker_model_id": str(row.id),
        "reranker_name": row.name,
        "feature_schema_sha": row.feature_schema_sha or "",
        "external_source": row.external_source or "",
    }


def _export_rerankers_by_aspect(
    session: Session,
    by_aspect: dict[str, str],
    output_dir: Path,
) -> dict[str, Any]:
    """Multi-booster export. Materialises ``<output_dir>/reranker/<aspect>.txt``
    for each provided aspect and writes ``routing.json`` with per-aspect
    provenance. Returns aggregated metadata for the manifest.
    """
    reranker_dir = output_dir / "reranker"
    reranker_dir.mkdir(parents=True, exist_ok=True)

    routing: dict[str, dict[str, str]] = {}
    schemas: set[str] = set()
    for aspect, name in by_aspect.items():
        if aspect not in {"F", "P", "C"}:
            raise ValueError(f"Unknown GO aspect for reranker routing: {aspect!r}")
        row, text = _fetch_reranker_text(session, name, None)
        path = reranker_dir / f"{aspect}.txt"
        path.write_bytes(text.encode("utf-8"))
        routing[aspect] = {
            "filename": path.name,
            "reranker_model_id": str(row.id),
            "reranker_name": row.name,
            "feature_schema_sha": row.feature_schema_sha or "",
            "external_source": row.external_source or "",
        }
        if row.feature_schema_sha:
            schemas.add(row.feature_schema_sha)
        print(
            f"[export_lafa_bundle] reranker[{aspect}]: {row.name} "
            f"(schema_sha={row.feature_schema_sha[:12] if row.feature_schema_sha else '?'}, "
            f"{len(text)} chars)",
        )

    (reranker_dir / "routing.json").write_text(
        json.dumps({"version": "1", "boosters": routing}, indent=2, sort_keys=True),
    )

    if len(schemas) > 1:
        print(
            f"[export_lafa_bundle] WARNING: per-aspect boosters have "
            f"divergent feature_schema_sha: {sorted(schemas)}",
        )

    # Pick one canonical schema_sha for the manifest. When all boosters
    # agree it is the shared sha; otherwise the manifest records the
    # alphabetically-first one and the warning above flags the drift.
    canonical_sha = sorted(schemas)[0] if schemas else ""
    return {
        "reranker_routing": "per_aspect",
        "reranker_aspects": sorted(by_aspect.keys()),
        "feature_schema_sha": canonical_sha,
    }


def _write_manifest(
    output_dir: Path,
    *,
    embedding_config_id: uuid.UUID,
    annotation_set_id: uuid.UUID,
    ontology_snapshot_id: uuid.UUID,
    cutoff_version: str,
    cutoff_date: str,
    n_refs: int,
    n_annotations: int,
    n_go_terms: int,
    embedding_dim: int,
    reranker_meta: dict[str, Any],
) -> None:
    payload = {
        "schema_version": "1",
        "cutoff_version": cutoff_version,
        "cutoff_date": cutoff_date,
        "embedding_config_id": str(embedding_config_id),
        "annotation_set_id": str(annotation_set_id),
        "ontology_snapshot_id": str(ontology_snapshot_id),
        "n_refs": int(n_refs),
        "n_annotations": int(n_annotations),
        "n_go_terms": int(n_go_terms),
        "embedding_dim": int(embedding_dim),
        **reranker_meta,
    }
    (output_dir / "manifest.json").write_text(json.dumps(payload, indent=2, sort_keys=True))
    print(
        f"[export_lafa_bundle] manifest: cutoff={cutoff_version} ({cutoff_date}), "
        f"refs={n_refs}, ann={n_annotations}, go_terms={n_go_terms}",
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--embedding_config_id", required=True, type=uuid.UUID)
    parser.add_argument("--annotation_set_id", required=True, type=uuid.UUID)
    parser.add_argument(
        "--ontology_snapshot_id",
        type=uuid.UUID,
        default=None,
        help="Default: derived from the AnnotationSet row.",
    )
    parser.add_argument(
        "--reranker_model_id",
        type=uuid.UUID,
        help="Legacy single-booster path; writes reranker.txt at the bundle root.",
    )
    parser.add_argument(
        "--reranker_model_name",
        type=str,
        help="Legacy single-booster path; writes reranker.txt at the bundle root.",
    )
    parser.add_argument(
        "--reranker_for_F",
        type=str,
        help="Per-aspect MFO booster name (writes reranker/F.txt).",
    )
    parser.add_argument(
        "--reranker_for_P",
        type=str,
        help="Per-aspect BPO booster name (writes reranker/P.txt).",
    )
    parser.add_argument(
        "--reranker_for_C",
        type=str,
        help="Per-aspect CCO booster name (writes reranker/C.txt).",
    )
    parser.add_argument("--output_dir", required=True, type=Path)
    parser.add_argument("--cutoff_version", required=True, type=str)
    parser.add_argument(
        "--cutoff_date",
        type=str,
        default=str(date.today()),
        help="Default: today's date (YYYY-MM-DD).",
    )
    parser.add_argument("--max_refs", type=int, default=None)
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)

    settings = load_settings(Path.cwd())
    factory = build_session_factory(settings.db_url)

    with session_scope(factory) as session:
        ontology_snapshot_id = args.ontology_snapshot_id
        if ontology_snapshot_id is None:
            from protea.infrastructure.orm.models.annotation.annotation_set import (
                AnnotationSet,
            )

            anno_set = session.execute(
                select(AnnotationSet).where(AnnotationSet.id == args.annotation_set_id),
            ).scalar_one_or_none()
            if anno_set is None:
                print(
                    f"[export_lafa_bundle] AnnotationSet {args.annotation_set_id} not found",
                    file=sys.stderr,
                )
                sys.exit(2)
            ontology_snapshot_id = anno_set.ontology_snapshot_id

        accessions, embeddings = _export_reference_embeddings(
            session,
            args.embedding_config_id,
            args.output_dir / "reference_embeddings.parquet",
            max_refs=args.max_refs,
        )
        n_annotations = _export_reference_annotations(
            session,
            args.annotation_set_id,
            accessions,
            args.output_dir / "reference_annotations.parquet",
        )
        n_go_terms = _export_go_metadata(
            session,
            ontology_snapshot_id,
            args.output_dir / "go_term_metadata.parquet",
        )
        _export_pca_state(
            args.embedding_config_id,
            embeddings,
            args.output_dir / "pca_state.npz",
        )
        _copy_anc2vec(args.output_dir / "anc2vec.npz")

        per_aspect = {
            aspect: getattr(args, f"reranker_for_{aspect}")
            for aspect in ("F", "P", "C")
            if getattr(args, f"reranker_for_{aspect}")
        }
        if per_aspect:
            reranker_meta = _export_rerankers_by_aspect(
                session, per_aspect, args.output_dir,
            )
        elif args.reranker_model_name or args.reranker_model_id:
            reranker_meta = _export_reranker(
                session,
                args.reranker_model_name,
                args.reranker_model_id,
                args.output_dir / "reranker.txt",
            )
        else:
            print(
                "[export_lafa_bundle] error: provide either --reranker_model_id /"
                " --reranker_model_name (legacy) or --reranker_for_{F,P,C} "
                "(per-aspect).",
                file=sys.stderr,
            )
            sys.exit(2)
        _write_manifest(
            args.output_dir,
            embedding_config_id=args.embedding_config_id,
            annotation_set_id=args.annotation_set_id,
            ontology_snapshot_id=ontology_snapshot_id,
            cutoff_version=args.cutoff_version,
            cutoff_date=args.cutoff_date,
            n_refs=len(accessions),
            n_annotations=n_annotations,
            n_go_terms=n_go_terms,
            embedding_dim=int(embeddings.shape[1]),
            reranker_meta=reranker_meta,
        )

    print(f"[export_lafa_bundle] bundle ready at {args.output_dir}")


if __name__ == "__main__":
    main()
