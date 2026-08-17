"""Compute an Information Accretion table and publish it as a tracked artifact.

IA(v) = -log2( P(v | parents(v)) ), the term weight cafaeval and LAFA use for
``f_micro_w``. The arithmetic lives in :mod:`protea.core.ia`, which is validated
against an independent reimplementation of LAFA's ``calc_ia``; this operation
owns the corpus, the gates and the artifact.

Why this is an operation and not a script. IA is identified by three axes
(ontology snapshot, annotation corpus, evidence regime). Its predecessor,
``scripts/compute_ia_for_snapshot.py``, pinned one of them in the output
filename, offered no evidence predicate at all, and wrote to local disk with a
``file://`` URL that only the producing machine could resolve. A table computed
that way over the full GOA corpus (89.8 percent IEA on v226) sits 8.8x further
from the reference than one computed over the scored evidence regime, and
nothing recorded which had happened.

Gates. Every invariant below was measured clean on the v226 pivot before being
made load-bearing (40214 terms, 69188 True Path Rule edges, 5907336 annotations,
25644893 propagated pairs). They are enforced rather than logged because each
one has a silent failure mode: ``protea.core.ia.term_ia`` clamps ``num > denom``
to 0.0 rather than raising, so a broken propagation would otherwise surface as a
plausible table of mostly-zero weights.
"""

from __future__ import annotations

import dataclasses
import hashlib
import sys
import tempfile
import time
import uuid
from collections import defaultdict
from pathlib import Path
from typing import Any, NamedTuple

from pydantic import field_validator
from sqlalchemy import bindparam, text
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, OperationResult, ProteaPayload
from protea.core.ia import PROPAGATE_RELATIONS, build_ancestors, term_ia
from protea.core.ia_regimes import DEFAULT_REGIME, EVIDENCE_REGIMES, resolve_regime
from protea.core.utils import job_id_from_payload
from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
from protea.infrastructure.orm.models.annotation.information_accretion_set import (
    InformationAccretionSet,
)
from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot
from protea.infrastructure.settings import load_settings
from protea.infrastructure.storage import get_artifact_store


def ia_key_for(ia_set_id: Any) -> str:
    """Storage key under which an InformationAccretionSet's TSV lives."""
    return f"information_accretion/{ia_set_id}/IA.tsv"


class ComputeInformationAccretionPayload(ProteaPayload, frozen=True):
    ontology_snapshot_id: str
    annotation_set_id: str
    #: Regime name from ``protea.core.ia_regimes``. Defaults to the
    #: board-comparable one; ``"all"`` has to be asked for by name.
    evidence_regime: str = DEFAULT_REGIME
    #: Recompute and replace an existing table for the same three axes.
    force: bool = False
    #: Maximum share of corpus annotations whose GO id is absent from the target
    #: snapshot. Measured 0.000 percent on v226; a non-trivial drop means the
    #: corpus and the snapshot do not belong together.
    max_drop_rate_pct: float = 1.0

    @field_validator("ontology_snapshot_id", "annotation_set_id", mode="before")
    @classmethod
    def must_be_non_empty(cls, v: str) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ValueError("must be a non-empty string")
        return v.strip()

    @field_validator("evidence_regime", mode="before")
    @classmethod
    def regime_in_vocab(cls, v):
        if not isinstance(v, str) or v not in EVIDENCE_REGIMES:
            raise ValueError(f"must be one of {sorted(EVIDENCE_REGIMES)}")
        return v


class InformationAccretionGateError(RuntimeError):
    """A structural invariant of the computed table did not hold."""


class _Inputs(NamedTuple):
    """What this run was asked to do, once resolved against the database."""

    p: "ComputeInformationAccretionPayload"
    payload: dict[str, Any]
    snapshot: Any
    corpus: Any
    evidence_codes: tuple[str, ...] | None
    existing: Any


class _Measured(NamedTuple):
    """What the run found while building the table.

    Separate from :class:`_Dag` because these are counts about the work rather
    than the ontology itself, and they are what the persisted row reports.
    """

    gate_stats: dict[str, Any]
    raw: int
    proteins: set[str]
    propagated_pairs: int
    aspect_of: dict[str, str | None]
    started: float


@dataclasses.dataclass(frozen=True)
class _Dag:
    """The propagated ontology the gates inspect.

    Five structures that are always computed together and always passed
    together. Bundling them is not to shorten a signature: it is that none of
    the gates is meaningful against a subset of them, so a caller holding four
    of the five has nothing it can legally check.
    """

    terms: list[str]
    parents: dict[str, set[str]]
    ancestors: dict[str, frozenset[str]]
    proteins_per_term: dict[str, set[str]]
    ia: dict[str, float]


class ComputeInformationAccretionOperation:
    name = "compute_information_accretion"
    description = (
        "Compute Information Accretion over an ontology snapshot and an "
        "annotation corpus restricted to an evidence regime, gate its "
        "structural invariants, and publish the TSV to the artifact store."
    )

    # ------------------------------------------------------------------ DAG
    def _load_dag(
        self, session: Session, snapshot_id: str
    ) -> tuple[list[str], dict[str, str | None], dict[str, set[str]]]:
        rows = session.execute(
            text(
                "select go_id, aspect from go_term "
                "where ontology_snapshot_id = :s and is_obsolete = false"
            ),
            {"s": snapshot_id},
        ).all()
        terms = [r[0] for r in rows]
        aspect_of = {r[0]: r[1] for r in rows}

        rels_stmt = text(
            "select c.go_id, p.go_id "
            "from go_term_relationship r "
            "join go_term c on c.id = r.child_go_term_id "
            "join go_term p on p.id = r.parent_go_term_id "
            "where r.ontology_snapshot_id = :s and r.relation_type in :rels"
        ).bindparams(bindparam("rels", expanding=True))
        parents: dict[str, set[str]] = defaultdict(set)
        for child, parent in session.execute(
            rels_stmt, {"s": snapshot_id, "rels": list(PROPAGATE_RELATIONS)}
        ):
            parents[child].add(parent)
        return terms, aspect_of, dict(parents)

    # -------------------------------------------------------------- corpus
    def _propagate(
        self,
        session: Session,
        annotation_set_id: str,
        evidence_codes: tuple[str, ...] | None,
        ancestors: dict[str, frozenset[str]],
        emit: EmitFn,
    ) -> tuple[dict[str, set[str]], int, int, set[str]]:
        sql = (
            "select pga.protein_accession, gt.go_id "
            "from protein_go_annotation pga "
            "join go_term gt on gt.id = pga.go_term_id "
            "where pga.annotation_set_id = :a"
        )
        params: dict[str, Any] = {"a": annotation_set_id}
        if evidence_codes is not None:
            sql += " and pga.evidence_code in :ev"
            params["ev"] = list(evidence_codes)
            stmt = text(sql).bindparams(bindparam("ev", expanding=True))
        else:
            stmt = text(sql)

        proteins_per_term: dict[str, set[str]] = defaultdict(set)
        proteins: set[str] = set()
        raw = dropped = 0
        for accession, go_id in session.execute(stmt, params).yield_per(100_000):
            raw += 1
            closure = ancestors.get(go_id)
            if closure is None:
                dropped += 1
                continue
            accession = sys.intern(accession)
            proteins.add(accession)
            for ancestor in closure:
                proteins_per_term[ancestor].add(accession)
            if raw % 1_000_000 == 0:
                emit(
                    "compute_information_accretion.propagating",
                    f"{raw:,} annotations",
                    {"raw": raw, "dropped": dropped},
                    "info",
                )
        return dict(proteins_per_term), raw, dropped, proteins

    # --------------------------------------------------------------- gates
    def _gate(
        self,
        dag: "_Dag",
        raw: int,
        dropped: int,
        max_drop_rate_pct: float,
    ) -> dict[str, Any]:
        """Structural invariants. Each raises rather than warns.

        Split by what each check inspects rather than by length. The corpus
        checks read only the annotation counts, the shape check reads only the
        parent and ancestor maps, propagation reads the protein sets, and the
        value checks read the computed IA. Nothing here shares state with the
        next thing, which is why it was six checks in one method rather than
        one check that needed ninety-seven lines.
        """
        drop_rate = self._gate_corpus(raw, dropped, max_drop_rate_pct)
        self._gate_acyclic(dag)
        self._gate_propagation(dag)
        roots, nonzero = self._gate_values(dag)
        return {
            "drop_rate_pct": drop_rate,
            "cycles": 0,
            "tpr_violations": 0,
            "roots": roots,
            "nonzero": nonzero,
        }

    @staticmethod
    def _gate_corpus(raw: int, dropped: int, max_drop_rate_pct: float) -> float:
        """The corpus exists and matches the snapshot. Returns the drop rate."""
        if raw == 0:
            raise InformationAccretionGateError(
                "corpus is empty for this annotation set and evidence regime; "
                "an IA table over no annotations would be all zeros and would "
                "score as uniform IC=1 without ever saying so"
            )
        drop_rate = 100.0 * dropped / raw
        if drop_rate > max_drop_rate_pct:
            raise InformationAccretionGateError(
                f"{dropped:,} of {raw:,} annotations ({drop_rate:.3f} percent) "
                f"reference a GO id absent from the target snapshot, above the "
                f"{max_drop_rate_pct} percent limit; the corpus and the "
                f"snapshot are probably not congruent"
            )
        return drop_rate

    @staticmethod
    def _gate_acyclic(dag: "_Dag") -> None:
        """A term must not be its own strict ancestor."""
        cycles = [
            t for t in dag.terms
            if t in dag.parents
            and t in {a for p in dag.parents[t] for a in dag.ancestors.get(p, frozenset())}
        ]
        if cycles:
            raise InformationAccretionGateError(
                f"{len(cycles)} terms participate in a True Path Rule cycle, "
                f"e.g. {cycles[:5]}; the DAG is not acyclic"
            )

    @staticmethod
    def _gate_propagation(dag: "_Dag") -> None:
        """No term holds more proteins than its parents' intersection.

        After propagation a child's protein set is a subset of every parent's,
        so num <= denom always holds. ``term_ia`` clamps a violation to 0.0
        silently, which is why this is checked here instead.
        """
        violations: list[tuple[str, int, int]] = []
        for t in dag.terms:
            ps = dag.parents.get(t)
            if not ps:
                continue
            parent_sets = [dag.proteins_per_term.get(p) for p in ps]
            if any(not s for s in parent_sets):
                denom = 1
            else:
                denom = len(set.intersection(*parent_sets)) + 1  # type: ignore[arg-type]
            num = len(dag.proteins_per_term.get(t, ())) + 1
            if num > denom:
                violations.append((t, num, denom))
                if len(violations) >= 20:
                    break
        if violations:
            raise InformationAccretionGateError(
                f"{len(violations)}+ terms hold more proteins than the "
                f"intersection of their parents, e.g. {violations[:5]}; "
                f"propagation is broken and term_ia would clamp these to 0.0"
            )

    @staticmethod
    def _gate_values(dag: "_Dag") -> tuple[int, int]:
        """The computed values are possible. Returns (roots, nonzero)."""
        negative = [t for t in dag.terms if dag.ia[t] < 0.0]
        if negative:
            raise InformationAccretionGateError(
                f"{len(negative)} terms have negative IA, e.g. {negative[:5]}"
            )
        roots = [t for t in dag.terms if not dag.parents.get(t)]
        if not roots:
            raise InformationAccretionGateError(
                "no root term (a term with no True Path Rule parent) exists; "
                "the relationship table is probably empty for this snapshot"
            )
        bad_roots = [t for t in roots if dag.ia[t] != 0.0]
        if bad_roots:
            raise InformationAccretionGateError(
                f"{len(bad_roots)} roots have non-zero IA, e.g. {bad_roots[:5]}"
            )
        nonzero = sum(1 for v in dag.ia.values() if v > 0.0)
        if nonzero == 0:
            raise InformationAccretionGateError(
                "every term has IA 0.0; the table would weight identically to "
                "the uniform IC=1 fallback it is meant to replace"
            )
        return len(roots), nonzero

    # ------------------------------------------------------------- execute
    @staticmethod
    def _resolve(
        session: Session, p: ComputeInformationAccretionPayload, payload: dict[str, Any]
    ) -> "_Inputs":
        """Resolve the three axes IA is identified by, plus any existing table.

        Missing snapshot or corpus raises here rather than later: a table
        computed against a half-resolved identity would be indistinguishable
        from a correct one once written.
        """
        snapshot = session.get(OntologySnapshot, uuid.UUID(p.ontology_snapshot_id))
        if snapshot is None:
            raise ValueError(f"OntologySnapshot {p.ontology_snapshot_id} not found")
        corpus = session.get(AnnotationSet, uuid.UUID(p.annotation_set_id))
        if corpus is None:
            raise ValueError(f"AnnotationSet {p.annotation_set_id} not found")
        evidence_codes = resolve_regime(p.evidence_regime)
        existing = (
            session.query(InformationAccretionSet)
            .filter_by(
                ontology_snapshot_id=snapshot.id,
                annotation_set_id=corpus.id,
                evidence_regime=p.evidence_regime,
            )
            .one_or_none()
        )
        return _Inputs(p, payload, snapshot, corpus, evidence_codes, existing)

    @staticmethod
    def _reuse_result(existing: Any, emit: EmitFn) -> OperationResult:
        """Report the existing table instead of recomputing it.

        ``reused`` is in the result rather than only in the log, so a caller
        reading the job outcome can tell a fresh computation from a hit without
        parsing events.
        """
        emit(
            "compute_information_accretion.reused",
            "table already exists for these three axes",
            {"information_accretion_set_id": str(existing.id),
             "artifact_uri": existing.artifact_uri},
            "info",
        )
        return OperationResult(
            result={
                "information_accretion_set_id": str(existing.id),
                "artifact_uri": existing.artifact_uri,
                "content_sha256": existing.content_sha256,
                "reused": True,
            }
        )

    @staticmethod
    def _record_shape(
        ia_set: InformationAccretionSet,
        inp: "_Inputs",
        dag: "_Dag",
        measured: "_Measured",
        uri: str,
        digest: str,
    ) -> None:
        """Write the shape of the table onto its own row.

        These counters are what make wrong provenance visible without fetching
        the artifact. Two tables over the same snapshot and corpus but
        different evidence regimes differ here and nowhere else in the row.
        """
        ia = dag.ia
        values = list(ia.values())
        ia_set.artifact_uri = uri
        ia_set.content_sha256 = digest
        ia_set.term_count = len(dag.terms)
        ia_set.nonzero_count = measured.gate_stats["nonzero"]
        ia_set.annotation_count = measured.raw
        ia_set.protein_count = len(measured.proteins)
        ia_set.propagated_pairs = measured.propagated_pairs
        ia_set.ia_max = max(values)
        ia_set.ia_mean = sum(values) / len(values)
        ia_set.stats = {
            **measured.gate_stats,
            "obo_version": inp.snapshot.obo_version,
            "corpus_source_version": inp.corpus.source_version,
            "terms_with_proteins": len(dag.proteins_per_term),
            "aspects": sorted({a for a in measured.aspect_of.values() if a}),
            "elapsed_s": round(time.time() - measured.started, 1),
        }

    @staticmethod
    def _persist(
        session: Session,
        inp: "_Inputs",
        dag: "_Dag",
        measured: "_Measured",
        emit: EmitFn,
    ) -> OperationResult:
        """Write the row and the artifact, and report what was written.

        The shape counters live on the row rather than only in the artifact so
        wrong provenance is visible without fetching the file: the same
        snapshot and corpus give ia_max 18.956 over all evidence and 15.943
        under the LAFA regime, and a row that did not carry them would look
        identical either way.
        """
        p, existing = inp.p, inp.existing
        raw, terms, ia = measured.raw, dag.terms, dag.ia
        ia_set = existing or InformationAccretionSet(id=uuid.uuid4())
        ia_set.ontology_snapshot_id = inp.snapshot.id
        ia_set.annotation_set_id = inp.corpus.id
        ia_set.evidence_regime = p.evidence_regime
        ia_set.evidence_codes = (
            list(inp.evidence_codes) if inp.evidence_codes else None
        )
        ia_set.job_id = job_id_from_payload(inp.payload)

        key, uri, digest = self._write_artifact(ia, ia_set.id)

        ComputeInformationAccretionOperation._record_shape(
            ia_set, inp, dag, measured, uri, digest
        )
        if existing is None:
            session.add(ia_set)
        session.flush()

        emit(
            "compute_information_accretion.persisted",
            None,
            {"information_accretion_set_id": str(ia_set.id),
             "artifact_uri": uri, "key": key, "content_sha256": digest,
             "ia_max": ia_set.ia_max, "ia_mean": ia_set.ia_mean},
            "info",
        )
        return OperationResult(
            result={
                "information_accretion_set_id": str(ia_set.id),
                "artifact_uri": uri,
                "content_sha256": digest,
                "term_count": ia_set.term_count,
                "nonzero_count": ia_set.nonzero_count,
                "annotation_count": raw,
                "protein_count": ia_set.protein_count,
                "ia_max": ia_set.ia_max,
                "ia_mean": ia_set.ia_mean,
                "evidence_regime": p.evidence_regime,
                "reused": False,
            },
            progress_current=len(terms),
            progress_total=len(terms),
        )

    @staticmethod
    def _write_artifact(ia: dict[str, float], ia_set_id: uuid.UUID) -> tuple[str, str, str]:
        """Serialise the table and publish it. Returns (key, uri, sha256).

        The digest is taken from the bytes that were written rather than
        recomputed from the mapping, so it certifies the file the store
        received and not an equivalent one this process could have produced.
        """
        project_root = Path(__file__).resolve().parents[3]
        store = get_artifact_store(load_settings(project_root))
        key = ia_key_for(ia_set_id)
        with tempfile.TemporaryDirectory(prefix="protea_ia_") as tmp:
            local_path = Path(tmp) / "IA.tsv"
            with local_path.open("w", encoding="utf-8") as fh:
                for go_id in sorted(ia):
                    fh.write(f"{go_id}\t{ia[go_id]}\n")
            digest = hashlib.sha256(local_path.read_bytes()).hexdigest()
            uri = store.put(key, local_path)
        return key, uri, digest

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        """One line naming the three axes IA is identified by.

        The regime is first because it is the axis most often left implicit:
        the same snapshot and corpus give ia_max 18.956 over all evidence and
        15.943 under the LAFA regime, so a history entry that omitted it would
        make two different tables look like the same job run twice.
        """
        p = payload or {}
        regime = p.get("evidence_regime") or DEFAULT_REGIME
        snapshot = str(p.get("ontology_snapshot_id") or "")[:8]
        corpus = str(p.get("annotation_set_id") or "")[:8]
        bits = [f"regime={regime}"]
        if snapshot:
            bits.append(f"snapshot={snapshot}")
        if corpus:
            bits.append(f"corpus={corpus}")
        if p.get("force"):
            bits.append("force")
        return " ".join(bits)

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = ComputeInformationAccretionPayload.model_validate(payload)
        started = time.time()

        inp = self._resolve(session, p, payload)
        if inp.existing is not None and not p.force:
            return self._reuse_result(inp.existing, emit)
        snapshot, corpus, evidence_codes, existing = (
            inp.snapshot, inp.corpus, inp.evidence_codes, inp.existing
        )

        emit(
            "compute_information_accretion.started",
            None,
            {
                "ontology_snapshot": snapshot.obo_version,
                "annotation_set": corpus.source_version,
                "evidence_regime": p.evidence_regime,
                "evidence_codes": list(evidence_codes) if evidence_codes else None,
            },
            "info",
        )

        terms, aspect_of, parents = self._load_dag(session, str(snapshot.id))
        ancestors = build_ancestors(parents)
        for t in terms:
            ancestors.setdefault(t, frozenset({t}))
        emit(
            "compute_information_accretion.dag_loaded",
            None,
            {"terms": len(terms),
             "tpr_edges": sum(len(v) for v in parents.values())},
            "info",
        )

        proteins_per_term, raw, dropped, proteins = self._propagate(
            session, str(corpus.id), evidence_codes, ancestors, emit
        )
        propagated_pairs = sum(len(v) for v in proteins_per_term.values())
        emit(
            "compute_information_accretion.propagated",
            None,
            {"raw": raw, "dropped": dropped, "proteins": len(proteins),
             "propagated_pairs": propagated_pairs},
            "info",
        )

        ia = {t: term_ia(t, parents.get(t), proteins_per_term) for t in terms}

        dag = _Dag(terms, parents, ancestors, proteins_per_term, ia)
        gate_stats = self._gate(dag, raw, dropped, p.max_drop_rate_pct)
        emit("compute_information_accretion.gates_passed", None, gate_stats, "info")

        measured = _Measured(gate_stats, raw, proteins, propagated_pairs, aspect_of, started)
        return self._persist(session, inp, dag, measured, emit)
