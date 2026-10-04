"""Evolucion del corpus de anotaciones a lo largo de las releases cargadas.

QUE PREGUNTA CONTESTA. Como crece y como se encoge el conjunto de anotaciones
experimentales release a release, y por donde entra el conocimiento nuevo:
proteinas que no tenian nada (NK), proteinas que no tenian nada en ese aspecto
(LK), y proteinas que ya tenian y profundizan (PK).

LA UNIDAD ES (proteina, aspecto), no el par (proteina, termino). Es la unidad
que usa la evaluacion de la plataforma, y respetarla importa: NK es una
propiedad GLOBAL de la proteina -- sin experimental en NINGUN aspecto en t0 --
mientras LK y PK son por aspecto. Definir NK por aspecto produciria un
vocabulario paralelo, incompatible con los evaluation_set ya construidos.

SOLO EXPERIMENTAL, con ``_EXP_CODES``, porque los estratos de la plataforma lo
son. Eso deja ~585.000 pares por release en lugar de ~5 millones.

SE PROPAGA ANTES DE DIFERENCIAR, igual que ``_reconcile_experimental_side``.
No es un adorno. NK y LK son invariantes a propagar -- preguntan si hay ALGO, y
propagar no crea entradas para una proteina que no tenia ninguna -- pero PK no
lo es: una proteina que solo gana un termino MAS GENERAL seria PK en bruto y no
lo es tras propagar, porque el conjunto propagado ya lo contenia.

Y lo mismo vale para ``quitado``, donde esta la diferencia que mas cuesta ver.
Medido el 2026-10-04 sobre la transicion 219 -> 220 en el aspecto F, la mayor
caida de la serie: de 2.316 pares perdidos en una muestra de 3.000 proteinas,
el 97,4% eran ancestros de un termino que la misma proteina CONSERVA. En bruto
eso parece retirada de conocimiento; tras propagar no aparece siquiera, porque
el termino sigue implicado. Lo que esta operacion cuenta como quitado es, por
tanto, retirada de verdad.

TABLAS TEMP y no permanentes, de modo que una ejecucion interrumpida no deja
residuo que la siguiente tenga que limpiar.
"""

from __future__ import annotations

import hashlib
import tempfile
from pathlib import Path
from typing import Any

from pydantic import field_validator
from sqlalchemy import text
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, OperationResult, ProteaPayload
from protea.core.evaluation import _EXP_CODES
from protea.core.utils import contract_payload
from protea.infrastructure.settings import load_settings
from protea.infrastructure.storage import get_artifact_store

#: La 193 es byte a byte el mismo fichero que la 192, publicado dos veces. Su
#: intervalo aportaria un delta de cero que no es una observacion, y en tasas
#: por dia hunde el tramo porque el denominador no es nulo.
RELEASES_DUPLICADAS = (193,)

PROPAGATE_RELATIONS = ("is_a", "part_of")


def evolution_key_for(job_id: Any, nombre: str) -> str:
    """Clave de almacenamiento de un artefacto de esta operacion."""
    return f"annotation_evolution/{job_id}/{nombre}"


class AnalyzeAnnotationEvolutionPayload(ProteaPayload, frozen=True):
    source: str = "goa"
    #: Primera y ultima release a incluir. ``None`` toma todas las cargadas.
    from_release: int | None = None
    to_release: int | None = None
    #: Releases a excluir ademas de las duplicadas conocidas.
    skip_releases: tuple[int, ...] = ()

    @field_validator("source", mode="before")
    @classmethod
    def must_be_non_empty(cls, v: str) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ValueError("must be a non-empty string")
        return v.strip()


class AnalyzeAnnotationEvolutionOperation:
    name = "analyze_annotation_evolution"
    description = (
        "Walk the loaded annotation releases in order and measure how the "
        "experimental corpus grows and contracts: per-release composition by "
        "aspect, and per-transition NK/LK/PK/removed strata per protein, "
        "following the same CAFA5 rules the evaluation uses."
    )

    # ------------------------------------------------------------- releases
    def _releases(self, session: Session, p: AnalyzeAnnotationEvolutionPayload) -> list[int]:
        """Las releases con carga correcta, en orden, sin las duplicadas.

        Se exige un job SUCCEEDED y no la mera existencia del annotation_set:
        un set abierto por un intento que luego fallo contiene datos parciales,
        y en una serie eso es un escalon que no ocurrio.
        """
        filas = session.execute(
            text(
                "select distinct a.source_version from annotation_set a "
                "where a.source = :src and a.source_version ~ '^[0-9]+$' "
                "and exists (select 1 from job j where j.operation = "
                "'load_goa_annotations' and j.status = 'SUCCEEDED' "
                "and j.payload->>'source_version' = a.source_version)"
            ),
            {"src": p.source},
        ).fetchall()
        fuera = set(RELEASES_DUPLICADAS) | set(p.skip_releases)
        rels = sorted(int(r[0]) for r in filas if int(r[0]) not in fuera)
        if p.from_release is not None:
            rels = [r for r in rels if r >= p.from_release]
        if p.to_release is not None:
            rels = [r for r in rels if r <= p.to_release]
        return rels

    # ------------------------------------------------------------- resumen
    def summarize_payload(self, payload: dict[str, Any]) -> str:
        """Una linea que nombra el recorrido, no solo la operacion.

        El rango va primero porque es lo que distingue dos ejecuciones que por
        lo demas son identicas: la misma operacion sobre 160..235 y sobre
        220..227 mide cosas distintas, y un historial que omitiera el rango
        haria parecer que el mismo trabajo se corrio dos veces.
        """
        p = payload or {}
        desde = p.get("from_release")
        hasta = p.get("to_release")
        tramo = f"{desde or 'inicio'}..{hasta or 'fin'}"
        bits = [f"source={p.get('source') or 'goa'}", f"releases={tramo}"]
        fuera = list(RELEASES_DUPLICADAS) + list(p.get("skip_releases") or ())
        if fuera:
            bits.append("excluidas=" + ",".join(str(x) for x in fuera))
        return " · ".join(bits)

    # ---------------------------------------------------------------- carga
    def _cargar(self, session: Session, p, rel: int, tabla: str) -> None:
        """Los pares experimentales positivos de una release, propagados."""
        session.execute(text(f"drop table if exists {tabla}, {tabla}_c"))
        session.execute(
            text(
                f"create temp table {tabla}_r as "
                "select distinct pga.protein_accession as prot, gt.go_id, gt.aspect as asp "
                "from protein_go_annotation pga "
                "join go_term gt on gt.id = pga.go_term_id "
                "join annotation_set a on a.id = pga.annotation_set_id "
                "where a.source = :src and a.source_version = :rel "
                "and pga.evidence_code = any(:codes) "
                "and (pga.qualifier is null or pga.qualifier not like '%NOT%')"
            ),
            {"src": p.source, "rel": str(rel), "codes": list(_EXP_CODES)},
        )
        self._propagar(session, p, rel, tabla)

    def _propagar(self, session: Session, p, rel: int, tabla: str) -> None:
        """Cierre bajo la True Path Rule, dentro del snapshot de la release."""
        session.execute(
            text(
                f"create temp table {tabla}_c as "
                "with recursive arista as ("
                "  select ch.go_id as hijo, pa.go_id as padre "
                "  from go_term_relationship r "
                "  join go_term ch on ch.id = r.child_go_term_id "
                "  join go_term pa on pa.id = r.parent_go_term_id "
                "  join annotation_set a on a.ontology_snapshot_id = r.ontology_snapshot_id "
                "  where a.source = :src and a.source_version = :rel "
                "    and r.relation_type = any(:rels)), "
                "c(desc_id, anc) as ("
                "  select hijo, padre from arista "
                "  union select c.desc_id, a.padre from c join arista a on a.hijo = c.anc) "
                "select desc_id, anc from c"
            ),
            {"src": p.source, "rel": str(rel), "rels": list(PROPAGATE_RELATIONS)},
        )
        session.execute(text(f"create index on {tabla}_c (desc_id)"))
        session.execute(text(f"analyze {tabla}_c"))
        session.execute(
            text(
                f"create temp table {tabla} as "
                f"select prot, go_id, asp from {tabla}_r "
                "union "
                "select x.prot, k.anc, g.aspect "
                f"from {tabla}_r x join {tabla}_c k on k.desc_id = x.go_id "
                "join go_term g on g.go_id = k.anc "
                "join annotation_set a on a.ontology_snapshot_id = g.ontology_snapshot_id "
                "where a.source = :src and a.source_version = :rel"
            ),
            {"src": p.source, "rel": str(rel)},
        )
        session.execute(text(f"create index on {tabla} (prot, go_id)"))
        session.execute(text(f"create index on {tabla} (prot, asp)"))
        session.execute(text(f"analyze {tabla}"))
        session.execute(text(f"drop table if exists {tabla}_r, {tabla}_c"))

    # ---------------------------------------------------------- composicion
    @staticmethod
    def _composicion(session: Session, tabla: str, rel: int) -> list[tuple]:
        filas = session.execute(
            text(
                "select asp, count(*), count(distinct prot), count(distinct go_id) "
                f"from {tabla} group by 1 order by 1"
            )
        ).fetchall()
        return [(rel, a, int(p), int(pr), int(t)) for a, p, pr, t in filas]

    # ------------------------------------------------------------- estratos
    @staticmethod
    def _estratos(session: Session, viejo: str, nuevo: str, ra: int, rb: int) -> list[tuple]:
        """NK / LK / PK / quitado, con las reglas de _classify_protein_deltas.

        NK se decide ANTES de mirar aspectos, porque es global: una proteina
        sin ninguna entrada experimental en t0 es NK, y todo lo que gana cuenta
        como NK sin importar en cuantos aspectos lo gane. Mezclarlo con el
        reparto por aspecto es el error que esta consulta evita.
        """
        filas = session.execute(
            text(
                "with nueva_prot as ("
                f"  select distinct n.prot from {nuevo} n "
                f"  where not exists (select 1 from {viejo} v where v.prot = n.prot)), "
                "nk as (select n.asp as asp, 'nk' as estrato, "
                "        count(distinct n.prot) as prots, count(*) as terms "
                f"       from {nuevo} n join nueva_prot s on s.prot = n.prot group by 1), "
                "ganado as ("
                "  select n.prot, n.asp, n.go_id, "
                f"        exists (select 1 from {viejo} v "
                "                 where v.prot = n.prot and v.asp = n.asp) as tenia "
                f"  from {nuevo} n "
                "  where not exists (select 1 from nueva_prot s where s.prot = n.prot) "
                f"    and not exists (select 1 from {viejo} v "
                "                      where v.prot = n.prot and v.go_id = n.go_id)), "
                "lk as (select asp, 'lk', count(distinct prot), count(*) from ganado "
                "       where not tenia group by 1), "
                "pk as (select asp, 'pk', count(distinct prot), count(*) from ganado "
                "       where tenia group by 1), "
                "quitado as (select v.asp, 'quitado', count(distinct v.prot), count(*) "
                f"           from {viejo} v where not exists (select 1 from {nuevo} n "
                "             where n.prot = v.prot and n.go_id = v.go_id) group by 1) "
                "select * from nk union all select * from lk "
                "union all select * from pk union all select * from quitado"
            )
        ).fetchall()
        return [(ra, rb, a, e, int(p), int(t)) for a, e, p, t in filas]

    # -------------------------------------------------------------- publicar
    @staticmethod
    def _publicar(job_id: Any, nombre: str, cabecera: str, filas: list[tuple]) -> tuple[str, str]:
        """Escribe un TSV, lo sube, y devuelve (uri, sha256).

        El sha va en el resultado del job porque un artefacto que nadie puede
        verificar no es evidencia: sin el, dos lecturas del mismo uri no son
        comparables si el almacen cambia por debajo.
        """
        store = get_artifact_store(load_settings(Path(__file__).resolve().parents[3]))
        cuerpo = cabecera + "\n" + "\n".join("\t".join(str(c) for c in f) for f in filas) + "\n"
        digest = hashlib.sha256(cuerpo.encode()).hexdigest()
        with tempfile.TemporaryDirectory(prefix="protea_evol_") as tmp:
            local = Path(tmp) / nombre
            local.write_text(cuerpo, encoding="utf-8")
            uri = store.put(evolution_key_for(job_id, nombre), local)
        return uri, digest

    # --------------------------------------------------------------- execute
    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = AnalyzeAnnotationEvolutionPayload.model_validate(contract_payload(payload))
        rels = self._releases(session, p)
        if len(rels) < 2:
            raise ValueError(
                f"hacen falta al menos dos releases cargadas de '{p.source}' para "
                f"medir una evolucion; se encontraron {len(rels)}"
            )
        emit(
            "analyze_annotation_evolution.start",
            None,
            {
                "source": p.source,
                "releases": len(rels),
                "primera": rels[0],
                "ultima": rels[-1],
                "excluidas": list(RELEASES_DUPLICADAS) + list(p.skip_releases),
            },
            "info",
        )
        comp: list[tuple] = []
        est: list[tuple] = []
        ant = nant = None
        for i, rel in enumerate(rels):
            act = "ev_b" if ant in (None, "ev_a") else "ev_a"
            self._cargar(session, p, rel, act)
            comp.extend(self._composicion(session, act, rel))
            if ant is not None:
                est.extend(self._estratos(session, ant, act, nant, rel))
                session.execute(text(f"drop table if exists {ant}"))
            emit(
                "analyze_annotation_evolution.release_done",
                None,
                {"release": rel, "indice": i, "de": len(rels)},
                "info",
            )
            ant, nant = act, rel
        session.execute(text(f"drop table if exists {ant}"))
        return self._resultado(payload, comp, est, rels, emit)

    def _resultado(self, payload, comp, est, rels, emit: EmitFn) -> OperationResult:
        """Sube los dos artefactos y resume lo medido."""
        job_id = payload.get("_job_id", "sin-job")
        uri_c, sha_c = self._publicar(
            job_id, "composicion.tsv", "release\taspecto\tpares\tproteinas\tterminos", comp
        )
        uri_e, sha_e = self._publicar(
            job_id, "estratos.tsv", "de\ta\taspecto\testrato\tproteinas\tterminos", est
        )
        por_estrato: dict[str, int] = {}
        for _de, _a, _asp, estrato, _pr, terms in est:
            por_estrato[estrato] = por_estrato.get(estrato, 0) + terms
        res = {
            "releases": len(rels),
            "primera": rels[0],
            "ultima": rels[-1],
            "transiciones": len(rels) - 1,
            "composicion_uri": uri_c,
            "composicion_sha256": sha_c,
            "estratos_uri": uri_e,
            "estratos_sha256": sha_e,
            "terminos_por_estrato": por_estrato,
        }
        emit("analyze_annotation_evolution.done", None, res, "info")
        return OperationResult(result=res)
