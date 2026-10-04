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



class _Artefacto:
    """Un TSV que se escribe en streaming y se hashea de paso.

    No se acumula en memoria porque el detalle son 5.148.173 filas y la
    transicion mayor aporta 375.372: una lista que crece con el corpus es el
    patron que ya tumbo al worker una vez. El sha se calcula sobre lo que se
    escribe, en el mismo recorrido, de modo que no hay una segunda lectura que
    pudiera ver otra cosa.
    """

    def __init__(self, directorio: Path, nombre: str, cabecera: str) -> None:
        self.nombre = nombre
        self.ruta = directorio / nombre
        self._h = hashlib.sha256()
        self._fh = self.ruta.open("w", encoding="utf-8")
        self.filas = 0
        self._escribe(cabecera + "\n")

    def _escribe(self, texto: str) -> None:
        self._fh.write(texto)
        self._h.update(texto.encode())

    def anade(self, filas) -> None:
        for f in filas:
            self._escribe("\t".join(str(c) for c in f) + "\n")
            self.filas += 1

    def cierra(self, job_id: Any) -> dict[str, Any]:
        self._fh.close()
        store = get_artifact_store(load_settings(Path(__file__).resolve().parents[3]))
        uri = store.put(evolution_key_for(job_id, self.nombre), self.ruta)
        return {"uri": uri, "sha256": self._h.hexdigest(), "filas": self.filas}


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

    # -------------------------------------------------------------- detalle
    @staticmethod
    def _detalle(session: Session, viejo: str, nuevo: str, ra: int, rb: int):
        """Una fila por (transicion, proteina, aspecto, termino, evento).

        Esto es el material de verdad: los agregados por aspecto dicen cuanto
        se movio, y esto dice QUE se movio y A QUIEN. Son 5.148.173 filas sobre
        las 69 transiciones, mediana de 54.350 por transicion.

        Se devuelve como cursor y no como lista porque la transicion mayor
        aporta 375.372 filas y acumularlas todas en memoria para escribirlas
        luego es exactamente el patron que reventaba al worker: una sola
        estructura que crece con el corpus.

        Los cuatro eventos son los de ``_classify_protein_deltas``, y el orden
        de las ramas importa igual que alli: NK se decide antes de repartir por
        aspecto, porque es una propiedad global de la proteina.
        """
        return session.execute(
            text(
                "with nueva_prot as ("
                f"  select distinct n.prot from {nuevo} n "
                f"  where not exists (select 1 from {viejo} v where v.prot = n.prot)) "
                f"select {ra}, {rb}, n.prot, n.asp, n.go_id, "
                "       case when s.prot is not null then 'nk' "
                f"            when exists (select 1 from {viejo} v "
                "                 where v.prot = n.prot and v.asp = n.asp) then 'pk' "
                "            else 'lk' end as evento "
                f"from {nuevo} n "
                "left join nueva_prot s on s.prot = n.prot "
                f"where not exists (select 1 from {viejo} v "
                "                    where v.prot = n.prot and v.go_id = n.go_id) "
                "union all "
                f"select {ra}, {rb}, v.prot, v.asp, v.go_id, 'quitado' "
                f"from {viejo} v "
                f"where not exists (select 1 from {nuevo} n "
                "                    where n.prot = v.prot and n.go_id = v.go_id)"
            )
        ).yield_per(50_000)

    # -------------------------------------------------------------- publicar
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
        job_id = payload.get("_job_id", "sin-job")
        with tempfile.TemporaryDirectory(prefix="protea_evol_") as tmp:
            d = Path(tmp)
            arts = {
                "composicion": _Artefacto(
                    d, "composicion.tsv", "release\taspecto\tpares\tproteinas\tterminos"
                ),
                "estratos": _Artefacto(
                    d, "estratos.tsv", "de\ta\taspecto\testrato\tproteinas\tterminos"
                ),
                "detalle": _Artefacto(
                    d, "detalle.tsv", "de\ta\tproteina\taspecto\ttermino\tevento"
                ),
            }
            ant = nant = None
            for i, rel in enumerate(rels):
                act = "ev_b" if ant in (None, "ev_a") else "ev_a"
                self._cargar(session, p, rel, act)
                arts["composicion"].anade(self._composicion(session, act, rel))
                if ant is not None:
                    arts["estratos"].anade(self._estratos(session, ant, act, nant, rel))
                    arts["detalle"].anade(self._detalle(session, ant, act, nant, rel))
                    session.execute(text(f"drop table if exists {ant}"))
                emit(
                    "analyze_annotation_evolution.release_done",
                    None,
                    {"release": rel, "indice": i, "de": len(rels),
                     "detalle_filas": arts["detalle"].filas},
                    "info",
                )
                ant, nant = act, rel
            session.execute(text(f"drop table if exists {ant}"))
            res = {a: art.cierra(job_id) for a, art in arts.items()}
        res.update({"releases": len(rels), "primera": rels[0], "ultima": rels[-1],
                    "transiciones": len(rels) - 1})
        emit("analyze_annotation_evolution.done", None, res, "info")
        return OperationResult(result=res)

