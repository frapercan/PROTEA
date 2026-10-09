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

AND THAT CHOICE IS WHAT MAKES THE RUN SINGLE-THREADED. PostgreSQL never plans a
parallel path over a temporary relation -- ``set_rel_consider_parallel`` returns
early on ``RELPERSISTENCE_TEMP``, so the partial paths are not costed and
discarded, they are never generated -- and ``plan_create_index_workers`` refuses
the same way for the two indexes. Measured on 2026-10-09 over two full cycles:
the backend holds 96% of ONE core with 3.9% of its samples off CPU, while 12 of
the machine's 16 cores sit idle. No value of ``max_parallel_workers_per_gather``
changes that. The way to spend more than one core here is therefore NOT inside a
run, it is to run several over disjoint ranges -- which is what
``seed_from_previous`` exists to make exact.

THE SHARD IS THE RANGE PLUS ITS SEED. ``composicion`` is per release and
``estratos``/``detalle`` are per transition, so a range cannot serve as both
without either repeating its first release or losing its leading transition.
With ``seed_from_previous`` the shards of a series concatenate into exactly what
one run would have written.
"""

from __future__ import annotations

import hashlib
import re
import tempfile
from pathlib import Path
from typing import Any, NamedTuple

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


class _Plan(NamedTuple):
    """What a run will read and what it will describe, which are not the same.

    They were two locals passed side by side, and every caller had to remember
    that the sequence may open with a release the range does not contain. Making
    it one value means the derived parts cannot drift from the parts they come
    from.
    """

    semilla: int | None
    rels: list[int]

    @property
    def secuencia(self) -> list[int]:
        """Every release to load, seed first when there is one."""
        return ([self.semilla] if self.semilla is not None else []) + self.rels

    @property
    def describir(self) -> set[int]:
        """The releases this run owns, which is the sequence without the seed."""
        return set(self.rels)


class SeriesMovedError(RuntimeError):
    """The set of loaded releases changed while the run was reading it.

    Only reachable because the run commits per release instead of holding one
    snapshot of the whole series. A concurrent load that lands inside the range
    would make two artefacts describe different corpora, so the run refuses its
    own output rather than publishing numbers nothing can be attributed to.
    """


class AnalyzeAnnotationEvolutionPayload(ProteaPayload, frozen=True):
    source: str = "goa"
    #: Primera y ultima release a incluir. ``None`` toma todas las cargadas.
    from_release: int | None = None
    to_release: int | None = None
    #: Releases a excluir ademas de las duplicadas conocidas.
    skip_releases: tuple[int, ...] = ()
    #: Also load the release just before ``from_release``, use it only to emit
    #: the first transition, and do NOT describe it in ``composicion``. This is
    #: what makes a range a shard: without it a shard either loses its leading
    #: transition or repeats a release of the series. The operation resolves the
    #: predecessor itself, over the releases that EXIST, so a caller never has to
    #: know that 194 follows 192 or that 211 follows 205.
    seed_from_previous: bool = False
    #: ``temp_buffers`` for this session, e.g. ``"1GB"``. TEMP tables are read
    #: through these buffers and never through ``shared_buffers``, so the default
    #: of 8 MB has the whole working set going back to the page cache on every
    #: scan. ``None`` leaves the server value untouched.
    temp_buffers: str | None = None

    @field_validator("source", mode="before")
    @classmethod
    def must_be_non_empty(cls, v: str) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ValueError("must be a non-empty string")
        return v.strip()

    @field_validator("temp_buffers", mode="before")
    @classmethod
    def must_be_a_memory_size(cls, v: Any) -> Any:
        """Reject a bad size here, not twenty minutes into the run.

        ``set_config`` passes the value as data, so this is not about injection:
        it is about a typo failing at payload construction, where it is cheap,
        instead of on the first statement of a job that was about to run for
        hours.
        """
        if v is None:
            return None
        if not isinstance(v, str) or not re.fullmatch(r"\d+(kB|MB|GB)?", v.strip()):
            raise ValueError("must look like '1GB', '512MB', '262144kB' or '32768'")
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
    def _releases_cargadas(
        self, session: Session, p: AnalyzeAnnotationEvolutionPayload
    ) -> list[int]:
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
        return sorted(int(r[0]) for r in filas if int(r[0]) not in fuera)

    def _secuencia(self, session: Session, p: AnalyzeAnnotationEvolutionPayload) -> _Plan:
        """The seed and the described range, from one read of the series.

        The two are different things and conflating them is the defect this
        separation removes. ``composicion`` describes a release and must appear
        once per release in the whole series; ``estratos`` and ``detalle``
        describe a transition and need the release BEFORE the first one they
        report. A range that serves as both therefore double-counts its first
        release, or, without the seed, loses its leading transition.

        The predecessor is taken from the releases that exist, after the
        duplicates and the skips are out. That is the whole point of resolving
        it here: in this series the release before 194 is 192, because 193 is the
        same file as 192, and the release before 211 is 205, because 206 to 210
        were never published. A caller computing ``from_release - 1`` would be
        wrong at both places, and wrong silently.
        """
        todas = self._releases_cargadas(session, p)
        rels = [
            r
            for r in todas
            if (p.from_release is None or r >= p.from_release)
            and (p.to_release is None or r <= p.to_release)
        ]
        semilla = None
        if p.seed_from_previous and rels:
            previas = [r for r in todas if r < rels[0]]
            semilla = previas[-1] if previas else None
        return _Plan(semilla, rels)

    def _releases(self, session: Session, p: AnalyzeAnnotationEvolutionPayload) -> list[int]:
        """The releases this run describes, which is the range without the seed."""
        return self._secuencia(session, p).rels

    def _comprobar_serie(
        self,
        session: Session,
        p: AnalyzeAnnotationEvolutionPayload,
        plan: _Plan,
    ) -> None:
        """Refuse the output if the series moved while the run was reading it.

        This is the price of committing per release, paid back. Holding one
        transaction gave a single snapshot of the whole series for free; the
        per-release commit gives that up, so the property it was protecting has
        to be checked instead of assumed. The check is one query against a table
        of a few dozen rows, and it turns a corpus that changed underneath the
        run from a silent inconsistency between two artefacts into a failed job.

        It cannot run while ``describir`` is being built, only after: the point
        is to compare the series as it was read against the series as it is now.
        """
        ahora = self._secuencia(session, p)
        if ahora == plan:
            return
        raise SeriesMovedError(
            f"la serie de '{p.source}' cambio mientras se leia: se empezo con "
            f"semilla={plan.semilla} y {len(plan.rels)} releases "
            f"({plan.rels[0]}..{plan.rels[-1]}) y ahora hay semilla={ahora.semilla} "
            f"y {len(ahora.rels)} "
            + (f"({ahora.rels[0]}..{ahora.rels[-1]})" if ahora.rels else "(ninguna)")
            + "; los artefactos describirian corpus distintos y no se publican"
        )

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
        if p.get("seed_from_previous"):
            # A shard and a whole run can name the same range and not be the same
            # work: the shard also loads the release before it. If the summary hid
            # that, two rows of history would look like the same job run twice.
            bits.append("semilla=la anterior")
        fuera = list(RELEASES_DUPLICADAS) + list(p.get("skip_releases") or ())
        if fuera:
            bits.append("excluidas=" + ",".join(str(x) for x in fuera))
        if p.get("temp_buffers"):
            bits.append(f"temp_buffers={p['temp_buffers']}")
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
    @staticmethod
    def _artefactos(d: Path) -> dict[str, _Artefacto]:
        """The three TSVs, which differ in their unit and not only in their name.

        ``composicion`` is one row per release and aspect; the other two are one
        row per transition. Keeping that visible here is what stops a range from
        being used as if it were both.
        """
        return {
            "composicion": _Artefacto(
                d, "composicion.tsv", "release\taspecto\tpares\tproteinas\tterminos"
            ),
            "estratos": _Artefacto(
                d, "estratos.tsv", "de\ta\taspecto\testrato\tproteinas\tterminos"
            ),
            "detalle": _Artefacto(d, "detalle.tsv", "de\ta\tproteina\taspecto\ttermino\tevento"),
        }

    def _recorrer(
        self,
        session: Session,
        p: AnalyzeAnnotationEvolutionPayload,
        plan: _Plan,
        arts: dict[str, _Artefacto],
        emit: EmitFn,
    ) -> None:
        """Walk the sequence, describing only the releases this run owns.

        ``secuencia`` may open with a seed, which is loaded to produce the first
        transition and is NOT in ``describir``. Everything else is the ping-pong
        between two TEMP tables: the release just read becomes the old side of
        the next comparison, so only one of them is ever dropped per step.
        """
        ant = nant = None
        # Bound once: ``describir`` builds a set on every read, and the loop
        # asks for it twice per release.
        secuencia, describir = plan.secuencia, plan.describir
        ant = nant = None
        for i, rel in enumerate(secuencia):
            act = "ev_b" if ant in (None, "ev_a") else "ev_a"
            self._cargar(session, p, rel, act)
            if rel in describir:
                arts["composicion"].anade(self._composicion(session, act, rel))
            if ant is not None:
                arts["estratos"].anade(self._estratos(session, ant, act, nant, rel))
                arts["detalle"].anade(self._detalle(session, ant, act, nant, rel))
                session.execute(text(f"drop table if exists {ant}"))
            emit(
                "analyze_annotation_evolution.release_done",
                None,
                {
                    "release": rel,
                    "indice": i,
                    "de": len(secuencia),
                    "descrita": rel in describir,
                    "detalle_filas": arts["detalle"].filas,
                },
                "info",
            )
            ant, nant = act, rel
            # One transaction for the whole series retains every dropped TEMP
            # file until the end -- measured at 577 MB a release, so 43 GB over
            # the series -- and pins xmin, which stops vacuum across the whole
            # database for as long as the run lasts. Committing here unlinks
            # the files and lets xmin advance. The TEMP tables survive: they
            # are session-scoped and default to ON COMMIT PRESERVE ROWS.
            session.commit()
        session.execute(text(f"drop table if exists {ant}"))

    # --------------------------------------------------------------- execute
    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = AnalyzeAnnotationEvolutionPayload.model_validate(contract_payload(payload))
        if p.temp_buffers:
            # Before the first TEMP table of the session: afterwards the server
            # ignores the change for this session and says nothing about it.
            session.execute(
                text("select set_config('temp_buffers', :v, false)"),
                {"v": p.temp_buffers},
            )
        plan = self._secuencia(session, p)
        if len(plan.secuencia) < 2:
            raise ValueError(
                f"hacen falta al menos dos releases cargadas de '{p.source}' para "
                f"medir una evolucion; se encontraron {len(plan.secuencia)}"
            )
        emit(
            "analyze_annotation_evolution.start",
            None,
            {
                "source": p.source,
                "releases": len(plan.rels),
                "primera": plan.rels[0],
                "ultima": plan.rels[-1],
                "semilla": plan.semilla,
                "excluidas": list(RELEASES_DUPLICADAS) + list(p.skip_releases),
            },
            "info",
        )
        job_id = payload.get("_job_id", "sin-job")
        with tempfile.TemporaryDirectory(prefix="protea_evol_") as tmp:
            arts = self._artefactos(Path(tmp))
            self._recorrer(session, p, plan, arts, emit)
            self._comprobar_serie(session, p, plan)
            res = {a: art.cierra(job_id) for a, art in arts.items()}
        res.update(
            {
                "releases": len(plan.rels),
                "primera": plan.rels[0],
                "ultima": plan.rels[-1],
                "semilla": plan.semilla,
                "transiciones": len(plan.secuencia) - 1,
            }
        )
        emit("analyze_annotation_evolution.done", None, res, "info")
        return OperationResult(result=res)
