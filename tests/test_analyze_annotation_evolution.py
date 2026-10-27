"""Tests de la operacion de evolucion del corpus de anotaciones."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from protea.core.operations.analyze_annotation_evolution import (
    RELEASES_DUPLICADAS,
    AnalyzeAnnotationEvolutionOperation,
    AnalyzeAnnotationEvolutionPayload,
    SeriesMovedError,
    _Plan,
    evolution_key_for,
)


def _sesion_con(releases):
    s = MagicMock()
    s.execute.return_value.fetchall.return_value = [(str(r),) for r in releases]
    return s


class TestQueReleasesEntran:
    """La serie no es "todas las que tengan un annotation_set".

    Dos exclusiones, y las dos se pagaron antes de escribirse. Un set abierto
    por un intento que luego fallo contiene datos parciales, asi que se exige
    un job SUCCEEDED y no la existencia de la fila. Y la release 193 es byte a
    byte el mismo fichero que la 192, publicado dos veces: su intervalo
    aportaria un delta de cero que no es una observacion.
    """

    def test_la_193_no_entra_porque_es_la_192_repetida(self):
        op = AnalyzeAnnotationEvolutionOperation()
        p = AnalyzeAnnotationEvolutionPayload()
        rels = op._releases(_sesion_con([191, 192, 193, 194]), p)
        assert 193 not in rels, (
            "la 193 es el mismo fichero que la 192; incluirla mete un intervalo "
            "con delta cero que no es una observacion"
        )
        assert rels == [191, 192, 194]

    def test_exige_un_job_correcto_y_no_solo_el_set(self):
        op = AnalyzeAnnotationEvolutionOperation()
        s = _sesion_con([200, 201])
        op._releases(s, AnalyzeAnnotationEvolutionPayload())
        sql = str(s.execute.call_args[0][0])
        assert "SUCCEEDED" in sql, (
            "sin exigir job SUCCEEDED entraria un set parcial de un intento "
            "fallido, que en una serie es un escalon que no ocurrio"
        )

    def test_el_rango_acota_por_los_dos_lados(self):
        op = AnalyzeAnnotationEvolutionOperation()
        p = AnalyzeAnnotationEvolutionPayload(from_release=220, to_release=227)
        rels = op._releases(_sesion_con([218, 220, 225, 227, 230]), p)
        assert rels == [220, 225, 227]

    def test_se_pueden_excluir_mas_releases(self):
        op = AnalyzeAnnotationEvolutionOperation()
        p = AnalyzeAnnotationEvolutionPayload(skip_releases=(201,))
        rels = op._releases(_sesion_con([200, 201, 202]), p)
        assert rels == [200, 202]
        assert RELEASES_DUPLICADAS == (193,)


class TestUnaEvolucionNecesitaDosPuntos:
    def test_se_niega_con_una_sola_release(self):
        op = AnalyzeAnnotationEvolutionOperation()
        with pytest.raises(ValueError, match="al menos dos releases"):
            op.execute(_sesion_con([220]), {}, emit=lambda *a, **k: None)

    def test_se_niega_con_ninguna(self):
        op = AnalyzeAnnotationEvolutionOperation()
        with pytest.raises(ValueError, match="al menos dos releases"):
            op.execute(_sesion_con([]), {}, emit=lambda *a, **k: None)


class TestNKSeDecideAntesDeRepartirPorAspecto:
    """NK es una propiedad GLOBAL de la proteina, no por aspecto.

    Asi lo define `_classify_protein_deltas`: una proteina sin ninguna entrada
    experimental en t0 es NK, y todo lo que gana cuenta como NK sin importar en
    cuantos aspectos lo gane. LK y PK si son por aspecto.

    Definir NK por aspecto habria producido un vocabulario paralelo,
    incompatible con los evaluation_set ya construidos -- y es el error natural
    si uno escribe la consulta agrupando primero.
    """

    def test_la_consulta_separa_las_proteinas_nuevas_antes_de_agrupar(self):
        s = MagicMock()
        s.execute.return_value.fetchall.return_value = []
        AnalyzeAnnotationEvolutionOperation._estratos(s, "viejo", "nuevo", 219, 220)
        sql = str(s.execute.call_args[0][0])
        assert "nueva_prot" in sql
        i_nueva = sql.index("nueva_prot as")
        i_ganado = sql.index("ganado as")
        assert i_nueva < i_ganado, (
            "el reparto por aspecto se calcula antes que el conjunto de "
            "proteinas nuevas: NK dejaria de ser global"
        )
        # y lo repartido por aspecto excluye explicitamente a las NK
        tras = sql[i_ganado:]
        assert "not exists (select 1 from nueva_prot" in tras, (
            "LK y PK no excluyen a las proteinas NK, asi que una proteina "
            "nueva se contaria dos veces"
        )


class TestLaClaveDelArtefactoLlevaElJob:
    def test_la_clave_incluye_el_job(self):
        k = evolution_key_for("abc-123", "estratos.tsv")
        assert k == "annotation_evolution/abc-123/estratos.tsv", (
            "sin el job en la clave, dos ejecuciones se pisan el artefacto y "
            "ninguna cifra queda atribuible"
        )


class TestElDetalleEsLoQueDeVerdadSeQuiere:
    """Una fila por (transicion, proteina, aspecto, termino, evento).

    Los agregados por aspecto dicen cuanto se movio; esto dice QUE se movio y
    A QUIEN, que es el material sobre el que se puede trabajar despues. Son
    5.148.173 filas sobre las 69 transiciones, con mediana de 54.350 por
    transicion y un maximo de 375.372.
    """

    @staticmethod
    def _sql():
        s = MagicMock()
        AnalyzeAnnotationEvolutionOperation._detalle(s, "viejo", "nuevo", 219, 220)
        return str(s.execute.call_args[0][0])

    def test_emite_los_cuatro_eventos(self):
        sql = self._sql()
        for ev in ("'nk'", "'pk'", "'lk'", "'quitado'"):
            assert ev in sql, f"el detalle no clasifica {ev}"

    def test_nk_se_decide_antes_de_mirar_el_aspecto(self):
        """Igual que en los agregados: NK es global, no por aspecto."""
        sql = self._sql()
        i_nk = sql.index("then 'nk'")
        i_pk = sql.index("then 'pk'")
        assert i_nk < i_pk, (
            "la rama de aspecto se evalua antes que la de proteina nueva, asi "
            "que una proteina NK se clasificaria como LK o PK"
        )

    def test_no_acumula_en_memoria(self):
        """yield_per, porque la transicion mayor aporta 375.372 filas.

        Acumularlas para escribirlas luego es el patron que ya tumbo al worker
        una vez: una sola estructura que crece con el corpus.
        """
        s = MagicMock()
        AnalyzeAnnotationEvolutionOperation._detalle(s, "viejo", "nuevo", 219, 220)
        assert s.execute.return_value.yield_per.called, (
            "el detalle se materializa entero en lugar de recorrerse"
        )


class TestElArtefactoHasheaLoQueEscribe:
    """El sha se calcula en el mismo recorrido que la escritura.

    Hacerlo leyendo el fichero despues abre un hueco entre lo que se subio y
    lo que se midio. Un artefacto cuyo sha no describe sus bytes no sirve para
    comparar dos lecturas.
    """

    def test_el_sha_corresponde_al_contenido_y_cuenta_las_filas(self, tmp_path):
        import hashlib

        from protea.core.operations.analyze_annotation_evolution import _Artefacto

        a = _Artefacto(tmp_path, "x.tsv", "a\tb")
        a.anade([(1, 2), (3, 4)])
        a.anade([(5, 6)])
        esperado = hashlib.sha256(b"a\tb\n1\t2\n3\t4\n5\t6\n").hexdigest()
        # cierra() sube el fichero; se comprueba el hash y el contador sin subir
        assert a.filas == 3
        a._fh.close()
        assert a.ruta.read_text() == "a\tb\n1\t2\n3\t4\n5\t6\n"
        assert a._h.hexdigest() == esperado, "el sha no describe los bytes escritos"


#: The series as it is actually loaded: 156 to 235, with 206 to 210 never
#: published and 193 excluded because it is the same file as 192. Both gaps are
#: in here on purpose, because both are where a shard boundary goes wrong.
SERIE_REAL = [r for r in range(156, 236) if r not in range(206, 211)]


def _recorrido(op, releases, **campos):
    """What a run with these fields would describe, and which transitions it emits."""
    p = AnalyzeAnnotationEvolutionPayload(**campos)
    plan = op._secuencia(_sesion_con(releases), p)
    s = plan.secuencia
    return plan.semilla, plan.rels, list(zip(s, s[1:], strict=False))


class TestAShardIsTheRangePlusItsSeed:
    """A range cannot be both the releases described and the transitions emitted.

    ``composicion`` is one row per release and ``estratos``/``detalle`` are one
    row per transition. A shard that just narrows the range therefore has to
    choose: describe its first release and lose the transition that leads into
    it, or keep the transition by starting a release earlier and describe that
    release a second time. The seed is the third option, and these tests pin the
    two boundaries where getting it wrong is invisible.
    """

    def test_the_seed_is_the_release_that_exists_and_not_the_number_before(self):
        op = AnalyzeAnnotationEvolutionOperation()
        semilla, rels, _ = _recorrido(op, SERIE_REAL, from_release=194, seed_from_previous=True)
        assert semilla == 192, (
            "the release before 194 is 192, because 193 is byte for byte the same "
            "file as 192 and is excluded; a caller computing from_release - 1 "
            "would seed with a release that is not in the series"
        )
        assert 192 not in rels

    def test_the_seed_crosses_a_gap_in_the_series(self):
        op = AnalyzeAnnotationEvolutionOperation()
        semilla, _, _ = _recorrido(op, SERIE_REAL, from_release=211, seed_from_previous=True)
        assert semilla == 205, (
            "206 to 210 were never published, so the release before 211 is 205; "
            "this is the second place where arithmetic on the number fails"
        )

    def test_the_seed_is_not_described(self):
        op = AnalyzeAnnotationEvolutionOperation()
        semilla, rels, transiciones = _recorrido(
            op, SERIE_REAL, from_release=194, to_release=196, seed_from_previous=True
        )
        assert semilla not in rels, (
            "describing the seed is exactly the duplication this field removes: "
            "the previous shard already described it"
        )
        assert transiciones[0] == (192, 194), "the seed exists to emit this one"

    def test_without_a_seed_the_leading_transition_is_lost(self):
        op = AnalyzeAnnotationEvolutionOperation()
        _, _, con = _recorrido(
            op, SERIE_REAL, from_release=194, to_release=196, seed_from_previous=True
        )
        _, _, sin = _recorrido(op, SERIE_REAL, from_release=194, to_release=196)
        assert (192, 194) in con and (192, 194) not in sin, (
            "a shard without a seed silently drops the transition into its first "
            "release, and at this boundary that is the transition across 193"
        )

    def test_the_first_shard_asks_for_a_seed_and_correctly_gets_none(self):
        op = AnalyzeAnnotationEvolutionOperation()
        semilla, rels, _ = _recorrido(op, SERIE_REAL, to_release=170, seed_from_previous=True)
        assert semilla is None, "nothing precedes the first release of the series"
        assert rels[0] == 156

    def test_shards_describe_every_release_once_and_emit_every_transition_once(self):
        op = AnalyzeAnnotationEvolutionOperation()
        _, enteras, todas_trans = _recorrido(op, SERIE_REAL)

        cortes = [156, 180, 200, 216, 236]
        descritas: list[int] = []
        transiciones: list[tuple[int, int]] = []
        for i, (desde, hasta) in enumerate(zip(cortes, cortes[1:], strict=False)):
            _, rels, trans = _recorrido(
                op,
                SERIE_REAL,
                from_release=desde,
                to_release=hasta - 1,
                seed_from_previous=i > 0,
            )
            descritas += rels
            transiciones += trans

        assert descritas == enteras, (
            "the shards concatenate into the whole series with nothing repeated "
            "and nothing missing; a repeat here is the defect class already on "
            "record for 192 and 193, a series counting one interval twice"
        )
        assert transiciones == todas_trans
        assert len(set(descritas)) == len(descritas)
        assert len(set(transiciones)) == len(transiciones)


class TestTheSeriesIsCheckedWhenTheRunCommitsPerRelease:
    """Committing per release gives up the single snapshot, so it is verified.

    One transaction for the whole run retained every dropped TEMP file to the
    end and pinned xmin for hours. Committing per release fixes both and costs
    the guarantee that all the artefacts describe one corpus, so that guarantee
    stops being free and starts being checked.
    """

    def test_a_series_that_did_not_move_passes(self):
        op = AnalyzeAnnotationEvolutionOperation()
        p = AnalyzeAnnotationEvolutionPayload()
        op._comprobar_serie(_sesion_con([200, 201, 202]), p, _Plan(None, [200, 201, 202]))

    def test_a_release_that_appears_mid_run_refuses_the_artefacts(self):
        op = AnalyzeAnnotationEvolutionOperation()
        p = AnalyzeAnnotationEvolutionPayload()
        s = _sesion_con([200, 201, 202, 203])
        with pytest.raises(SeriesMovedError) as e:
            op._comprobar_serie(s, p, _Plan(None, [200, 201, 202]))
        assert "cambio mientras se leia" in str(e.value)

    def test_a_seed_that_moved_also_refuses(self):
        op = AnalyzeAnnotationEvolutionOperation()
        p = AnalyzeAnnotationEvolutionPayload(from_release=201, seed_from_previous=True)
        s = _sesion_con([199, 200, 201, 202])
        with pytest.raises(SeriesMovedError):
            op._comprobar_serie(s, p, _Plan(198, [201, 202]))


class TestTempBuffersFailsAtTheEdgeOrNotAtAll:
    """A typo in a size should cost a payload, not four hours of a run."""

    @pytest.mark.parametrize("v", ["1GB", "512MB", "262144kB", "32768"])
    def test_a_size_is_accepted(self, v):
        assert AnalyzeAnnotationEvolutionPayload(temp_buffers=v).temp_buffers == v

    @pytest.mark.parametrize("v", ["1 GB", "1gb", "GB", "1TB", "mucho", ""])
    def test_a_typo_is_refused_before_the_job_starts(self, v):
        with pytest.raises(ValueError):
            AnalyzeAnnotationEvolutionPayload(temp_buffers=v)

    def test_none_is_the_default_and_leaves_the_server_alone(self):
        assert AnalyzeAnnotationEvolutionPayload().temp_buffers is None

    def test_the_summary_names_the_shard_and_the_buffers(self):
        op = AnalyzeAnnotationEvolutionOperation()
        linea = op.summarize_payload(
            {
                "from_release": 194,
                "to_release": 216,
                "seed_from_previous": True,
                "temp_buffers": "1GB",
            }
        )
        assert "semilla" in linea and "temp_buffers=1GB" in linea, (
            "a shard and a whole run can name the same range and not be the same "
            "work; a history that hid the seed would show one job run twice"
        )


class _SesionDeSerie:
    """A session that answers by the shape of the query, to drive a whole run.

    The other tests in this file read the SQL a method built. This one is for
    the opposite question: not what a statement says, but which statements the
    loop decides to issue, and for which releases. That decision is where the
    shard boundary was wrong, and it is not visible from any single method.
    """

    def __init__(self, releases, aspectos=("C", "F", "P")):
        self.releases = list(releases)
        self.aspectos = aspectos
        self.commits = 0

    def execute(self, clause, params=None):
        sql = str(clause)
        r = MagicMock()
        if "a.source_version from annotation_set" in sql:
            r.fetchall.return_value = [(str(x),) for x in self.releases]
        elif "count(distinct go_id)" in sql:
            r.fetchall.return_value = [(a, 1, 1, 1) for a in self.aspectos]
        else:
            r.fetchall.return_value = []
            r.yield_per.return_value = []
        return r

    def commit(self):
        self.commits += 1


class _AlmacenQueLee:
    """An artifact store that keeps what was written, so a test can read it."""

    def __init__(self):
        self.ficheros: dict[str, list[str]] = {}

    def put(self, key, ruta):
        self.ficheros[key.rsplit("/", 1)[-1]] = ruta.read_text(encoding="utf-8").splitlines()
        return f"file://{key}"


def _corre(monkeypatch, releases, **campos):
    """Drive the whole operation and give back what each artefact got."""
    almacen = _AlmacenQueLee()
    monkeypatch.setattr(
        "protea.core.operations.analyze_annotation_evolution.get_artifact_store",
        lambda *_a, **_k: almacen,
    )
    op = AnalyzeAnnotationEvolutionOperation()
    s = _SesionDeSerie(releases)
    eventos: list[tuple] = []
    op.execute(
        s,
        {"_job_id": "j", **campos},
        emit=lambda ev, _a, campos_ev, _n: eventos.append((ev, campos_ev)),
    )
    return almacen, eventos, s


class TestTheLoopDescribesEachReleaseOnceAcrossShards:
    """The guard that keeps ``composicion`` out of the seed, driven end to end.

    ``_secuencia`` deciding that the seed is not in the described range is only
    half of it: the loop has to act on that. These run the operation and read
    the artefact, so a change that emitted composition for the whole sequence
    would fail here even though every other test still passed.
    """

    def test_the_seed_gets_no_composition_row(self, monkeypatch):
        almacen, _, _ = _corre(
            monkeypatch,
            SERIE_REAL,
            from_release=194,
            to_release=196,
            seed_from_previous=True,
        )
        filas = almacen.ficheros["composicion.tsv"][1:]
        descritas = sorted({int(f.split("\t")[0]) for f in filas})
        assert descritas == [194, 195, 196], (
            "192 is loaded to emit the transition into 194 and must not be "
            "described: the shard before this one already described it"
        )

    def test_the_transition_into_the_first_release_is_still_emitted(self, monkeypatch):
        _, eventos, _ = _corre(
            monkeypatch,
            SERIE_REAL,
            from_release=194,
            to_release=196,
            seed_from_previous=True,
        )
        hechas = [c for e, c in eventos if e.endswith("release_done")]
        assert [c["release"] for c in hechas] == [192, 194, 195, 196]
        assert [c["descrita"] for c in hechas] == [False, True, True, True]
        arranque = next(c for e, c in eventos if e.endswith(".start"))
        assert arranque["semilla"] == 192, (
            "the seed belongs in the start event: a run whose range looks the "
            "same but read one release more is not the same work"
        )

    def test_the_shards_concatenate_into_the_whole_series(self, monkeypatch):
        entera, _, _ = _corre(monkeypatch, SERIE_REAL)
        cortes = [156, 180, 200, 216, 236]
        troceada: list[str] = []
        for i, (desde, hasta) in enumerate(zip(cortes, cortes[1:], strict=False)):
            parte, _, _ = _corre(
                monkeypatch,
                SERIE_REAL,
                from_release=desde,
                to_release=hasta - 1,
                seed_from_previous=i > 0,
            )
            troceada += parte.ficheros["composicion.tsv"][1:]
        assert troceada == entera.ficheros["composicion.tsv"][1:], (
            "four shards must write exactly the rows one run writes; a repeated "
            "boundary release is the defect already on record for 192 and 193, "
            "a series counting one interval twice"
        )

    def test_it_commits_once_per_release(self, monkeypatch):
        _, _, s = _corre(
            monkeypatch, SERIE_REAL, from_release=194, to_release=196, seed_from_previous=True
        )
        assert s.commits == 4, (
            "one transaction for the whole run retained every dropped TEMP file "
            "to the end, measured at 577 MB a release, and pinned xmin for hours"
        )

    def test_temp_buffers_is_set_before_the_first_temp_table(self, monkeypatch):
        vistas: list[str] = []
        almacen = _AlmacenQueLee()
        monkeypatch.setattr(
            "protea.core.operations.analyze_annotation_evolution.get_artifact_store",
            lambda *_a, **_k: almacen,
        )

        class _Espia(_SesionDeSerie):
            def execute(self, clause, params=None):
                vistas.append(str(clause))
                return super().execute(clause, params)

        AnalyzeAnnotationEvolutionOperation().execute(
            _Espia(SERIE_REAL),
            {"_job_id": "j", "to_release": 158, "temp_buffers": "1GB"},
            emit=lambda *_a: None,
        )
        i_set = next(i for i, s in enumerate(vistas) if "temp_buffers" in s)
        i_temp = next(i for i, s in enumerate(vistas) if "create temp table" in s)
        assert i_set < i_temp, (
            "after the first TEMP table of the session the server ignores the "
            "change and reports nothing, so the order is the whole guarantee"
        )
