"""Tests de la operacion de evolucion del corpus de anotaciones."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from protea.core.operations.analyze_annotation_evolution import (
    RELEASES_DUPLICADAS,
    AnalyzeAnnotationEvolutionOperation,
    AnalyzeAnnotationEvolutionPayload,
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
        assert a._h.hexdigest() == esperado, (
            "el sha no describe los bytes escritos"
        )
