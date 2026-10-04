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
