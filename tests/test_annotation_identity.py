"""La identidad de una anotacion: que conserva, y que las dos capas coincidan.

La restriccion anterior era ``(set, protein, go_term, evidence_code)``, asi que
dos filas del GAF que difirieran solo en ``qualifier`` o en ``db_reference``
colapsaban, y el cargador usa ``on_conflict_do_nothing``: ganaba la primera en
orden de fichero. Medido sobre GOA 156, 875.990 filas fiables caian a 620.565
tripletas -- el 29,2% -- y 358 pares (proteina, termino) perdian el NOT del todo,
que es lo que ``_reconcile_not_side`` necesita para restar.

Dos cosas de este arreglo fallarian en silencio, y las dos se fijan aqui.
"""

from __future__ import annotations

from protea.core.operations.load_goa_annotations import (
    _IDENTITY_ELEMENTS,
    _identity_key,
)
from protea.infrastructure.orm.models.annotation.protein_go_annotation import (
    ProteinGOAnnotation,
)

_ESPERADO = (
    "annotation_set_id",
    "protein_accession",
    "go_term_id",
    "coalesce(evidence_code, '')",
    "coalesce(qualifier, '')",
    "coalesce(db_reference, '')",
)


class TestLasDosCapasDicenLoMismo:
    """Hay DOS deduplicados: el ``seen`` del buffer y el ``on_conflict`` de la
    base. Si solo se ensancha uno, el otro sigue colapsando y el cambio parece
    hecho sin estarlo. Esa es la forma de fallo que este fichero existe para
    impedir."""

    def test_el_on_conflict_apunta_a_la_identidad_completa(self):
        assert tuple(str(e) for e in _IDENTITY_ELEMENTS) == _ESPERADO

    def test_el_dedup_del_buffer_usa_los_mismos_seis_campos(self):
        """Un campo de menos aqui y la polaridad se pierde EN EL BUFFER, antes de
        que la base pueda opinar. Se llama a la funcion con dos registros que solo
        difieren en qualifier: tienen que dar claves distintas."""
        import uuid
        from types import SimpleNamespace

        sid = uuid.uuid4()

        def rec(qualifier=None, ref=None, ev="IDA"):
            return SimpleNamespace(evidence_code=ev, qualifier=qualifier, db_reference=ref)

        base = _identity_key(sid, "P12345", 7, rec())
        assert len(base) == 6, "seis campos, como el indice"
        assert _identity_key(sid, "P12345", 7, rec(qualifier="NOT")) != base, (
            "el NOT tiene que dar una clave distinta o se pierde en el buffer"
        )
        assert _identity_key(sid, "P12345", 7, rec(ref="PMID:2")) != base, (
            "una referencia distinta tiene que dar una clave distinta"
        )
        assert _identity_key(sid, "P12345", 7, rec(ev="IMP")) != base

    def test_None_y_cadena_vacia_son_la_misma_clave(self):
        """Porque en el indice lo son: ``coalesce(col, '')``. Si el buffer los
        tratara como distintos, dos filas que la base va a fundir pasarian las dos
        y la insercion dependeria del orden."""
        import uuid
        from types import SimpleNamespace

        sid = uuid.uuid4()
        vacio = SimpleNamespace(evidence_code="IDA", qualifier="", db_reference="")
        nulo = SimpleNamespace(evidence_code="IDA", qualifier=None, db_reference=None)
        assert _identity_key(sid, "P12345", 7, vacio) == _identity_key(sid, "P12345", 7, nulo)


class TestElIndiceEsSobreCoalesce:
    """En Postgres ``NULL`` nunca entra en conflicto con ``NULL``, y en GOA 156 el
    qualifier esta vacio en 871.484 de 875.990 filas fiables. Una
    ``UniqueConstraint`` que lo incluyera dejaria de deduplicar justo ahi, y cada
    reintento de una carga interrumpida duplicaria el corpus."""

    def test_no_queda_ninguna_unique_constraint_en_la_tabla(self):
        from sqlalchemy import UniqueConstraint

        uniques = [
            c for c in ProteinGOAnnotation.__table__.constraints
            if isinstance(c, UniqueConstraint)
        ]
        assert uniques == [], (
            "una UniqueConstraint sobre columnas opcionales no deduplica: "
            f"{[c.name for c in uniques]}"
        )

    def test_el_indice_unico_existe_y_cubre_los_seis(self):
        idx = {i.name: i for i in ProteinGOAnnotation.__table__.indexes}
        assert "uq_pga_annotation_identity" in idx
        objetivo = idx["uq_pga_annotation_identity"]
        assert objetivo.unique is True
        texto = " ".join(str(e) for e in objetivo.expressions)
        for campo in ("evidence_code", "qualifier", "db_reference"):
            assert f"coalesce({campo}, '')" in texto or campo in texto, campo

    def test_la_migracion_y_el_modelo_nombran_el_mismo_indice(self):
        """Un nombre distinto en la migracion deja el indice sin crear y el
        ``on_conflict`` apuntando a nada, que en Postgres falla en la primera
        insercion y no antes. Se importa el modulo en vez de leer su texto: la
        migracion monta el DDL con f-strings, asi que buscar la cadena ya
        ensamblada comprueba la plantilla y no el valor.
        """
        import importlib.util
        import pathlib

        raiz = pathlib.Path(__file__).resolve().parents[1]
        ruta = next(
            (raiz / "alembic" / "versions").glob("*annotation_identity_keeps_qualifier*")
        )
        spec = importlib.util.spec_from_file_location("mig_identidad", ruta)
        assert spec and spec.loader
        mig = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mig)

        assert mig._NEW == "uq_pga_annotation_identity", "el indice del modelo y el de la migracion"
        assert mig._OLD == "uq_pga_set_protein_term_evidence", "la que se retira"
        assert mig.down_revision == "3cd5f76d282f", "cuelga de la cabeza aplicada"
        for campo in ("evidence_code", "qualifier", "db_reference"):
            assert f"coalesce({campo}, '')" in mig._COLS, campo
        # el indice del modelo y las columnas de la migracion, en el mismo orden
        idx = {i.name: i for i in ProteinGOAnnotation.__table__.indexes}
        del_modelo = [str(e) for e in idx[mig._NEW].expressions]
        assert len(del_modelo) == 6
        for campo in ("annotation_set_id", "protein_accession", "go_term_id"):
            assert campo in " ".join(del_modelo) and campo in mig._COLS, campo


class TestTheGateMatchesTheForeignKey:
    """``_load_accessions`` builds the set that decides which GAF rows may be
    stored. The constraint that actually governs storage is
    ``protein_go_annotation.protein_accession`` referencing ``protein.accession``.

    Until 2026-10-05 the gate read ``canonical_accession``. The two coincide only
    on canonical rows, so every row whose accession differs from its canonical was
    refused by the gate although the foreign key would have taken it: isoforms, and
    the merge aliases PROTEA#980 had just been built to admit. Nothing failed -- the
    rows landed in ``skipped``, which also counts accessions genuinely outside the
    corpus, so the number looked ordinary.
    """

    def test_the_gate_selects_accession_not_canonical_accession(self):
        import inspect

        from protea.core.operations.load_goa_annotations import LoadGOAAnnotationsOperation

        src = inspect.getsource(LoadGOAAnnotationsOperation._load_accessions)
        assert "select(Protein.accession)" in src
        assert "distinct(Protein.canonical_accession)" not in src, (
            "the gate must name the column the foreign key names"
        )

    def test_the_context_field_is_not_called_canonical(self):
        """The field was named ``canonical_accessions``, which is what made the
        defect read as intentional for weeks."""
        from protea.core.operations.load_goa_annotations import _GoaStoreCtx

        assert "admissible_accessions" in _GoaStoreCtx._fields
        assert "canonical_accessions" not in _GoaStoreCtx._fields

    def test_an_alias_row_would_now_be_admitted(self):
        """The end-to-end property: a GAF naming a merged secondary accession must
        pass the gate when that accession exists in ``protein`` as an alias."""
        from unittest.mock import MagicMock

        from protea.core.operations.load_goa_annotations import LoadGOAAnnotationsOperation

        session = MagicMock()
        # P30456 exists as an alias row; its canonical_accession is P04439.
        session.scalars.return_value = ["P04439", "P30456", "P12345-2"]
        got = LoadGOAAnnotationsOperation()._load_accessions(session, MagicMock())
        assert "P30456" in got, "the merge alias has to be admissible"
        assert "P12345-2" in got, "so does an isoform row"
