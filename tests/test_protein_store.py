"""El almacen compartido: una sola copia del dedup por MD5 y del upsert.

Estas pruebas vivian en ``tests/test_insert_proteins.py`` cuando el codigo era
``InsertProteinsOperation._store_records``, un metodo privado al que
``ensure_goa_universe`` llegaba desde fuera. Se movieron con el codigo, que es
donde tienen que estar: lo que prueban ahora lo usan DOS operaciones --
``insert_proteins`` y ``resolve_protein_sequences`` -- y un fallo aqui rompe las
dos, no una.

Session y HTTP simulados: sin base y sin red.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from sqlalchemy.orm import Session

import protea.infrastructure.orm.models  # noqa: F401
from protea.core.operations._protein_store import StoreCounts, store_records


def _noop_emit(event, message, fields, level):
    pass


def _make_mock_session():
    """Session mock that returns empty results for all DB queries."""
    session = MagicMock(spec=Session)
    session.query.return_value.filter.return_value.all.return_value = []
    session.query.return_value.filter.return_value.first.return_value = None
    return session


def _make_record(
    accession: str = "P12345",
    sequence: str = "MKTAYIAK",
    is_canonical: bool = True,
    isoform_index: int | None = None,
    canonical_accession: str | None = None,
):
    """Build a UniProtProteinRecord for store-records testing."""
    from protea_contracts import UniProtProteinRecord, compute_sequence_hash

    return UniProtProteinRecord(
        accession=accession,
        entry_name="TEST_HUMAN",
        canonical_accession=canonical_accession or accession,
        is_canonical=is_canonical,
        isoform_index=isoform_index,
        organism="Homo sapiens",
        taxonomy_id="9606",
        gene_name="TEST",
        reviewed=True,
        sequence=sequence,
        length=len(sequence),
        sequence_hash=compute_sequence_hash(sequence),
    )


class TestStoreRecords:
    def test_empty_records_returns_zeros(self):
        """Sin registros no se consulta nada: la salida temprana."""
        session = _make_mock_session()
        result = store_records(session, [], _noop_emit)
        assert result == StoreCounts(0, 0, 0, 0)
        session.query.assert_not_called()

    def test_updates_existing_protein(self):
        """Una fila que ya existe se parchea, no se pisa.

        Esto es lo que hace componibles a las dos operaciones del universo:
        ``extract_goa_universe`` escribe una fila con todo a NULL y esta funcion la
        completa despues sin tocar nada que ya tuviera valor.
        """
        record = _make_record()
        seq_hash = record.sequence_hash

        # Existing protein with missing fields (triggers updates)
        existing_prot = MagicMock()
        existing_prot.accession = "P12345"
        existing_prot.sequence_id = None  # will be updated
        existing_prot.entry_name = None  # will be updated
        existing_prot.canonical_accession = "OLD_ACC"  # will be updated
        existing_prot.is_canonical = False  # will be updated
        existing_prot.isoform_index = 2  # will be updated
        existing_prot.reviewed = None  # will be updated
        existing_prot.taxonomy_id = None  # will be updated
        existing_prot.organism = None  # will be updated
        existing_prot.gene_name = None  # will be updated
        existing_prot.length = None  # will be updated

        session = MagicMock(spec=Session)

        # _load_existing_sequences returns the hash → id map
        seq_query = MagicMock()
        seq_query.filter.return_value.all.return_value = [(seq_hash, 42)]

        # _load_existing_proteins returns the existing protein
        prot_query = MagicMock()
        prot_query.filter.return_value.all.return_value = [existing_prot]

        call_idx = {"n": 0}

        def query_side_effect(*args):
            call_idx["n"] += 1
            if call_idx["n"] == 1:
                return seq_query
            return prot_query

        session.query.side_effect = query_side_effect

        c = store_records(session, [record], _noop_emit)

        assert c.proteins_inserted == 0
        assert c.proteins_updated == 1  # existing protein was updated
        assert c.sequences_reused == 1  # sequence was reused from DB
        assert c.sequences_inserted == 0
        # Verify fields were updated
        assert existing_prot.sequence_id == 42
        assert existing_prot.entry_name == "TEST_HUMAN"
        assert existing_prot.canonical_accession == "P12345"
        assert existing_prot.is_canonical is True
        assert existing_prot.isoform_index is None
        assert existing_prot.reviewed is True

    def test_inserts_new_sequence_when_missing(self):
        """Un hash que la base no tiene entra como fila nueva de ``sequence``."""
        record = _make_record()

        session = MagicMock(spec=Session)

        # No existing sequences
        seq_query = MagicMock()
        seq_query.filter.return_value.all.return_value = []

        # No existing proteins
        prot_query = MagicMock()
        prot_query.filter.return_value.all.return_value = []

        call_idx = {"n": 0}

        def query_side_effect(*args):
            call_idx["n"] += 1
            if call_idx["n"] == 1:
                return seq_query
            return prot_query

        session.query.side_effect = query_side_effect

        c = store_records(session, [record], _noop_emit)

        assert c.proteins_inserted == 1
        assert c.proteins_updated == 0
        assert c.sequences_inserted == 1
        assert c.sequences_reused == 0
        # add_all called twice: once for sequences, once for proteins
        assert session.add_all.call_count == 2




class TestLaFilaSinSecuenciaSeCompletaDespues:
    """El invariante que hace posible construir el universo en dos pasos.

    ``extract_goa_universe`` escribe una fila con accesion, canonica y nada mas;
    ``resolve_protein_sequences`` la encuentra despues y la completa. Si el parcheo
    pisara valores, el orden de las dos operaciones importaria, y una tercera
    pasada de ``insert_proteins`` sobre la misma accesion desharia una de las dos.
    """

    def _fila_de_extraccion(self, accession="P12345"):
        """Lo mismo que escribe ``ExtractGoaUniverseOperation._accession_row``."""
        from protea.infrastructure.orm.models.protein.protein import Protein

        canonical, is_canonical, isoform = Protein.parse_isoform(accession)
        return Protein(
            accession=accession,
            canonical_accession=canonical,
            is_canonical=is_canonical,
            isoform_index=isoform,
            first_admitted_release=156,
        )

    def test_rellena_todo_lo_que_estaba_nulo(self):
        from protea.core.operations._protein_store import apply_record_updates

        fila = self._fila_de_extraccion()
        assert apply_record_updates(fila, _make_record(), seq_id=42) is True
        assert fila.sequence_id == 42
        assert fila.entry_name == "TEST_HUMAN"
        assert fila.organism == "Homo sapiens"
        assert fila.taxonomy_id == "9606"
        assert fila.gene_name == "TEST"
        assert fila.reviewed is True
        assert fila.length == 8

    def test_no_toca_la_release_que_la_admitio(self):
        """``first_admitted_release`` es lo unico que el GAF sabe y UniProt no.
        Un parcheo que lo pisara borraria la unica observacion de la serie."""
        from protea.core.operations._protein_store import apply_record_updates

        fila = self._fila_de_extraccion()
        apply_record_updates(fila, _make_record(), seq_id=42)
        assert fila.first_admitted_release == 156

    def test_un_segundo_parcheo_no_cambia_nada(self):
        from protea.core.operations._protein_store import apply_record_updates

        fila = self._fila_de_extraccion()
        apply_record_updates(fila, _make_record(), seq_id=42)
        assert apply_record_updates(fila, _make_record(), seq_id=99) is False
        assert fila.sequence_id == 42, "la secuencia ya estaba y no se repisa"
