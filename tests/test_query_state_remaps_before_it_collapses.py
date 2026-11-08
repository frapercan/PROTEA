"""``query_state``: the order of the two steps, and the two counts they produce.

WHY THE ORDER IS NOT INTERCHANGEABLE. Both steps remove rows. A card is read in
the id space of its OWN snapshot, where ``go_term.id`` is a surrogate per
snapshot; it has to be rewritten into the declared axis BEFORE equal cards are
collapsed. Collapsing first leaves two rows for one term and reports the wrong
number for both steps, and the rows it leaves are ones the loader joins to
nothing.

WHY THE TWO COLLAPSES ARE COUNTED APART. One number for both would print the
same digit for a bundle whose GAF duplication changed and a bundle whose
synonym map changed. They are different facts about the data.

WHAT THESE TESTS WOULD HAVE CAUGHT. The first version of this module collapsed
nothing at all and read each snapshot's own ``go_id``, which the laboratory
measured at 20 to 22 per cent duplicate rows and 3,84 per cent of rows naming a
term the axis does not have.
"""

from __future__ import annotations

import uuid

from protea.core._truth_remap import Remap, SideFrame
from protea.core.transitions_export import _query_state as Q

VIVO = "GO:0000001"
OTRO = "GO:0000002"
ALIAS = "GO:0000900"
FUERA = "GO:0009999"
CRUZA = "GO:0000800"


class _Eset:
    def __init__(self) -> None:
        self.id = uuid.uuid4()
        self.old_annotation_set_id = "aset-t"
        self.new_annotation_set_id = "aset-t-prime"


class _Session:
    """Answers ``_STATE`` with the rows given per annotation set."""

    def __init__(self, por_aset: dict[str, list[tuple[str, str, str, str]]]) -> None:
        self._por_aset = por_aset

    def execute(self, _sql, params):
        return list(self._por_aset.get(params["aset"], []))


def _facts() -> Q.WindowFacts:
    eset = _Eset()
    return Q.WindowFacts(
        eset=eset,
        window=str(eset.id),
        data=None,
        version_t="205",
        version_t_prime="211",
        horizon_days=311,
        old_end_snapshot=None,
        new_end_snapshot=None,
    )


def _axis() -> SideFrame:
    """An axis of two live terms, with ``ALIAS`` and ``CRUZA`` resolving onto it."""
    return SideFrame(
        snapshot_id=uuid.uuid4(),
        live={VIVO, OTRO},
        remap=Remap(
            by_alias={ALIAS: (VIVO, "alt_id"), CRUZA: (OTRO, "replaced_by")},
            alt_of={ALIAS: VIVO},
            replaced_by={CRUZA: {OTRO}},
            crossed_aspect={CRUZA},
        ),
    )


# --------------------------------------------- el remapeo va ANTES del DISTINCT


def test_two_surrogates_of_one_term_become_one_card():
    """The property the wrong order makes unexpressible.

    ``VIVO`` and ``ALIAS`` are two ids for one term. They arrive as two
    different cards and have to leave as ONE, counted as a remap collapse and
    NOT as a GAF one. Collapsing before the rewrite would leave both.
    """
    filas = [("P1", VIVO, "IDA", ""), ("P1", ALIAS, "IDA", "")]
    tabla, st = Q.state_of(_Session({"aset-t": filas}), _facts(), ["P1"], _axis())

    assert tabla.num_rows == 1
    assert tabla.column("go_id").to_pylist() == [VIVO]
    assert st.collapse.remap == 1
    assert st.collapse.gaf == 0


def test_the_gaf_s_own_repeat_is_counted_as_the_gaf_s():
    """The same card written twice in the file, before anything is rewritten."""
    filas = [("P1", VIVO, "IDA", ""), ("P1", VIVO, "IDA", "")]
    _, st = Q.state_of(_Session({"aset-t": filas}), _facts(), ["P1"], _axis())

    assert st.collapse.gaf == 1
    assert st.collapse.remap == 0
    assert st.rows == 1


def test_a_term_the_axis_cannot_name_is_dropped_and_counted():
    """Not written, and visible in the event rather than inferred from a total."""
    filas = [("P1", VIVO, "IDA", ""), ("P1", FUERA, "IEA", "")]
    tabla, st = Q.state_of(_Session({"aset-t": filas}), _facts(), ["P1"], _axis())

    assert tabla.column("go_id").to_pylist() == [VIVO]
    assert st.discards.off_axis == 1
    assert st.rows == 1


def test_crossing_an_aspect_is_counted_and_NOT_dropped():
    """The ontology named that successor, so the substitution happens anyway."""
    filas = [("P1", CRUZA, "IDA", "")]
    tabla, st = Q.state_of(_Session({"aset-t": filas}), _facts(), ["P1"], _axis())

    assert tabla.column("go_id").to_pylist() == [OTRO]
    assert st.discards.crossed_aspect == 1
    assert st.discards.off_axis == 0


def test_a_card_with_no_evidence_code_is_dropped_and_counted():
    """The branch the live data does not take, which is where they survive.

    ``protein_go_annotation.evidence_code`` is nullable in the schema, so a
    card with no evidence class is representable; the top eight codes of a
    sample of release 211 are all real ones, so nothing exercises it today.
    Written as a counted drop rather than as an empty string, because an empty
    evidence class is not a class and the laboratory's loader would drop it as
    unknown with only its own counter to show it.
    """
    filas = [("P1", VIVO, "IDA", ""), ("P1", OTRO, None, "")]
    tabla, st = Q.state_of(_Session({"aset-t": filas}), _facts(), ["P1"], _axis())

    assert tabla.column("go_id").to_pylist() == [VIVO]
    assert st.discards.no_evidence == 1
    assert "" not in st.evidence["t"]


# ------------------------------------------------------ la ficha, y los momentos


def test_the_same_card_at_both_moments_is_two_rows():
    """The moment is part of the card, which is what makes the state a state."""
    filas = [("P1", VIVO, "IDA", "")]
    tabla, st = Q.state_of(
        _Session({"aset-t": filas, "aset-t-prime": filas}), _facts(), ["P1"], _axis()
    )

    assert tabla.num_rows == 2
    assert sorted(tabla.column("moment").to_pylist()) == ["t", "t'"]
    assert st.by_moment == {"t": 1, "t'": 1}
    assert st.collapse.gaf == 0


def test_the_query_never_reads_the_source_of_an_annotation():
    """``assigned_by`` is not in the card, and the query does not even select it.

    Asserted on the SQL because that is where it would come back: production's
    scorer never reads the source, and inside the key it multiplies the cards
    by 1,177, measured over a sixteenth of release 211, which is exactly the
    overlap that makes the laboratory's loader reject a whole table.
    """
    assert "assigned_by" not in str(Q._STATE)


def test_the_evidence_histogram_is_per_moment():
    """Per moment and not over the bundle, so a narrowed end is visible."""
    tabla, st = Q.state_of(
        _Session(
            {
                "aset-t": [("P1", VIVO, "IDA", ""), ("P1", OTRO, "IEA", "")],
                "aset-t-prime": [("P1", VIVO, "IDA", "")],
            }
        ),
        _facts(),
        ["P1"],
        _axis(),
    )

    assert st.evidence["t"] == {"IDA": 1, "IEA": 1}
    assert st.evidence["t'"] == {"IDA": 1}
    assert tabla.num_rows == 3


def test_a_negation_is_contains_NOT_and_the_qualifier_goes_through_raw():
    """The qualifier is a RELATION and the regime changes at release 202."""
    filas = [("P1", VIVO, "IDA", "NOT|enables"), ("P1", OTRO, "IDA", "involved_in")]
    tabla, st = Q.state_of(_Session({"aset-t": filas}), _facts(), ["P1"], _axis())

    assert sorted(tabla.column("qualifier").to_pylist()) == ["NOT|enables", "involved_in"]
    assert st.negated == 1
