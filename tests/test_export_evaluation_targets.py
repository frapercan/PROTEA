"""Writing the target file that makes the grid and the submission one measurement.

The defect this closes is not a crash. ``protea_predict`` requires
``--query_file``; the internal payload leaves the query population optional and
selects the whole corpus when it is omitted. A run that forgets to name its
targets therefore produces a complete, plausible, differently-populated result.

So the tests that matter here are about population and about bytes: that the
targets are the proteins which GAINED annotation and not the ones that lost it,
that the same evaluation set always produces the same file, and that a target
whose sequence is absent is reported rather than quietly dropped behind an
authoritative-looking sha256.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import pytest

from protea.core.operations.export_evaluation_targets import (
    SCORED_CATEGORIES,
    WRAP,
    ExportEvaluationTargetsOperation,
    ExportEvaluationTargetsPayload,
    delta_accessions,
    format_fasta,
)


@dataclass
class _Data:
    """Stands in for EvaluationData; only the category maps are read."""

    nk: dict[str, set[str]] = field(default_factory=dict)
    lk: dict[str, set[str]] = field(default_factory=dict)
    pk: dict[str, set[str]] = field(default_factory=dict)
    removed: dict[str, set[str]] = field(default_factory=dict)


# --------------------------------------------------------------------------- population

def test_the_targets_are_the_union_of_the_scored_categories():
    data = _Data(nk={"P1": {"GO:1"}}, lk={"P2": {"GO:2"}}, pk={"P3": {"GO:3"}})

    assert delta_accessions(data, list(SCORED_CATEGORIES)) == ["P1", "P2", "P3"]


def test_a_protein_in_two_categories_appears_once():
    data = _Data(nk={"P1": {"GO:1"}}, lk={"P1": {"GO:2"}})

    assert delta_accessions(data, ["nk", "lk"]) == ["P1"]


def test_proteins_that_only_lost_annotation_are_never_targets():
    """The regression. ``removed`` is reported by the evaluation and never scored,
    so a target there is a query no metric can reward."""
    data = _Data(nk={"P1": {"GO:1"}}, removed={"P9": {"GO:9"}})

    assert delta_accessions(data, list(SCORED_CATEGORIES)) == ["P1"]


def test_removed_cannot_be_requested_as_a_category():
    """It would be a plausible request that silently changes the population."""
    with pytest.raises(ValueError, match="removed|unknown categories"):
        ExportEvaluationTargetsPayload(evaluation_set_id="e", categories=["removed"])


def test_an_empty_category_list_is_refused():
    with pytest.raises(ValueError, match="cannot be empty"):
        ExportEvaluationTargetsPayload(evaluation_set_id="e", categories=[])


def test_the_default_is_all_three_scored_categories():
    p = ExportEvaluationTargetsPayload(evaluation_set_id="e")

    assert p.categories == list(SCORED_CATEGORIES)


def test_a_single_category_can_be_taken_alone():
    """Useful for a no-knowledge-only run, which is a real thing we measure."""
    data = _Data(nk={"P1": set()}, lk={"P2": set()})

    assert delta_accessions(data, ["nk"]) == ["P1"]


# --------------------------------------------------------------------------- bytes

def test_the_order_is_sorted_rather_than_map_order():
    """A dict's order reflects how the delta happened to be built, so the same
    evaluation set would otherwise produce different bytes on a rebuild."""
    data = _Data(nk={"P9": set(), "P1": set(), "P5": set()})

    assert delta_accessions(data, ["nk"]) == ["P1", "P5", "P9"]


def test_the_header_is_a_bare_accession():
    """LAFA's parser takes the token before the first whitespace, and the field
    between the first two pipes only if they are there. A bare accession
    round-trips, so no convention has to be agreed twice."""
    got = format_fasta([("P12345", "MKV")]).decode()

    assert got.startswith(">P12345\n")


def test_sequences_are_wrapped_at_a_fixed_width():
    got = format_fasta([("P1", "A" * (WRAP + 5))]).decode().splitlines()

    assert got[1] == "A" * WRAP
    assert got[2] == "A" * 5


def test_a_sequence_shorter_than_the_wrap_is_one_line():
    got = format_fasta([("P1", "MKV")]).decode().splitlines()

    assert got == [">P1", "MKV"]


def test_the_same_records_always_produce_the_same_bytes():
    """The sha256 is the proof that two consumers scored one population, so the
    bytes have to be a function of the records and nothing else."""
    records = [("P1", "MKV"), ("P2", "AAA")]

    assert format_fasta(records) == format_fasta(list(records))


def test_no_records_produce_no_bytes_rather_than_a_stray_newline():
    assert format_fasta([]) == b""


def test_the_file_ends_with_a_newline():
    """Concatenation and line-counting downstream both assume it."""
    assert format_fasta([("P1", "MKV")]).endswith(b"\n")


# --------------------------------------------------------------------------- the operation

def test_the_operation_declares_that_it_writes_no_rows():
    op = ExportEvaluationTargetsOperation()

    assert op.name == "export_evaluation_targets"
    assert "writes no rows" in op.description.lower()


def test_the_description_says_which_proteins_are_excluded():
    """A reader must not have to guess whether losses are targets."""
    op = ExportEvaluationTargetsOperation()

    assert "lost" in op.description


def test_summarize_names_the_evaluation_set():
    op = ExportEvaluationTargetsOperation()

    assert "eval-1" in op.summarize_payload({"evaluation_set_id": "eval-1"})


def test_it_is_registered_so_it_can_be_dispatched():
    from protea.core.operation_catalog import build_operation_registry

    registry = build_operation_registry()

    assert registry.get("export_evaluation_targets").name == "export_evaluation_targets"
