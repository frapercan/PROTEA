"""Unit tests for the protea-sparse-knn LAFA container driver.

Every test here runs without torch, without the backbone and without the
frozen bank: the pieces exercised are the ones a wrong answer would ship
silently, namely the ontology walk, the vote, the propagation and the
output contract.
"""

from __future__ import annotations

import gzip
import json
from pathlib import Path

import numpy as np
import pytest

from apps.lafa_sparse_knn import sparse_driver as driver

# A three-level toy ontology. GO:0000003 is a molecular function whose
# parent chain reaches the root, so a vote on the leaf must surface every
# ancestor under the true path rule.
OBO = """format-version: 1.2
data-version: releases/2025-07-22

[Term]
id: GO:0000001
name: root function
namespace: molecular_function

[Term]
id: GO:0000002
name: middle function
namespace: molecular_function
is_a: GO:0000001 ! root function

[Term]
id: GO:0000003
name: leaf function
namespace: molecular_function
alt_id: GO:0000099
is_a: GO:0000002 ! middle function

[Term]
id: GO:0000010
name: a component
namespace: cellular_component
relationship: part_of GO:0000011 ! bigger component

[Term]
id: GO:0000011
name: bigger component
namespace: cellular_component

[Term]
id: GO:0000020
name: dead term
namespace: biological_process
is_obsolete: true
"""


@pytest.fixture
def ontology(tmp_path: Path) -> Path:
    path = tmp_path / "go-basic.obo"
    path.write_text(OBO)
    return path


def test_parse_obo_reads_parents_aspects_and_alt_ids(ontology: Path) -> None:
    parents, aspect, alt = driver.parse_obo(ontology)

    assert parents["GO:0000003"] == ["GO:0000002"]
    assert aspect["GO:0000003"] == "F"
    assert aspect["GO:0000010"] == "C"
    assert alt["GO:0000099"] == "GO:0000003"


def test_parse_obo_drops_obsolete_terms(ontology: Path) -> None:
    parents, _, _ = driver.parse_obo(ontology)

    assert "GO:0000020" not in parents


def test_part_of_counts_as_a_parent(ontology: Path) -> None:
    """A component that is part_of another must propagate into it.

    Dropping ``part_of`` and keeping only ``is_a`` is the classic way to
    under-propagate the cellular component aspect, which then reads as a
    weak method rather than a parsing bug.
    """
    parents, _, _ = driver.parse_obo(ontology)

    assert driver.ancestors_of("GO:0000010", parents, {}) == {"GO:0000010", "GO:0000011"}


def test_ancestors_include_the_term_and_the_whole_chain(ontology: Path) -> None:
    parents, _, _ = driver.parse_obo(ontology)

    assert driver.ancestors_of("GO:0000003", parents, {}) == {
        "GO:0000003",
        "GO:0000002",
        "GO:0000001",
    }


def test_read_fasta_keeps_file_order_and_takes_the_first_token(tmp_path: Path) -> None:
    path = tmp_path / "q.fasta"
    path.write_text(">Q1 some description here\nMKV\nLLA\n>Q2\nGGG\n")

    assert driver.read_fasta(path) == {"Q1": "MKVLLA", "Q2": "GGG"}


def test_transfer_scores_by_similarity_weighted_agreement(ontology: Path) -> None:
    """A term every neighbour carries must reach 1.0; a term one of two
    carries at equal similarity must reach 0.5."""
    parents, _, alt = driver.parse_obo(ontology)
    donors = {"D1": ["GO:0000003"], "D2": ["GO:0000002"]}
    neighbours = [[("D1", 1.0), ("D2", 1.0)]]

    scored = driver.transfer(["Q1"], neighbours, donors, parents, alt)

    # Both donors imply the root and the middle term, only D1 the leaf.
    assert scored["Q1"]["GO:0000001"] == pytest.approx(1.0)
    assert scored["Q1"]["GO:0000002"] == pytest.approx(1.0)
    assert scored["Q1"]["GO:0000003"] == pytest.approx(0.5)


def test_transfer_resolves_an_alternate_identifier(ontology: Path) -> None:
    """A donor annotated with a secondary id must still vote for the term.

    The bank carries whatever the annotation release used; if the
    container did not map alt ids it would drop those votes silently.
    """
    parents, _, alt = driver.parse_obo(ontology)
    scored = driver.transfer(
        ["Q1"], [[("D1", 1.0)]], {"D1": ["GO:0000099"]}, parents, alt
    )

    assert scored["Q1"]["GO:0000003"] == pytest.approx(1.0)


def test_transfer_weights_a_closer_neighbour_more(ontology: Path) -> None:
    parents, _, alt = driver.parse_obo(ontology)
    donors = {"D1": ["GO:0000003"], "D2": ["GO:0000002"]}

    scored = driver.transfer(["Q1"], [[("D1", 0.9), ("D2", 0.1)]], donors, parents, alt)

    assert scored["Q1"]["GO:0000003"] == pytest.approx(0.9)


def test_an_unannotated_neighbour_does_not_dilute_the_score(ontology: Path) -> None:
    """A neighbour with no GO terms must not lower the other neighbours' scores.

    About 3 percent of the bank has an embedding and no annotation. If
    those entered the denominator, a query whose retrieval happened to
    return three of them would see every score fall by a tenth, which is
    a property of the reference set and not of the evidence for the call.
    """
    parents, _, alt = driver.parse_obo(ontology)
    donors = {"D1": ["GO:0000003"], "D2": []}

    with_silent = driver.transfer(["Q1"], [[("D1", 1.0), ("D2", 1.0)]], donors, parents, alt)
    without = driver.transfer(["Q1"], [[("D1", 1.0)]], donors, parents, alt)

    assert with_silent["Q1"]["GO:0000003"] == pytest.approx(1.0)
    assert with_silent["Q1"] == without["Q1"]


def test_a_neighbour_absent_from_the_donor_table_is_also_silent(ontology: Path) -> None:
    """Absent from the donor file and present but empty must behave alike."""
    parents, _, alt = driver.parse_obo(ontology)

    scored = driver.transfer(
        ["Q1"], [[("D1", 1.0), ("UNKNOWN", 1.0)]], {"D1": ["GO:0000003"]}, parents, alt
    )

    assert scored["Q1"]["GO:0000003"] == pytest.approx(1.0)


def test_transfer_yields_nothing_when_no_neighbour_can_vote(ontology: Path) -> None:
    """Every neighbour unannotated is not the same as a weak call: it is no
    call at all, so the query gets an empty prediction rather than zeros."""
    parents, _, alt = driver.parse_obo(ontology)

    scored = driver.transfer(["Q1"], [[("D1", 0.9), ("D2", 0.8)]], {}, parents, alt)

    assert scored["Q1"] == {}


def test_transfer_yields_nothing_when_every_neighbour_is_orthogonal(ontology: Path) -> None:
    """Non-positive similarity carries no evidence, so the query gets no
    calls rather than a uniform prior."""
    parents, _, alt = driver.parse_obo(ontology)

    scored = driver.transfer(["Q1"], [[("D1", 0.0)]], {"D1": ["GO:0000003"]}, parents, alt)

    assert scored["Q1"] == {}


def test_scores_never_leave_the_unit_interval(ontology: Path) -> None:
    parents, _, alt = driver.parse_obo(ontology)
    donors = {f"D{i}": ["GO:0000003"] for i in range(10)}
    neighbours = [[(f"D{i}", 1.0) for i in range(10)]]

    scored = driver.transfer(["Q1"], neighbours, donors, parents, alt)

    assert all(0.0 <= v <= 1.0 for v in scored["Q1"].values())


def test_write_predictions_caps_each_aspect_independently(
    ontology: Path, tmp_path: Path
) -> None:
    """The cap is per aspect on purpose.

    A global cap lets a large biological process closure crowd out the
    molecular function calls, which shows up as a collapsed MF score and
    not as a truncation.
    """
    _, aspect, _ = driver.parse_obo(ontology)
    scored = {
        "Q1": {
            "GO:0000001": 0.9,
            "GO:0000002": 0.8,
            "GO:0000003": 0.7,
            "GO:0000010": 0.6,
        }
    }
    out = tmp_path / "predictions.tsv"

    written = driver.write_predictions(scored, aspect, ["Q1"], out, {"P": 2, "F": 2, "C": 2})

    lines = out.read_text().strip().split("\n")
    assert written == 3
    functions = [line for line in lines if line.split("\t")[1] in ("GO:0000001", "GO:0000002")]
    components = [line for line in lines if line.split("\t")[1] == "GO:0000010"]
    assert len(functions) == 2
    assert len(components) == 1


def test_output_is_the_three_column_contract(ontology: Path, tmp_path: Path) -> None:
    _, aspect, _ = driver.parse_obo(ontology)
    out = tmp_path / "predictions.tsv"

    driver.write_predictions({"Q1": {"GO:0000001": 0.5}}, aspect, ["Q1"], out, {"P": 500, "F": 500, "C": 500})

    assert out.read_text() == "Q1\tGO:0000001\t0.500\n"


def test_load_bank_rejects_a_misaligned_bank(tmp_path: Path) -> None:
    """The accession list and the code rows index each other positionally.

    If they ever disagree, every prediction is attributed to the wrong
    protein and nothing downstream would notice, so this must fail loudly.
    """
    (tmp_path / "BANK.json").write_text(json.dumps({"dict_dim": 16}))
    np.save(tmp_path / "codes_idx.npy", np.zeros((3, 2), dtype=np.uint16))
    np.save(tmp_path / "codes_val.npy", np.ones((3, 2), dtype=np.float16))
    np.save(tmp_path / "accessions.npy", np.array(["A", "B"]))

    with pytest.raises(ValueError, match="misaligned"):
        driver.load_bank(tmp_path)


def test_search_returns_the_nearest_first(tmp_path: Path) -> None:
    """End to end over the sparse retrieval, with a bank built by hand."""
    from scipy import sparse

    bank = sparse.csr_matrix(
        np.array([[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.7, 0.7, 0.0]], dtype=np.float32)
    )
    queries = sparse.csr_matrix(np.array([[1.0, 0.0, 0.0]], dtype=np.float32))

    hits = driver.search(queries, bank, ["A", "B", "C"], k=3)

    assert [accession for accession, _ in hits[0]] == ["A", "C", "B"]


def test_load_donors_groups_by_accession(tmp_path: Path) -> None:
    with gzip.open(tmp_path / "donors.tsv.gz", "wt") as handle:
        handle.write("A\tGO:0000001\nA\tGO:0000002\nB\tGO:0000003\n")

    donors = driver.load_donors(tmp_path)

    assert donors["A"] == ["GO:0000001", "GO:0000002"]
    assert donors["B"] == ["GO:0000003"]
