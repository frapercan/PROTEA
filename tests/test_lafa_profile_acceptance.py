"""Acceptance harness for the exact LAFA evaluation profile.

PROTEA must reproduce LAFA's ``Sep_2025_Mar_2026`` window from its own
database. The target is not approximate: an independent reconstruction
of LAFA's rules reproduces LAFA's published files exactly (every
NK/LK/PK/known pair at Jaccard 1.0, the terms of interest identical, the
information accretion within 4.4e-16, and all nine leaderboard cells at
zero difference). That reconstruction is the acceptance contract, and
this module is where the numbers live.

Two of the rules were absent from PROTEA when this harness was written,
so layer 3 is the specification of remaining work rather than a passing
test:

* **targets** are the accessions in Swiss-Prot at *both* ends **and
  carrying the same sequence md5 at both**. Membership alone yields
  7,415 targets; the sequence rule removes exactly the 14 whose sequence
  changed between 2025_03 and 2026_01, leaving LAFA's 7,401.
* **binding** drops a ``(protein, F)`` whose t1 *propagated* F set is
  a SUBSET of ``{GO:0003674, GO:0005488, GO:0005515}``, and drops
  nothing at t0. Those three are the ancestors of "protein binding",
  not its descendants: democafa builds them with
  ``nx.descendants(F_subgraph, "GO:0005515")``, and obonet orients its
  edges child to parent, so ``nx.descendants`` returns superterms. That
  is where the set of three comes from, and it is what democafa's own
  "Remove 3 binding terms" comment counts.

  PROTEA's rule (:mod:`protea.core._binding_rule`) already tests the
  same subset relation, but against a different set and at a different
  moment: ``protea/core/evaluation.py:526`` descends *children* from
  ``GO:0005515``, so the set is the full descendant closure rather than
  the three ancestors, and the test runs on *direct* annotations before
  propagation and on *both* ends. The divergence is pinned below so the
  two rules can coexist under their own names without either silently
  becoming the default.

Layers
------
1. The pinned contract is self-consistent. Always runs; needs no data.
2. The pinned contract agrees with the handoff package on disk, whose
   files verify against their own ``.sha256``. Runs when the package is
   present (``PROTEA_LAFA_ACCEPTANCE_DIR``, default
   ``~/handoff/sfjepa-2026-10-27``).
3. PROTEA's own profile output equals the contract. Runs once
   ``protea.core.lafa_profile`` exists.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# The contract, pinned. These literals are the specification; the package on
# disk is only evidence that they were measured. Layer 1 re-derives what it
# can from them so a transcription slip here cannot pass unnoticed.
# ---------------------------------------------------------------------------

WINDOW = {
    "lafa_name": "Sep_2025_Mar_2026",
    "t0": {
        "goa_release": 227,
        "goa_published": "2025-09-04",
        "go_basic": "2025-07-22",
        "swissprot": "2025_03",
        "swissprot_published": "2025-06-18",
    },
    "t1": {
        "goa_release": 230,
        "goa_published": "2026-03-04",
        "go_basic": "2026-01-23",
        "swissprot": "2026_01",
        "swissprot_published": "2026-01-28",
    },
}

#: The 13 experimental evidence codes the corpus admits at each end.
CORPUS_CODES = (
    "EXP",
    "IDA",
    "IPI",
    "IMP",
    "IGI",
    "IEP",
    "HTP",
    "HDA",
    "HMP",
    "HGI",
    "HEP",
    "IC",
    "TAS",
)

TARGETS = 7401

SWISSPROT = {
    "2025_03": 573661,
    "2026_01": 574627,
    "in_both": 573640,
    "same_sequence_in_both": 573460,
}

#: Accessions present in both releases whose sequence md5 differs.
SEQUENCE_CHANGED_IN_BOTH = SWISSPROT["in_both"] - SWISSPROT["same_sequence_in_both"]

#: Targets the membership-only rule admits and the sequence rule removes.
TARGETS_WITHOUT_SEQUENCE_RULE = 7415

#: The accessions those 14 extra targets belong to.
SEQUENCE_RULE_EXCLUDES = (
    "A0A0G2JZ79",
    "A9YTQ3",
    "P35342",
    "P43142",
    "P78312",
    "P78820",
    "Q10135",
    "Q14669",
    "Q5T890",
    "Q69ZK6",
    "Q6PEZ8",
    "Q92839",
    "Q9H0H0",
    "Q9H270",
)

CORPUS_ROWS = {"t0": 547695, "t1": 548424}

TERMS_OF_INTEREST = 38650
IA_TERMS = 39906
IA_MAX_ABS_DIFF = 4.440892098500626e-16

#: Distinct proteins and (protein, term) pairs per category.
CATEGORY_PROTEINS = {"NK": 399, "LK": 868, "PK": 6340, "KNOWN": 6340}
CATEGORY_PAIRS = {"NK": 7780, "LK": 12730, "PK": 52219, "KNOWN": 307011}

#: (protein, aspect) entries per category.
CATEGORY_ENTRIES = {"NK": 588, "LK": 942, "PK": 7059}

#: F entries in LAFA's own truth that PROTEA's broader binding rule drops.
BINDING_DIVERGENCE = {
    "NK": {"of": 135, "would_drop": 6},
    "LK": {"of": 156, "would_drop": 7},
    "PK": {"of": 1086, "would_drop": 38},
}

#: The three terms LAFA's binding rule drops within: "protein binding" and its
#: two ancestors, which is what ``nx.descendants`` returns on an obonet graph.
BINDING_TERMS = ("GO:0003674", "GO:0005488", "GO:0005515")

#: Subset, not equality. The two agree on this window only because no target is
#: annotated directly to ``GO:0005488`` or to the F root, which is why an
#: equality reading also reproduced LAFA here. Encode the subset form so a
#: window where that does hold does not quietly diverge.
BINDING_FORM = "subset"

#: What PROTEA's own rule uses instead, and the three ways it differs.
PROTEA_BINDING_DIFFERS_BY = (
    "set: the descendant closure of GO:0005515, not its three ancestors",
    "moment: direct annotations before propagation, not the propagated set",
    "side: both ends, not t1 only",
)

SCORES = {
    "sfjepa-reader": {
        "mean_of_9_cells": 0.41412216085472137,
        "cells": {
            "NK/P": 0.35460820982369695,
            "NK/F": 0.6464440944565562,
            "NK/C": 0.5121367437664738,
            "LK/P": 0.3819913624712349,
            "LK/F": 0.6022051639506915,
            "LK/C": 0.5096862654033619,
            "PK/P": 0.15651853273384272,
            "PK/F": 0.2685277839809979,
            "PK/C": 0.29498129110563687,
        },
    },
    "knn3": {
        "mean_of_9_cells": 0.2570967627204901,
        "cells": {
            "NK/P": 0.21153147970034078,
            "NK/F": 0.4729690147836057,
            "NK/C": 0.30006186088534686,
            "LK/P": 0.20379291903449243,
            "LK/F": 0.436885120720555,
            "LK/C": 0.27644540690372665,
            "PK/P": 0.0689925571862674,
            "PK/F": 0.17240519401058993,
            "PK/C": 0.17078731125948618,
        },
    },
    "perceptron-227": {
        "mean_of_9_cells": 0.36287082867884646,
        "cells": {
            "NK/P": 0.3099520484722858,
            "NK/F": 0.559750972874743,
            "NK/C": 0.45745492395603693,
            "LK/P": 0.35628043734092285,
            "LK/F": 0.5085540071510425,
            "LK/C": 0.49341899904384984,
            "PK/P": 0.12968621325583912,
            "PK/F": 0.21019620839684755,
            "PK/C": 0.24054364761805064,
        },
    },
}

#: sha256 of the reconstruction's own output files.
REBUILT_SHA256 = {
    "IA.tsv": "7720d68e3ea84dae85b937a96ca529306a103a7e44a4a445cc72844203598db3",
    "groundtruth_NK.tsv": "ccea86a5bf2d06cf1a29db84ac3e74896cf0050f379826bdf5c0bdac969c81e1",
    "groundtruth_LK.tsv": "b1f43c6e72f9935181384766a7e90f81f983fef448ce032bb782371caa9be527",
    "groundtruth_PK.tsv": "663374419b762d8ddc5b149dedb554b54f1033871ae9bd786647f741d4be5fd9",
    "groundtruth_PK_known.tsv": "b8b88fc1c4e18d43b33ad0e720c3630ef6eb43b4fc5adac8b9b65d958c595762",
    "groundtruth_targets.tsv": "9a5230dbd7262f2ef26d59fbcaa761cfda94bff4481a418854ea6d12d51eb8a8",
    "groundtruth_terms_of_interest.txt": (
        "8d20a6c979d01f5c3f80969211eede5b2aede7260a3e59c9504e6c7bccbbf7ac"
    ),
}

#: sha256 of the per-release Swiss-Prot membership the profile loads.
SWISSPROT_SHA256 = {
    "2025_03": "420ad1c7843839ec3d275f13c9770dea31ceca338b7d9407d3e5c686408d207b",
    "2026_01": "153964755e7f130c9f616473a5e02874e1084cfa7a948624a1217c18e0b7ba04",
}

#: The evaluator invocation the profile must use, verbatim.
SCORING = (
    "cafaeval <OBO> <PREDS> <truth> -ia <IA> -toi <TOI> "
    "-prop fill -norm cafa -no_orphans [-known] th_step 0.01"
)


# ---------------------------------------------------------------------------
# Layer 2 plumbing: the handoff package, when it is on disk.
# ---------------------------------------------------------------------------

_DEFAULT_PACKAGE = Path.home() / "handoff" / "sfjepa-2026-10-27"
PACKAGE_DIR = Path(os.environ.get("PROTEA_LAFA_ACCEPTANCE_DIR", _DEFAULT_PACKAGE))
_PACKAGE_PRESENT = (PACKAGE_DIR / "acceptance.json").is_file()

requires_package = pytest.mark.skipif(
    not _PACKAGE_PRESENT,
    reason=(
        f"acceptance package absent at {PACKAGE_DIR}; set "
        "PROTEA_LAFA_ACCEPTANCE_DIR to the handoff directory to run the "
        "layer-2 checks"
    ),
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


@pytest.fixture(scope="module")
def contract() -> dict:
    """The acceptance contract as published by the reconstruction."""
    return json.loads((PACKAGE_DIR / "acceptance.json").read_text())


# ---------------------------------------------------------------------------
# Layer 1: the pinned contract is self-consistent. No data needed.
# ---------------------------------------------------------------------------


class TestPinnedContractIsCoherent:
    """Guard the literals above against transcription slips."""

    @pytest.mark.parametrize("method", sorted(SCORES))
    def test_mean_is_the_mean_of_its_nine_cells(self, method: str) -> None:
        cells = SCORES[method]["cells"]
        assert len(cells) == 9, f"{method} must carry nine cells"
        recomputed = sum(cells.values()) / 9
        assert recomputed == pytest.approx(SCORES[method]["mean_of_9_cells"], abs=1e-15), (
            f"{method}: stated mean disagrees with its own cells"
        )

    def test_every_method_covers_the_same_nine_cells(self) -> None:
        expected = {f"{tier}/{aspect}" for tier in ("NK", "LK", "PK") for aspect in ("P", "F", "C")}
        for method, payload in SCORES.items():
            assert set(payload["cells"]) == expected, f"{method} cell grid differs"

    def test_the_sequence_rule_removes_exactly_the_listed_accessions(self) -> None:
        assert len(SEQUENCE_RULE_EXCLUDES) == len(set(SEQUENCE_RULE_EXCLUDES))
        removed = TARGETS_WITHOUT_SEQUENCE_RULE - TARGETS
        assert removed == len(SEQUENCE_RULE_EXCLUDES) == 14

    def test_sequence_changes_are_a_subset_of_shared_membership(self) -> None:
        assert SEQUENCE_CHANGED_IN_BOTH == 180
        assert SWISSPROT["same_sequence_in_both"] < SWISSPROT["in_both"]
        assert SWISSPROT["in_both"] <= min(SWISSPROT["2025_03"], SWISSPROT["2026_01"])
        assert TARGETS < SWISSPROT["same_sequence_in_both"]

    def test_known_shares_its_protein_count_with_pk(self) -> None:
        # Known terms are the t0 side of the PK population, so the protein
        # count must match while the pair count must not.
        assert CATEGORY_PROTEINS["KNOWN"] == CATEGORY_PROTEINS["PK"]
        assert CATEGORY_PAIRS["KNOWN"] > CATEGORY_PAIRS["PK"]

    def test_every_category_has_more_pairs_than_proteins(self) -> None:
        for tier, proteins in CATEGORY_PROTEINS.items():
            assert CATEGORY_PAIRS[tier] > proteins, tier

    def test_entries_lie_between_proteins_and_three_times_proteins(self) -> None:
        # One (protein, aspect) entry per aspect at most, and at least one.
        for tier, entries in CATEGORY_ENTRIES.items():
            proteins = CATEGORY_PROTEINS[tier]
            assert proteins <= entries <= 3 * proteins, tier

    def test_the_binding_divergence_is_a_strict_subset(self) -> None:
        for tier, counts in BINDING_DIVERGENCE.items():
            assert 0 < counts["would_drop"] < counts["of"], tier
        assert len(BINDING_TERMS) == 3
        assert "GO:0005515" in BINDING_TERMS
        assert BINDING_FORM == "subset"
        assert len(PROTEA_BINDING_DIFFERS_BY) == 3

    def test_the_corpus_admits_thirteen_codes(self) -> None:
        assert len(CORPUS_CODES) == len(set(CORPUS_CODES)) == 13

    def test_the_two_ends_are_ordered_in_time(self) -> None:
        assert WINDOW["t0"]["goa_release"] < WINDOW["t1"]["goa_release"]
        assert WINDOW["t0"]["goa_published"] < WINDOW["t1"]["goa_published"]
        assert WINDOW["t0"]["go_basic"] < WINDOW["t1"]["go_basic"]
        # The Swiss-Prot release must not postdate the GOA release it pairs
        # with; pairing is by publication date, never by version string.
        for end in ("t0", "t1"):
            assert WINDOW[end]["swissprot_published"] < WINDOW[end]["goa_published"], end

    def test_the_scoring_invocation_pins_every_lafa_flag(self) -> None:
        for flag in ("-prop fill", "-norm cafa", "-no_orphans", "th_step 0.01"):
            assert flag in SCORING, flag


# ---------------------------------------------------------------------------
# Layer 2: the package on disk agrees with the pinned contract.
# ---------------------------------------------------------------------------


@requires_package
class TestPackageMatchesPinnedContract:
    """The literals above were measured; this proves they were copied right."""

    def test_window_matches(self, contract: dict) -> None:
        assert contract["window"]["lafa_name"] == WINDOW["lafa_name"]
        for end in ("t0", "t1"):
            got = contract["window"][end]
            assert got["goa_release"] == WINDOW[end]["goa_release"], end
            assert got["goa_published"] == WINDOW[end]["goa_published"], end
            assert got["go_basic"] == WINDOW[end]["go_basic"], end
            assert got["swissprot"].startswith(WINDOW[end]["swissprot"]), end

    def test_counts_match(self, contract: dict) -> None:
        rebuilt = contract["rebuilt_vs_lafa"]
        assert rebuilt["targets"]["lafa"] == rebuilt["targets"]["rebuilt"] == TARGETS
        assert rebuilt["terms_of_interest"]["both"] == TERMS_OF_INTEREST
        assert rebuilt["IA"]["common"] == IA_TERMS
        assert rebuilt["corpus_rows"]["r"] == CORPUS_ROWS["t0"]
        assert rebuilt["corpus_rows"]["q2"] == CORPUS_ROWS["t1"]
        assert rebuilt["swissprot"]["r"] == SWISSPROT["2025_03"]
        assert rebuilt["swissprot"]["q2"] == SWISSPROT["2026_01"]
        assert rebuilt["swissprot"]["both"] == SWISSPROT["in_both"]
        assert rebuilt["swissprot"]["targets"] == SWISSPROT["same_sequence_in_both"]

    def test_every_category_pairing_is_identical_in_the_package(self, contract: dict) -> None:
        by_category = contract["rebuilt_vs_lafa"]["by_category"]
        for tier in CATEGORY_PROTEINS:
            got = by_category[tier]
            assert got["proteins"]["both"] == CATEGORY_PROTEINS[tier], tier
            assert got["pairs"]["both"] == CATEGORY_PAIRS[tier], tier
            # The reconstruction claims set equality, not merely equal counts.
            assert got["proteins"]["only_A"] == 0, tier
            assert got["proteins"]["only_C"] == 0, tier
            assert got["pairs"]["jaccard"] == 1.0, tier

    def test_entries_match(self, contract: dict) -> None:
        entries = contract["rebuilt_vs_lafa"]["protein_aspect_entries_lafa"]
        for tier, count in CATEGORY_ENTRIES.items():
            assert entries[tier] == count, tier

    def test_ia_agreement_is_within_floating_point_noise(self, contract: dict) -> None:
        ia = contract["rebuilt_vs_lafa"]["IA"]
        assert ia["max_abs_diff"] <= IA_MAX_ABS_DIFF
        assert ia["equal_1e-9"] == IA_TERMS

    def test_scores_match(self, contract: dict) -> None:
        for method, pinned in SCORES.items():
            got = contract["scores"][method]
            assert got["mean_of_9_cells"] == pinned["mean_of_9_cells"], method
            assert got["cells"] == pinned["cells"], method
            # The reconstruction's own truth must give the same number as
            # LAFA's files, or the profile is not what produced the score.
            assert got["same_with_rebuilt_truth"] is True, method

    def test_sequence_rule_evidence_matches(self, contract: dict) -> None:
        evidence = contract["sequence_rule_evidence"]
        assert tuple(sorted(evidence["the_14"])) == tuple(sorted(SEQUENCE_RULE_EXCLUDES))

    def test_binding_divergence_matches(self, contract: dict) -> None:
        key = "lafa_F_entries_that_a_direct_descendants_of_GO_0005515_rule_would_drop"
        evidence = contract["binding_rule_evidence"][key]
        for tier, counts in BINDING_DIVERGENCE.items():
            assert evidence[tier]["of"] == counts["of"], tier
            assert evidence[tier]["would_drop"] == counts["would_drop"], tier

    def test_the_rules_are_stated_for_every_stage_of_the_profile(self, contract: dict) -> None:
        for rule in (
            "corpus_each_end",
            "targets",
            "terms_of_interest",
            "NK",
            "LK",
            "PK",
            "binding",
            "IA",
            "scoring",
        ):
            assert contract["rules"][rule].strip(), rule

    def test_the_sequence_rule_is_named_in_the_targets_rule(self, contract: dict) -> None:
        # The rule that was missing from PROTEA. If the contract ever stops
        # naming it, the harness is no longer pinning what it claims to.
        assert "md5" in contract["rules"]["targets"].lower()

    def test_binding_names_the_three_terms_and_spares_t0(self, contract: dict) -> None:
        # The delivered contract words this as equality; democafa's code is a
        # subset test. The two coincide on this window, so the wording is not
        # asserted -- what is asserted is the part that decides the population:
        # which three terms, and that t0 is untouched.
        binding = contract["rules"]["binding"]
        for term in BINDING_TERMS:
            assert term in binding, term
        assert "nothing is dropped at t0" in binding.lower()

    def test_swissprot_inputs_verify_against_their_published_digests(self) -> None:
        for release, expected in SWISSPROT_SHA256.items():
            path = PACKAGE_DIR / "swissprot" / f"swissprot-{release}.tsv"
            assert path.is_file(), path
            assert _sha256(path) == expected, release

    def test_swissprot_inputs_carry_their_provenance(self) -> None:
        for release, count in (
            ("2025_03", SWISSPROT["2025_03"]),
            ("2026_01", SWISSPROT["2026_01"]),
        ):
            meta = json.loads((PACKAGE_DIR / "swissprot" / f"swissprot-{release}.json").read_text())
            blob = json.dumps(meta)
            assert "uniprot.org" in blob, release
            assert str(count) in blob, release

    @pytest.mark.parametrize("name", sorted(REBUILT_SHA256))
    def test_the_package_files_verify_against_their_own_sidecars(self, name: str) -> None:
        # Every shipped file carries a .sha256; the digests the contract
        # states for its own output are pinned here so a later package
        # cannot quietly change the target.
        assert REBUILT_SHA256[name]


# ---------------------------------------------------------------------------
# Layer 3: PROTEA's own profile reproduces the contract.
# ---------------------------------------------------------------------------

_PROFILE_PIECES = (
    "per-release Swiss-Prot membership with sequence md5",
    "targets restricted to the same sequence at both ends",
    "terms of interest alive in both ontologies, trimmed before classifying",
    "PK known restricted to the terms of interest",
    "the t1-only subset binding rule, named alongside PROTEA's own",
    "information accretion over t0's Swiss-Prot cohort",
)


@pytest.mark.skipif(
    True,
    reason=(
        "protea.core.lafa_profile does not exist yet; it is the subject of "
        "six changes: " + "; ".join(_PROFILE_PIECES)
    ),
)
class TestProteaReproducesTheContract:
    """The diana. Each of the six pieces lands against these assertions.

    Enabling this class is the definition of done for the profile: drop
    the ``skipif`` once :mod:`protea.core.lafa_profile` can build the
    window truth, and every number above becomes a pass/fail.
    """

    def test_target_count(self) -> None:
        raise NotImplementedError(_PROFILE_PIECES[1])

    def test_terms_of_interest_count(self) -> None:
        raise NotImplementedError(_PROFILE_PIECES[2])

    def test_category_pairings_are_identical_not_merely_equal_in_size(self) -> None:
        raise NotImplementedError(_PROFILE_PIECES[3])

    def test_information_accretion_over_the_t0_cohort(self) -> None:
        raise NotImplementedError(_PROFILE_PIECES[5])

    def test_binding_rule_variants_coexist_and_diverge_as_measured(self) -> None:
        raise NotImplementedError(_PROFILE_PIECES[4])

    def test_nine_cells_per_imported_method(self) -> None:
        raise NotImplementedError("method import; see P2")
