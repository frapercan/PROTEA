"""Acceptance harness for the long validation window, 211 -> 227.

This is the sibling of :mod:`tests.test_lafa_profile_acceptance` and not a
duplicate of it. That one covers LAFA's official window (227 -> 230) under
LAFA's rules: Swiss-Prot only, same sequence at both ends, the three-term
binding rule, t1 alone. This one covers the lab's long window under the lab's
own frame (decision 42): every protein, PROTEA's binding rule applied per side
over the direct annotations, the ontology of 211 throughout. The two profiles
are different on purpose -- the long one has samples in nearly every stratum
and is what selects a method, the LAFA one is the official check -- and their
figures are not comparable with each other.

WHAT THIS PROVES. Given the lab's ground truth, its information accretion, its
terms of interest, its excluded-known base and its three frozen prediction
dumps, PROTEA's evaluator reproduces the lab's nine published cells
**exactly**: twenty of twenty-seven at a difference of 0.0 and the remaining
seven at one double rounding unit, 1.1e-16, with all three nine-cell means at
0.0. Two independent implementations of the same scoring profile agreeing to
the bit is the strongest statement available about either, and it is what
licenses reading a platform figure beside a lab figure at all.

THE THIRD ARM IS THE ONE THAT MATTERS MOST AND ARRIVED LAST. The deployable
recipe (eight readers, no residue similarity, headline 0.454665) was read
after the package was closed, so it ships as a supplement that reuses the
package's frame without touching a byte of it. Before it had a dump, the
recipe's headline was the one published figure with no evidence behind it on
this side, and it is precisely the figure the lab's single executable has to
reproduce. The older recipe with residue similarity, 0.454030, still has no
dump, so nothing here claims to reproduce it.

WHY THE ROUNDING IS LOAD-BEARING. The predictions must reach cafaeval at four
decimals. That is not a tolerance granted to make the test pass: it is what
PROTEA's own production path does, in ``_run_cafa_artifacts._write_scored_base``,
which formats every row as ``{score:.4f}``, and the comment there records why
-- two code paths that rounded differently put 9 of 396 cells exactly 0.0001
apart. The lab's wrapper does the same with ``float_format="%.4f"``. Feeding
the dumps' full nine decimals instead moves the cells by up to 1.6e-04 and the
means by 2.4e-05, which still rounds to the published 0.2930, 0.4531 and
0.4547 but is not identity. The dumps themselves carry nine significant
digits, so the rounding is the CONSUMER's job and the fixtures below do it. The drift was measured, not assumed, and these were ruled out as
its cause first: threshold tie-breaking (the argmax lands on the same tau in
all nine cells and the value differs AT that tau), the ontology (term census
and the is_a/part_of edge vocabulary both match), namespace assignment (one
disagreement in 893,481 rows, no alt_id used of the 3,590 present) and the
export's own precision (zero rows within 1e-9 of any threshold, so no row can
cross one).

WHY THE ONTOLOGY IS PINNED BY DIGEST AND NOT FETCHED. The pivot snapshot of
this window, ``releases/2022-07-01``, carries no ``obo_uri``: nothing has
archived it, so every evaluation re-downloads it and no stored digest fixes
the bytes. Until ``archive_ontology_snapshot`` has run on it, layer 3 takes
the file by path and verifies it against the digest pinned below, so a test
that passes has provably scored under the same ontology as the measurement it
reproduces.

Layers
------
1. The pinned contract is self-consistent. Always runs; needs no data.
2. The pinned contract agrees with the handoff package on disk, whose files
   verify against their own ``.sha256``. Runs when the package is present
   (``PROTEA_LONG_WINDOW_DIR``, default
   ``~/handoff/sfjepa-2026-10-27-validacion-larga``). The supplement has its
   own directory (``PROTEA_LONG_WINDOW_ENS8_DIR``, default the package path
   with ``-ens8``); when it is absent only its own arm skips.
3. PROTEA's evaluator reproduces the cells from those dumps. Runs when the
   package is present and ``PROTEA_LONG_WINDOW_OBO`` points at the 2022-07-01
   ``go-basic.obo``. Opt-in because it is minutes of work over 11.5 million
   prediction rows, and because the ontology is a 31 MB file a test must never
   fetch for itself.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# The contract, pinned. These literals are the specification; the package on
# disk is only evidence that they were measured.
# ---------------------------------------------------------------------------

WINDOW = {
    "r": 211,
    "q2": 227,
    "pivot": "releases/2022-07-01",
    "pivot_snapshot_id": "15f002ea-a6f1-4ab2-918a-832f79b082c0",
    "target_snapshot_id": "28c785ca-bbb0-460f-8bf9-a7334d608017",
    "dt_days": 1084,
    "decision": "42 (2026-10-17)",
}

#: The lab's terms of interest: non-obsolete in BOTH snapshots.
#:
#: Reproduced term for term from PROTEA's own tables, which is what settles
#: that the rule is this and not the intersection of presence. The three
#: counts are the whole argument: presence in both is 47,272 terms, while
#: non-obsolescence in both is 39,028, and the 8,244 between them are the
#: question. The two single-snapshot counts are pinned because each is also
#: the term count of that end's information accretion table -- 43,558 is the
#: 211 IA and 39,906 is LAFA's -- so one measurement ties three artifacts.
TERMS = {
    "in_211": 47_328,
    "in_227": 48_165,
    "present_in_both": 47_272,
    "live_in_211": 43_558,
    "live_in_227": 39_906,
    "live_in_both": 39_028,
    "obsolete_in_211": 3_770,
}

COUNTS = {
    "nk_proteins": 18_268,
    "nk_pairs": 391_694,
    "lk_proteins": 4_667,
    "lk_pairs": 63_572,
    "pk_proteins": 19_713,
    "pk_pairs": 203_679,
    "pk_known_pairs": 914_760,
    "known_proteins": 133_457,
    "known_pairs": 5_379_274,
    "delta_proteins": 40_531,
    "toi_terms": 39_028,
}

#: What ``cafaeval`` reads with ``-known``: everything known in 211, in all
#: aspects, for the proteins with any gain. Three different quantities answer
#: to "known" in this window and conflating them is the easiest mistake here,
#: so all three are pinned: this file, the corpus-wide ``known_pairs`` above,
#: and ``pk_known_pairs``, which counts only PK proteins in their PK aspects.
KNOWN_FILE = {"rows": 1_431_672, "proteins": 22_263}

#: The ontology the measurement was made under. See the module docstring on
#: why this is a pinned digest rather than a fetch.
OBO_SHA256 = "91eae63f06163aea5072d1d7c8d34a1f0cdf6295b23b527297bce4ac89dd022a"
OBO_DATA_VERSION = "releases/2022-07-01"

#: cafaeval's flags, named once. The lab's profile and PROTEA's call must agree
#: on every one of them or the cells are not about the same quantity.
SCORING = {
    "prop": "fill",
    "norm": "cafa",
    "no_orphans": True,
    "th_step": 0.01,
    "score_decimals": 4,
    "known_on": "PK",
}

#: The three arms with a frozen dump, and the nine cells each.
#: ``mean_of_9_cells`` is the headline figure; it is a flat mean, so the test
#: below re-derives it rather than trusting the pair.
#:
#: ``home`` says which directory ships the dump. Two arms come from the
#: package; ``ens_ts44_noresim8`` was read at 05:31 on 2026-10-27, after the
#: package closed at 04:13, so it ships as a separate supplement that reuses
#: the package's frame untouched. It is not folded into the package precisely
#: because the package's digests are pinned here: a supplement must not be
#: able to change a byte of what was already accepted.
#:
#: ``fixture_sha256`` is the dump itself. Layer 2 checks it even when layer 3
#: is not run, because the sidecar sweep only proves that a file agrees with
#: the digest shipped NEXT TO IT, and a replaced pair of files agrees with
#: itself.
ARMS: dict[str, dict] = {
    "knn3": {
        "home": "package",
        "fixture": "predictions/knn3.tsv",
        "fixture_sha256": "7cffd1b51652baef6c81a7de34e53899fd800507e3cc43544a86ec67878423f0",
        "mean_of_9_cells": 0.29299219982554986,
        "cells": {
            "NK/P": 0.3497460880313072,
            "NK/F": 0.5187366274953679,
            "NK/C": 0.3686570335051187,
            "LK/P": 0.2177738244766015,
            "LK/F": 0.40781504676983527,
            "LK/C": 0.31541501029024643,
            "PK/P": 0.08420965915024813,
            "PK/F": 0.16857836359135525,
            "PK/C": 0.20599814511986833,
        },
    },
    "ens_ts43_noresim": {
        "home": "package",
        "fixture": "predictions/deployable-reader-ens4.tsv",
        "fixture_sha256": "72a8a64e8ff2506e6fdd9ba07bf0cdac96737239ea00293d18f7443db1a2b988",
        "mean_of_9_cells": 0.4530671354437508,
        "cells": {
            "NK/P": 0.5948937078229356,
            "NK/F": 0.7101187342629114,
            "NK/C": 0.5441545819763537,
            "LK/P": 0.3951830484798382,
            "LK/F": 0.5792352435247633,
            "LK/C": 0.4932321049835799,
            "PK/P": 0.18696777028659417,
            "PK/F": 0.25960165158745113,
            "PK/C": 0.31421737606932965,
        },
    },
    # The deployable recipe: the average of eight readers with no residue
    # similarity, four of holes and four of history. This is the headline of
    # the clean 211-cut recipe and the figure the lab's single executable has
    # to reproduce, so until it had a dump it was the one published number
    # with no evidence behind it on this side.
    "ens_ts44_noresim8": {
        "home": "supplement",
        "fixture": "predictions/deployable-recipe-ens8.tsv",
        "fixture_sha256": "66b7a67cb8047c94e6d1b1dc27acb882110972991aec5fa498dd0a60816922e2",
        "mean_of_9_cells": 0.45466520460860976,
        "cells": {
            "NK/P": 0.5938682536333848,
            "NK/F": 0.7121032678632635,
            "NK/C": 0.5491363606810096,
            "LK/P": 0.39744018144383075,
            "LK/F": 0.5789627503181802,
            "LK/C": 0.49360488310285666,
            "PK/P": 0.18795394323597708,
            "PK/F": 0.2629708102008154,
            "PK/C": 0.3159463909981702,
        },
    },
}

#: The arms that read, as opposed to the neighbour-transfer baseline.
READER_ARMS = ("ens_ts43_noresim", "ens_ts44_noresim8")

#: Agreement at four decimals is identity, not approximation. One double
#: rounding unit is the only slack: five of the eighteen cells land there and
#: the other thirteen at zero.
CELL_TOLERANCE = 2e-16

#: What the same comparison costs when the dumps keep their nine decimals.
#: Pinned so the module records the measurement that justifies the rounding
#: rather than only the rule.
FULL_PRECISION_DRIFT = {"max_cell": 1.56e-04, "max_mean": 2.4e-05}

SETTINGS = ("NK", "LK", "PK")
ASPECTS = ("P", "F", "C")
_NS_LETTER = {
    "biological_process": "P",
    "molecular_function": "F",
    "cellular_component": "C",
}

# ---------------------------------------------------------------------------
# Layer 2 and 3 plumbing: the handoff package and the ontology, when present.
# ---------------------------------------------------------------------------

_DEFAULT_PACKAGE = Path.home() / "handoff" / "sfjepa-2026-10-27-validacion-larga"
PACKAGE_DIR = Path(os.environ.get("PROTEA_LONG_WINDOW_DIR", _DEFAULT_PACKAGE))
_PACKAGE_PRESENT = (PACKAGE_DIR / "acceptance.json").is_file()

_DEFAULT_SUPPLEMENT = Path(f"{_DEFAULT_PACKAGE}-ens8")
SUPPLEMENT_DIR = Path(os.environ.get("PROTEA_LONG_WINDOW_ENS8_DIR", _DEFAULT_SUPPLEMENT))

#: Where each ``home`` lives, and which file carries that home's claim. The
#: supplement states one arm at the top level; the package nests its arms
#: under ``arms``, so the record is read through :func:`_published_record`
#: rather than indexed the same way in both.
HOMES = {
    "package": (PACKAGE_DIR, "acceptance.json"),
    "supplement": (SUPPLEMENT_DIR, "acceptance-ens8.json"),
}


def _acceptance_path(home: str) -> Path:
    directory, filename = HOMES[home]
    return directory / filename


def _arm_dir(arm: str) -> Path:
    return HOMES[ARMS[arm]["home"]][0]


def _published_record(arm: str) -> dict:
    """What the arm's own home publishes about it."""
    home = ARMS[arm]["home"]
    document = json.loads(_acceptance_path(home).read_text())
    if home == "package":
        return document["arms"][arm]
    assert document["arm"] == arm, (document["arm"], arm)
    return document


#: Arms whose home is on this disk. An absent supplement skips its own arm
#: and leaves the package's two running, which is the behaviour a machine
#: that only has the original package needs.
PRESENT_ARMS = tuple(arm for arm in sorted(ARMS) if _acceptance_path(ARMS[arm]["home"]).is_file())


def _arm_params() -> list:
    """Every arm, each skipped individually when its home is absent."""
    return [
        pytest.param(
            arm,
            marks=pytest.mark.skipif(
                arm not in PRESENT_ARMS,
                reason=f"{ARMS[arm]['home']} of arm {arm} absent at {_arm_dir(arm)}",
            ),
        )
        for arm in sorted(ARMS)
    ]

_OBO_ENV = os.environ.get("PROTEA_LONG_WINDOW_OBO", "")
OBO_PATH = Path(_OBO_ENV) if _OBO_ENV else None
_OBO_PRESENT = OBO_PATH is not None and OBO_PATH.is_file()

requires_package = pytest.mark.skipif(
    not _PACKAGE_PRESENT,
    reason=(
        f"long-window package absent at {PACKAGE_DIR}; set "
        "PROTEA_LONG_WINDOW_DIR to the handoff directory to run the layer-2 "
        "checks"
    ),
)

requires_scoring = pytest.mark.skipif(
    not (_PACKAGE_PRESENT and _OBO_PRESENT),
    reason=(
        "layer 3 needs both the handoff package and the 2022-07-01 "
        "go-basic.obo; set PROTEA_LONG_WINDOW_DIR and PROTEA_LONG_WINDOW_OBO "
        "to run it (minutes of work over 6.2M prediction rows)"
    ),
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def _line_count(path: Path) -> int:
    with path.open("rb") as handle:
        return sum(block.count(b"\n") for block in iter(lambda: handle.read(1 << 20), b""))


@pytest.fixture(scope="module")
def contract() -> dict:
    """The acceptance contract as published by the lab's reconstruction."""
    return json.loads((PACKAGE_DIR / "acceptance.json").read_text())


# ---------------------------------------------------------------------------
# Layer 1: the pinned contract is self-consistent. No data needed.
# ---------------------------------------------------------------------------


class TestTheContractHoldsTogether:
    @pytest.mark.parametrize("arm", sorted(ARMS))
    def test_the_headline_is_the_flat_mean_of_its_nine_cells(self, arm: str) -> None:
        # Exactly the flat mean, not approximately: both published headlines
        # reproduce from their own nine cells at a difference of 0.0, so a
        # loose tolerance here would let a mistyped cell through.
        cells = ARMS[arm]["cells"]
        assert len(cells) == 9
        assert sum(cells.values()) / 9 == pytest.approx(
            ARMS[arm]["mean_of_9_cells"], abs=CELL_TOLERANCE
        )

    @pytest.mark.parametrize("arm", sorted(ARMS))
    def test_every_arm_covers_the_same_nine_cells(self, arm: str) -> None:
        expected = {f"{s}/{a}" for s in SETTINGS for a in ASPECTS}
        assert set(ARMS[arm]["cells"]) == expected

    @pytest.mark.parametrize("arm", READER_ARMS)
    def test_the_reader_beats_the_neighbour_transfer_in_every_cell(self, arm: str) -> None:
        # The claim the window exists to support. If it held only on average
        # the mean would be hiding a cell where the method is worse. It holds
        # for both reader arms, in all nine.
        knn, reader = ARMS["knn3"]["cells"], ARMS[arm]["cells"]
        assert all(reader[c] > knn[c] for c in knn), {
            c: (knn[c], reader[c]) for c in knn if reader[c] <= knn[c]
        }

    def test_the_recipe_of_eight_leads_on_the_mean_and_not_in_every_cell(self) -> None:
        # Between the two reader arms the improvement is NOT uniform: eight
        # readers lead in seven cells and lose NK/P and LK/F. So the ordering
        # of the two headlines is a statement about the mean of nine, and
        # writing it as "eight readers beat four" without that qualifier
        # would be claiming something the cells do not say. Pinned with the
        # two exceptions named, so a future arm that happens to win
        # everywhere fails here and gets its claim upgraded on purpose.
        four, eight = ARMS["ens_ts43_noresim"], ARMS["ens_ts44_noresim8"]
        assert eight["mean_of_9_cells"] > four["mean_of_9_cells"]
        worse = sorted(c for c, v in eight["cells"].items() if v <= four["cells"][c])
        assert worse == ["LK/F", "NK/P"], worse

    def test_terms_of_interest_are_the_live_intersection_and_not_the_present_one(
        self,
    ) -> None:
        # The distinction the whole frame turns on, and the one that was got
        # wrong first: presence in both is a wider set than life in both.
        assert TERMS["live_in_both"] == COUNTS["toi_terms"]
        assert TERMS["live_in_both"] < TERMS["present_in_both"]
        assert TERMS["present_in_both"] < TERMS["in_211"]
        assert TERMS["live_in_both"] <= min(TERMS["live_in_211"], TERMS["live_in_227"])

    def test_the_obsolete_count_reconciles_the_two_counts_of_211(self) -> None:
        assert TERMS["in_211"] - TERMS["obsolete_in_211"] == TERMS["live_in_211"]

    def test_the_three_known_quantities_are_ordered_and_distinct(self) -> None:
        # pk_known counts PK proteins in their PK aspects; the file cafaeval
        # reads covers every aspect of every protein with a gain; and the
        # corpus-wide figure covers every protein with anything at t0.
        assert COUNTS["pk_known_pairs"] < KNOWN_FILE["rows"] < COUNTS["known_pairs"]
        assert KNOWN_FILE["proteins"] < COUNTS["delta_proteins"]
        assert COUNTS["delta_proteins"] < COUNTS["known_proteins"]

    def test_every_category_has_more_pairs_than_proteins(self) -> None:
        for s in ("nk", "lk", "pk"):
            assert COUNTS[f"{s}_pairs"] > COUNTS[f"{s}_proteins"], s

    def test_the_scoring_profile_pins_every_flag_that_moves_a_cell(self) -> None:
        assert SCORING["prop"] == "fill"
        assert SCORING["norm"] == "cafa"
        assert SCORING["no_orphans"] is True
        assert SCORING["th_step"] == 0.01
        assert SCORING["known_on"] == "PK"

    def test_the_rounding_rule_is_the_production_path_and_not_a_tolerance(self) -> None:
        # Four decimals is what PROTEA writes. Read from the writer rather
        # than restated, so the day the format changes this fails instead of
        # going quietly stale.
        from protea.core.operations import _run_cafa_artifacts

        source = Path(_run_cafa_artifacts.__file__).read_text()
        assert f":.{SCORING['score_decimals']}f" in source, (
            "the production writer no longer formats scores to "
            f"{SCORING['score_decimals']} decimals, so layer 3 is reproducing "
            "something the platform does not do"
        )

    def test_identity_is_asserted_and_the_looser_agreement_is_only_recorded(
        self,
    ) -> None:
        # The tolerance must stay at rounding noise. The nine-decimal drift is
        # pinned as a measurement, and it is orders of magnitude larger, which
        # is the reason the rounding is part of the profile.
        assert CELL_TOLERANCE <= 2e-16
        assert FULL_PRECISION_DRIFT["max_cell"] > CELL_TOLERANCE * 1e10


# ---------------------------------------------------------------------------
# Layer 2: the contract agrees with the package on disk.
# ---------------------------------------------------------------------------


@requires_package
class TestThePackageSaysWhatTheContractSays:
    def test_the_window_matches(self, contract: dict) -> None:
        got = json.loads((PACKAGE_DIR / "frame" / "manifest.json").read_text())
        assert got["r"] == WINDOW["r"]
        assert got["q2"] == WINDOW["q2"]
        assert got["snapshot_id"] == WINDOW["pivot_snapshot_id"]
        assert got["snapshot_id_q2"] == WINDOW["target_snapshot_id"]
        assert got["dt_days"] == WINDOW["dt_days"]
        assert got["decision"] == WINDOW["decision"]

    def test_the_counts_match(self, contract: dict) -> None:
        for key, value in COUNTS.items():
            assert contract["counts"][key] == value, key

    def test_the_terms_of_interest_rule_is_stated_and_counted(self, contract: dict) -> None:
        manifest = json.loads((PACKAGE_DIR / "frame" / "manifest.json").read_text())
        rule = manifest["terms_of_interest"]
        assert rule["terms"] == TERMS["live_in_both"]
        assert "live" in rule["rule"], rule["rule"]

    @pytest.mark.parametrize("arm", _arm_params())
    def test_the_cells_match(self, arm: str) -> None:
        pinned = ARMS[arm]
        published = _published_record(arm)
        assert published["fixture"] == pinned["fixture"], arm
        assert published["mean_of_9_cells"] == pytest.approx(
            pinned["mean_of_9_cells"], abs=5e-16
        ), arm
        for cell, value in pinned["cells"].items():
            assert published["cells"][cell] == pytest.approx(value, abs=5e-15), (arm, cell)

    @pytest.mark.parametrize("setting", SETTINGS)
    def test_each_ground_truth_file_has_the_pairs_the_contract_claims(self, setting: str) -> None:
        path = PACKAGE_DIR / "frame" / f"ground-truth-{setting}.tsv"
        assert _line_count(path) == COUNTS[f"{setting.lower()}_pairs"]

    def test_the_excluded_known_file_is_the_one_cafaeval_reads(self) -> None:
        path = PACKAGE_DIR / "frame" / "known-terms.tsv"
        assert _line_count(path) == KNOWN_FILE["rows"]
        proteins = {line.split("\t", 1)[0] for line in path.read_text().splitlines()}
        assert len(proteins) == KNOWN_FILE["proteins"]

    def test_the_terms_of_interest_file_holds_the_live_intersection(self) -> None:
        path = PACKAGE_DIR / "frame" / "toi-window.txt"
        assert _line_count(path) == TERMS["live_in_both"]

    def test_the_information_accretion_covers_the_live_terms_of_the_pivot(self) -> None:
        # Not the terms of interest: IA is a property of one snapshot's
        # corpus, so it spans every term alive in 211 and the TOI trims later.
        path = PACKAGE_DIR / "frame" / "ia-211.tsv"
        assert _line_count(path) == TERMS["live_in_211"]

    @pytest.mark.parametrize("arm", _arm_params())
    def test_each_frozen_dump_is_the_file_the_cells_were_measured_from(self, arm: str) -> None:
        # The sidecar sweep below cannot say this: it compares a file with
        # the digest shipped beside it, so a dump replaced together with its
        # sidecar passes. The cells are pinned against THIS file.
        path = _arm_dir(arm) / ARMS[arm]["fixture"]
        assert _sha256(path) == ARMS[arm]["fixture_sha256"], path.name

    @pytest.mark.parametrize("arm", _arm_params())
    def test_each_frozen_dump_covers_every_protein_with_a_gain(self, arm: str) -> None:
        path = _arm_dir(arm) / ARMS[arm]["fixture"]
        proteins = set()
        with path.open() as handle:
            next(handle)
            for line in handle:
                proteins.add(line.split("\t", 1)[0])
        assert len(proteins) == COUNTS["delta_proteins"]

    @pytest.mark.parametrize(
        ("home", "minimum"),
        [
            pytest.param("package", 12, id="package"),
            pytest.param(
                "supplement",
                2,
                id="supplement",
                marks=pytest.mark.skipif(
                    not _acceptance_path("supplement").is_file(),
                    reason=f"supplement absent at {SUPPLEMENT_DIR}",
                ),
            ),
        ],
    )
    def test_every_shipped_file_verifies_against_its_own_sidecar(
        self, home: str, minimum: int
    ) -> None:
        checked = 0
        for sidecar in sorted(HOMES[home][0].rglob("*.sha256")):
            target = sidecar.with_suffix("")
            if not target.is_file():
                continue
            expected = sidecar.read_text().split()[0]
            assert _sha256(target) == expected, target.name
            checked += 1
        assert checked >= minimum, f"only {checked} files in {home} carried a sidecar"


# ---------------------------------------------------------------------------
# Layer 3: PROTEA's evaluator reproduces the cells.
# ---------------------------------------------------------------------------


@requires_scoring
class TestProteaReproducesTheCells:
    @pytest.fixture(scope="class")
    def ontology(self) -> Path:
        assert OBO_PATH is not None
        assert _sha256(OBO_PATH) == OBO_SHA256, (
            f"{OBO_PATH} is not the ontology this window was measured under; "
            f"expected sha256 {OBO_SHA256}"
        )
        header = OBO_PATH.read_text(errors="replace")[:4096]
        assert f"data-version: {OBO_DATA_VERSION}" in header
        return OBO_PATH

    @pytest.fixture(scope="class")
    def predictions_at_four_decimals(self, tmp_path_factory: pytest.TempPathFactory) -> Path:
        """The dumps as the production writer would have written them.

        cafaeval reads a directory and scores every file in it as its own
        method, so both arms go in one directory and one pass covers both.
        """
        out = tmp_path_factory.mktemp("predictions")
        for arm in PRESENT_ARMS:
            source = _arm_dir(arm) / ARMS[arm]["fixture"]
            target = out / Path(ARMS[arm]["fixture"]).name
            with source.open() as handle, target.open("w") as sink:
                next(handle)
                for line in handle:
                    protein, go_id, _aspect, score = line.rstrip("\n").split("\t")
                    sink.write(f"{protein}\t{go_id}\t{float(score):.4f}\n")
        return out

    @pytest.fixture(scope="class")
    def measured(self, ontology: Path, predictions_at_four_decimals: Path) -> dict:
        """``{fixture: {"NK/P": f_micro_w, ...}}`` from PROTEA's evaluator.

        Called exactly as ``_run_cafa_eval_driver`` calls it, including the
        per-protein sink, which is what keeps cafaeval on its sparse path.
        """
        from cafaeval.evaluation import PerProteinSink, cafa_eval

        frame = PACKAGE_DIR / "frame"
        out: dict[str, dict[str, float]] = {}
        for setting in SETTINGS:
            _df, best = cafa_eval(
                str(ontology),
                str(predictions_at_four_decimals),
                str(frame / f"ground-truth-{setting}.tsv"),
                ia=str(frame / "ia-211.tsv"),
                exclude=(
                    str(frame / "known-terms.tsv") if setting == SCORING["known_on"] else None
                ),
                prop=SCORING["prop"],
                norm=SCORING["norm"],
                no_orphans=SCORING["no_orphans"],
                toi_file=str(frame / "toi-window.txt"),
                th_step=SCORING["th_step"],
                n_cpu=1,
                per_protein_sink=PerProteinSink(),
            )
            for _, row in best["f_micro_w"].reset_index().iterrows():
                letter = _NS_LETTER.get(str(row["ns"]))
                if letter is None:
                    continue
                out.setdefault(str(row["filename"]), {})[f"{setting}/{letter}"] = float(
                    row["f_micro_w"]
                )
        return out

    def test_every_present_arm_was_scored(self, measured: dict) -> None:
        assert set(measured) == {Path(ARMS[arm]["fixture"]).name for arm in PRESENT_ARMS}

    @pytest.mark.parametrize("arm", _arm_params())
    def test_every_cell_reproduces(self, measured: dict, arm: str) -> None:
        name = Path(ARMS[arm]["fixture"]).name
        got = measured[name]
        assert set(got) == set(ARMS[arm]["cells"])
        off = {
            cell: (want, got[cell])
            for cell, want in ARMS[arm]["cells"].items()
            if abs(got[cell] - want) > CELL_TOLERANCE
        }
        assert not off, off

    @pytest.mark.parametrize("arm", _arm_params())
    def test_the_headline_reproduces(self, measured: dict, arm: str) -> None:
        name = Path(ARMS[arm]["fixture"]).name
        mean = sum(measured[name].values()) / 9
        assert abs(mean - ARMS[arm]["mean_of_9_cells"]) <= CELL_TOLERANCE

    def test_most_cells_land_on_the_bit_and_none_merely_rounds(self, measured: dict) -> None:
        # The distinction worth keeping: agreement here is identity, not
        # agreement to the four decimals the figures are published at. If a
        # future change turned this into rounding agreement the test above
        # would still pass at 1e-4, so the strength is asserted separately.
        exact = 0
        for arm in PRESENT_ARMS:
            pinned = ARMS[arm]
            got = measured[Path(pinned["fixture"]).name]
            for cell, want in pinned["cells"].items():
                diff = abs(got[cell] - want)
                assert diff <= CELL_TOLERANCE, (arm, cell, diff)
                exact += diff == 0.0
        cells = 9 * len(PRESENT_ARMS)
        assert exact >= cells // 2, f"only {exact} of {cells} cells landed exactly on 0.0"
