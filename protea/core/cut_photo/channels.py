"""The evidence channel of a GOA row (ADR-D59, decision 2).

A channel is a pure function of four facts about a row: its evidence code, its
reference (a ``GO_REF`` or a publication), who assigned it and whether the
protein is reviewed at the cut. The partition is enumerated: a code or an
electronic reference this module does not list raises :class:`UnknownEvidence`,
which rejects the photo instead of guessing (ADR-D59: "an unknown one is counted
and rejects the photo rather than being guessed").

Which channels the recipe reads is decided at query time
(:data:`RECIPE_CHANNELS`), so changing that decision changes a filter, not the
data. The UniRef50 profiles count only :data:`PROFILE_CHANNELS`.

THE ELECTRONIC REFERENCES WERE CHECKED AGAINST GO's OWN LIST. Each ``GO_REF`` of
an ``IEA`` row is classified from its title in the Gene Ontology's
``metadata/gorefs.yaml`` (go-site, read on 2026-11-16), the titles kept beside
each entry so a reader can see what was classified. The list covers the
references GOA 211 carries and the obsolete ones older cuts carry (keywords and
subcellular location were split into manual and automatic references before
they merged). A cut that brings a reference not listed here fails its photo,
and the entry is added after reading its title, never by default.
"""

from __future__ import annotations

from typing import Final, Literal

from protea.core.cut_photo.formats import AUTOMATIC_CODE, NO_DATA_CODE, TRUTH_CODES

Channel = Literal[
    "truth",
    "curated",
    "phylogenetic",
    "orthology_electronic",
    "reviewed_field_electronic",
    "signature_electronic",
    "no_data",
]

#: Every channel, in the order ADR-D59 lists them.
CHANNELS: Final[tuple[Channel, ...]] = (
    "truth",
    "curated",
    "phylogenetic",
    "orthology_electronic",
    "reviewed_field_electronic",
    "signature_electronic",
    "no_data",
)

#: The channels the recipe reads (the author, 2026-11-15). ``signature_electronic``
#: is stored and never read by the recipe: Pfam and InterPro are a mirror only.
RECIPE_CHANNELS: Final[frozenset[Channel]] = frozenset(
    {"truth", "curated", "phylogenetic", "orthology_electronic", "reviewed_field_electronic"}
)

#: The channels a UniRef50 profile counts (ADR-D59, decision 1). Neighbours are
#: mostly unreviewed, so a field translation on them is a rule's output, which is
#: ``signature_electronic``; the profile therefore leaves the field channel out.
PROFILE_CHANNELS: Final[tuple[Channel, ...]] = (
    "truth",
    "curated",
    "phylogenetic",
    "orthology_electronic",
)

#: Curated inference that is not phylogenetic. ``IKR`` and ``IRD`` admit a protein
#: through ADR-D49's curated tier but are PAINT's, so their channel is phylogenetic.
CURATED_CODES: Final[frozenset[str]] = frozenset({"ISS", "ISO", "ISA", "ISM", "IGC", "RCA", "NAS"})
PHYLOGENETIC_CODES: Final[frozenset[str]] = frozenset({"IBA", "IBD", "IKR", "IRD"})

#: How an electronic reference is read: ``orthology`` and ``signature`` name their
#: channel; ``field`` is a translation of a field of the entry, which is the
#: curator's on a reviewed entry and a rule's on an unreviewed one.
ElectronicKind = Literal["orthology", "field", "signature"]

#: Every ``GO_REF`` an ``IEA`` row may cite, with its kind and its title in GO's
#: list, word for word.
ELECTRONIC_REFERENCES: Final[dict[str, tuple[ElectronicKind, str]]] = {
    "GO_REF:0000107": (
        "orthology",
        "Automatic transfer of experimentally verified manual GO annotation "
        "data to orthologs using Ensembl Compara",
    ),
    "GO_REF:0000019": (
        "orthology",
        "OBSOLETE Automatic transfer of experimentally verified manual GO "
        "annotation data to orthologs using Ensembl Compara",
    ),
    "GO_REF:0000043": (
        "field",
        "Gene Ontology annotation based on UniProtKB/Swiss-Prot keyword mapping",
    ),
    "GO_REF:0000044": (
        "field",
        "Gene Ontology annotation based on UniProtKB/Swiss-Prot Subcellular "
        "Location vocabulary mapping, accompanied by conservative changes to "
        "GO terms applied by UniProt",
    ),
    "GO_REF:0000003": ("field", "Gene Ontology annotation based on Enzyme Commission mapping"),
    "GO_REF:0000041": ("field", "Gene Ontology annotation based on UniPathway vocabulary mapping"),
    "GO_REF:0000116": ("field", "Automatic Gene Ontology annotation based on Rhea mapping"),
    "GO_REF:0000037": (
        "field",
        "OBSOLETE Gene Ontology annotation based on manual assignment of "
        "UniProtKB keywords in UniProtKB/Swiss-Prot entries",
    ),
    "GO_REF:0000039": (
        "field",
        "OBSOLETE Gene Ontology annotation based on the manual assignment of "
        "UniProtKB Subcellular Location terms in UniProtKB/Swiss-Prot entries",
    ),
    "GO_REF:0000002": (
        "signature",
        "Gene Ontology annotation through association of InterPro records with GO terms",
    ),
    "GO_REF:0000104": (
        "signature",
        "Electronic Gene Ontology annotations created by transferring manual "
        "GO annotations between related proteins based on shared sequence "
        "features",
    ),
    "GO_REF:0000117": (
        "signature",
        "Electronic Gene Ontology annotations created by ARBA machine learning models",
    ),
    # HAMAP's family rules for microbial proteomes: a rule over a family, so a
    # signature, although the title speaks of orthologous proteins.
    "GO_REF:0000020": (
        "signature",
        "OBSOLETE Electronic Gene Ontology annotations created by "
        "transferring manual GO annotations between orthologous microbial "
        "proteins",
    ),
    "GO_REF:0000038": (
        "signature",
        "OBSOLETE Gene Ontology annotation based on automatic assignment of "
        "UniProtKB keywords in UniProtKB/TrEMBL entries",
    ),
    "GO_REF:0000040": (
        "signature",
        "OBSOLETE Gene Ontology annotation based on the automatic assignment "
        "of UniProtKB Subcellular Location terms in UniProtKB/TrEMBL entries",
    ),
    "GO_REF:0000108": (
        "signature",
        "Automatic assignment of GO terms using logical inference, based on "
        "on inter-ontology links",
    ),
    "GO_REF:0000118": ("signature", "TreeGrafter-generated GO annotations"),
    "GO_REF:0000120": ("signature", "Combined Automated Annotation using Multiple IEA Methods"),
}

#: The date GO's list was read on, recorded with every photo's channel table.
GOREF_LIST_READ_ON: Final[str] = "2026-11-16"


class UnknownEvidence(ValueError):
    """A code or an electronic reference the declared partition does not list."""


def channel_of(code: str, reference: str, reviewed: bool) -> Channel:
    """The channel of one GOA row.

    ``reference`` is the row's reference class as ``formats.reference_class``
    returns it: the first ``GO_REF`` it cites, ``publication`` or ``other``.
    """
    if code in TRUTH_CODES:
        return "truth"
    if code in CURATED_CODES:
        return "curated"
    if code in PHYLOGENETIC_CODES:
        return "phylogenetic"
    if code == NO_DATA_CODE:
        return "no_data"
    if code != AUTOMATIC_CODE:
        raise UnknownEvidence(f"evidence code {code!r} is not in the declared partition")
    entry = ELECTRONIC_REFERENCES.get(reference)
    if entry is None:
        raise UnknownEvidence(
            f"electronic reference {reference!r} is not in the declared table; read its "
            "title in GO's GO_REF list and add it"
        )
    kind = entry[0]
    if kind == "orthology":
        return "orthology_electronic"
    if kind == "field":
        return "reviewed_field_electronic" if reviewed else "signature_electronic"
    return "signature_electronic"
