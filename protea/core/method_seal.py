"""The seal on the METHOD side: a comparison declares its axis, everything else must match.

WHAT THIS IS THE BROTHER OF. ``seal_evaluation_frames`` enumerates what has to
agree for two numbers to be MEASUREMENTS of the same thing: the window, the
pivot, the accretion table, the caps. It leaves the thing being compared out of
the digest on purpose. This module is the same object one step upstream, over
``prediction_set``: what has to agree for two numbers to be measurements of two
systems that differ in ONE named way.

THE DEFECT IT ENDS. On 2026-08-28 two prediction sets were compared believing
only the donor bank moved. Measured afterwards, 31dd3eb8 (bank 220) and
0e076cb3 (bank 165) also differ in ``expand_votes_to_ancestors``,
``aspect_separated_knn``, ``code_revision`` and ``ontology_snapshot_id``. The
first of those alone explains one arm producing 2.76 times the candidates per
protein of the other, which was attributed to the ontology for five hours. Nine
deltas with intervals were published, six were called resolved, and three
mechanisms were proposed and refuted on that pair. None of it was about the
bank. The campaign calls this D2: a level named by fewer fields than it varies
in.

INCLUSION BY COMPLEMENT, NOT A DENY-LIST. The shape is:

    the comparison DECLARES its axis     "this compares annotation_set_id"
    the guard requires EVERYTHING ELSE   to agree, field by field
    and it refuses NAMING the ones that  do not, with both values

A deny-list of forbidden fields falls short the moment somebody adds a field.
The guard drafted on the night of the defect forbade ``ontology_snapshot_id``
only, and would have passed three of the four differences, including the one
that explained the effect. The complement of the axis fails on the safe side:
a field nobody thought about is required to agree rather than permitted to move.

THE DECLARATION IS EXACT IN BOTH DIRECTIONS. A field that moved and was not
declared is a refusal, and so is a field that was declared and did not move.
Without the second half the seal is disabled by declaring every field at once,
which costs a caller one line and reads, on the job row, exactly like a careful
declaration.

NOTHING UNCLASSIFIED PASSES. Every column of ``prediction_set`` and every key of
``prediction_set.meta`` is in one of two lists: identity, or execution with the
reason it is not identity. A column or key in neither is a refusal at the point
of comparison, so adding one without classifying it breaks the comparison
instead of silently widening what the seal lets through.

WHY NOT ``level_fields``. ``protea.api.routers._graph_panels.level_fields``
answers the neighbouring question -- which fields varied across a set of rows --
and it cannot be reused here, because it reads a hand-listed rendering
vocabulary of eight fields. ``annotation_set_id`` is not one of them, nor is
``expand_votes_to_ancestors``, nor ``aspect_separated_knn``: it would have
reported that this pair varied in ``code_revision`` and nothing else. A field
absent from that list is invisible to it, which is the failure mode the
complement exists to remove. This module is its brother, not its caller.
"""

from __future__ import annotations

import uuid
from collections.abc import Mapping, Sequence
from typing import Any

__all__ = [
    "EXECUTION_ONLY_FIELDS",
    "METHOD_IDENTITY_FIELDS",
    "MUST_BE_RECORDED",
    "CrossedMethodAxes",
    "assert_one_axis",
    "method_identity",
    "unclassified_columns",
]


class CrossedMethodAxes(ValueError):
    """Two prediction sets differ in more, or fewer, method fields than declared.

    Raised at the point of comparison and not when the record is read, for the
    reason ``CrossedFrames`` gives: the record is allowed to hold arms from any
    number of methods, and only a comparison that reaches across them is wrong.
    """


#: Method identity carried by ``prediction_set`` COLUMNS.
#:
#: ``annotation_set_id`` is the donor corpus: which annotations exist to be
#: voted. ``ontology_snapshot_id`` is the graph those votes are propagated and
#: expressed in, and it is not implied by the corpus -- the pair above differs
#: in both, and a comparison declaring only the first is naming half of what
#: moved. ``embedding_config_id`` is the space neighbours are near in.
#: ``query_set_id`` is which proteins were predicted at all, so two sets over
#: different populations are two experiments rather than two arms.
#: ``limit_per_entry`` is the RETRIEVAL depth and decides which candidates were
#: ever fetched, which ``_arm_identity`` separates from an evaluation cut for
#: the same reason it is identity here. ``distance_threshold`` decides which of
#: the fetched neighbours are allowed to vote.
_IDENTITY_COLUMNS: tuple[str, ...] = (
    "annotation_set_id",
    "distance_threshold",
    "embedding_config_id",
    "limit_per_entry",
    "ontology_snapshot_id",
    "query_set_id",
)

#: Method identity carried by keys of ``prediction_set.meta``, as
#: ``predict_go_terms._receipt.run_receipt`` writes it.
#:
#: ``expand_votes_to_ancestors`` is the field the defect above turned on: it
#: decides whether a donor's vote is cast for its term or for the whole ancestor
#: closure, so it moves the candidate count per protein by a factor before any
#: other decision is taken. ``aspect_separated_knn`` decides whether the
#: neighbour list is retrieved once or once per aspect, so it changes which
#: donors exist. ``exclude_self_neighbour`` decides whether a query votes for
#: itself, which is the difference between a measurement and a leak.
#: ``donor_policy`` decides which donors and which annotations may vote at all.
#: ``metric`` decides which neighbours are near. ``features`` decides which
#: channels were computed and fed forward. ``faiss`` and ``rerankers`` are
#: written only by the runs that used them, and both change the candidate list:
#: an approximate index returns different neighbours, and a reranker reorders
#: what survives the cut.
#:
#: ``search_backend`` is here and it is the exclusion a reader will reach for
#: first. It looks like execution -- the same neighbours, found by different
#: machinery -- and it is not: ``numpy`` searches exactly and ``faiss`` searches
#: approximately, so two arms on different backends can hold different donors
#: for the same query and the same K. An exclusion that reads as obviously safe
#: is exactly the shape of the hole this module exists to close.
#:
#: ``code_revision`` and ``dependency_revisions`` are identity for the argument
#: ``_arm_identity`` makes about ``donor_policy``: the stored object was
#: byte-identical either side of the 2026-08-29 change that moved its evidence
#: codes from gating pool admission to gating donation, so one recorded policy
#: names two incompatible experiments and only the revision separates them. The
#: dependency revisions make the same point one package out, because PROTEA pins
#: six siblings by commit and a node can hold the right tree with a stale one.
_IDENTITY_META: tuple[str, ...] = (
    "aspect_separated_knn",
    "code_revision",
    "dependency_revisions",
    "donor_policy",
    "exclude_self_neighbour",
    "expand_votes_to_ancestors",
    "faiss",
    "features",
    "metric",
    "rerankers",
    "search_backend",
)

#: Columns that are NOT method identity, each with why. An exclusion put wrong
#: is the hole this module exists to close, so none of them is silent.
_EXECUTION_COLUMNS: Mapping[str, str] = {
    "id": (
        "the row's own name. Two distinct prediction sets differ in it by "
        "construction, so requiring it to agree would refuse every comparison "
        "there is"
    ),
    "created_at": (
        "wall-clock time of the insert. Re-running the same method next Tuesday "
        "produces the same predictions"
    ),
    "meta": (
        "the container, not a field. It is classified key by key below; comparing "
        "the whole object would refuse on job_id, which is execution"
    ),
}

#: ``meta`` keys that are NOT method identity, each with why.
_EXECUTION_META: Mapping[str, str] = {
    "batch_size": (
        "how many queries ride in one forward pass. The search is per query and "
        "the arithmetic does not read the batch, so the same query gets the same "
        "neighbours at 256 or at 1024. This exclusion becomes wrong the day a "
        "backend builds an index per batch, which is why it is written down "
        "rather than assumed"
    ),
    "job_id": (
        "which job row ran it. Load-bearing provenance and not method: a set "
        "re-run under a new job id is the same method, and requiring it to agree "
        "would refuse every comparison for the same reason id would"
    ),
}

#: Every field that must agree unless the comparison declares it as its axis.
METHOD_IDENTITY_FIELDS: tuple[str, ...] = tuple(sorted(_IDENTITY_COLUMNS + _IDENTITY_META))

#: Every field deliberately left out, mapped to the reason it was left out.
EXECUTION_ONLY_FIELDS: Mapping[str, str] = {**_EXECUTION_COLUMNS, **_EXECUTION_META}

#: Fields whose ABSENCE is a refusal rather than a value both sides share.
#:
#: Everywhere else here two absences agree, and correctly: two runs that wrote
#: no ``faiss`` block both ran no faiss. A revision is different. An unrecorded
#: revision is not a revision two sets have in common, it is the absence of any
#: evidence that they ran the same code, and this project has already had one
#: node write 193,303 rows of a foreign format into a set that reported success.
#: This is the presence-first rule ``_frame_gate`` applies to its markers, held
#: to exactly the two fields it is true of.
MUST_BE_RECORDED: tuple[str, ...] = ("code_revision", "dependency_revisions")

_CLASSIFIED_COLUMNS = frozenset(_IDENTITY_COLUMNS) | frozenset(_EXECUTION_COLUMNS)
_CLASSIFIED_META = frozenset(_IDENTITY_META) | frozenset(_EXECUTION_META)

def _assert_the_lists_partition() -> None:
    """Refuse at import if a field is classified twice.

    A field in both lists would be required to agree and excused from agreeing
    at once, and which of the two won would be decided by the order the code
    happens to read them in. Run at import rather than in a test so the typo
    cannot reach a comparison even once.
    """
    for half, identity, execution in (
        ("columns", _IDENTITY_COLUMNS, _EXECUTION_COLUMNS),
        ("meta keys", _IDENTITY_META, _EXECUTION_META),
    ):
        both = sorted(set(identity) & set(execution))
        if both:
            raise ValueError(
                f"prediction_set {half} {both} are classified as method identity and as "
                "execution at once; a field can be one or the other"
            )


_assert_the_lists_partition()

#: How long a value may be before the refusal abbreviates it. A donor policy or
#: a dependency map renders to hundreds of characters, and a refusal nobody
#: finishes reading names nothing.
_SHOWN = 120


def _normalise(value: Any) -> Any:
    """A uuid column and the same uuid as text are one value, not two.

    The rows this seal reads come from ``SELECT *``, so a uuid column arrives as
    a ``UUID`` object while the same id inside ``meta`` arrives as a string.
    Comparing them raw would report a difference between two spellings of one
    value, which is a refusal a reader cannot act on, and it would render in the
    message as ``UUID('...')`` rather than as something they can paste into psql.
    """
    return str(value) if isinstance(value, uuid.UUID) else value


def _show(value: Any) -> str:
    text = repr(value)
    return text if len(text) <= _SHOWN else text[: _SHOWN - 3] + "..."


def _short(name: str) -> str:
    """The eight-character prefix this project names a set by.

    Used only on the per-field lines. The full ids are in the sentence above
    them, so nothing is lost and a refusal naming four fields stays readable
    instead of repeating two uuids eight times.
    """
    return name[:8]


def unclassified_columns(columns: Sequence[str]) -> tuple[str, ...]:
    """Which of ``columns`` this module has not classified.

    Exposed so a test can put the ORM model's columns through it and fail the
    build on the commit that adds a column, rather than at the first comparison
    that happens to read one. The runtime check in :func:`method_identity` is
    the same rule against the row actually fetched, and is what catches a
    ``meta`` key, which no model declares.
    """
    return tuple(sorted(set(columns) - _CLASSIFIED_COLUMNS))


def method_identity(row: Mapping[str, Any], *, name: str = "prediction set") -> dict[str, Any]:
    """Flatten one ``prediction_set`` row to the fields that name its method.

    ``row`` is the whole row -- every column, plus ``meta`` as a mapping. It has
    to be the whole row: a caller that hands over a hand-picked subset has
    re-introduced the deny-list this module refuses to be, one call site along.

    Raises:
        CrossedMethodAxes: when the row carries a column or a ``meta`` key that
            is in neither list, or when a revision is unrecorded.
    """
    meta = row.get("meta")
    if meta is None:
        meta = {}
    if not isinstance(meta, Mapping):
        raise CrossedMethodAxes(
            f"{name} carries meta of type {type(meta).__name__} rather than an object, so "
            "the method it ran cannot be read out of it"
        )

    stray_columns = unclassified_columns(list(row))
    stray_meta = tuple(sorted(set(meta) - _CLASSIFIED_META))
    if stray_columns or stray_meta:
        raise CrossedMethodAxes(
            f"{name} carries fields that protea.core.method_seal classifies as neither "
            f"method identity nor execution: columns {list(stray_columns)}, meta keys "
            f"{list(stray_meta)}. The seal is the complement of the declared axis, so an "
            "unclassified field would be neither required to agree nor knowingly excused: "
            "it would simply be invisible, which is how a comparison comes to vary in a "
            "field nobody named. Classify it in _IDENTITY_COLUMNS/_IDENTITY_META, or in "
            "_EXECUTION_COLUMNS/_EXECUTION_META with the reason it cannot change a result."
        )

    unrecorded = [field for field in MUST_BE_RECORDED if not meta.get(field)]
    if unrecorded:
        raise CrossedMethodAxes(
            f"{name} does not record {unrecorded}, so there is no evidence about which code "
            "produced it. Two unrecorded revisions are not a revision two sets share; they "
            "are the absence of the only field that separates a stored object from the same "
            "stored object under different semantics. This refusal is not waivable by "
            "declaring an axis, which names a DIFFERENCE and not an absence."
        )

    identity = {field: _normalise(row.get(field)) for field in _IDENTITY_COLUMNS}
    identity.update({field: _normalise(meta.get(field)) for field in _IDENTITY_META})
    return identity


def _refuse_undeclared(
    off_axis: Sequence[str],
    axis: Sequence[str],
    ids: tuple[dict[str, Any], dict[str, Any]],
    names: tuple[str, str],
) -> None:
    """Refuse a comparison that moves fields its declared axis does not name."""
    declared = ", ".join(axis) if axis else "nothing"
    lines = "\n".join(
        f"  {field}: {_short(names[0])}={_show(ids[0][field])}, "
        f"{_short(names[1])}={_show(ids[1][field])}"
        for field in off_axis
    )
    raise CrossedMethodAxes(
        f"{names[0]} and {names[1]} were declared a comparison of {declared}, and they are "
        f"not: they differ in {len(off_axis)} further method field(s) that nothing "
        f"declared.\n{lines}\n"
        "A level named by fewer fields than it varies in is not a level. The delta this "
        "comparison would report is the sum of every difference above and is attributable "
        "to none of them. Name every field that moved, or compare two prediction sets that "
        "differ only in the one you meant."
    )


def _refuse_unmoved(
    unmoved: Sequence[str], ids: tuple[dict[str, Any], dict[str, Any]], names: tuple[str, str]
) -> None:
    """Refuse a declaration that names a field both sides hold the same value for.

    Without this the seal is switched off by declaring the whole surface, which
    costs one line and is indistinguishable on a job row from a caller who
    measured what varied. With it the declaration is exactly the set of fields
    that moved, which is the property the campaign wanted from a level name.
    """
    lines = "\n".join(f"  {field}: both {_show(ids[0][field])}" for field in unmoved)
    raise CrossedMethodAxes(
        f"{names[0]} and {names[1]} were declared to compare {list(unmoved)}, and they do "
        f"not differ in it.\n{lines}\n"
        "An axis is the set of fields that moved, no more and no less. A declaration that "
        "names fields that did not move would let a caller declare the whole method surface "
        "and pass any pair at all, which is this guard switched off under a name that reads "
        "like care. Declare what actually varies."
    )


def assert_one_axis(
    axis: Sequence[str],
    a: Mapping[str, Any],
    b: Mapping[str, Any],
    *,
    names: tuple[str, str] = ("A", "B"),
) -> tuple[str, ...]:
    """Refuse unless ``a`` and ``b`` differ in exactly the fields ``axis`` names.

    Args:
        axis: the method fields this comparison claims to be varying. Empty
            means the claim is that the method is held entirely still, which is
            what a comparison of two scoring configs or two rerankers over one
            method is asserting.
        a: the whole ``prediction_set`` row of the arm under test.
        b: the whole ``prediction_set`` row of the baseline.
        names: what to call the two sides in a refusal. Pass the prediction set
            ids: a refusal that says "A" and "B" cannot be checked against the
            database by the person reading it.

    Returns:
        The declared axis, sorted, once it has been shown to be the whole truth
        about how the two differ. Callers record it beside the number.

    Raises:
        CrossedMethodAxes: when a method field outside ``axis`` differs, when a
            field in ``axis`` does not, when either row carries an unclassified
            field, or when either row has no recorded revision.
    """
    unknown = sorted(set(axis) - set(METHOD_IDENTITY_FIELDS))
    if unknown:
        raise CrossedMethodAxes(
            f"the declared axis names {unknown}, which is not method identity. "
            f"The fields that can be an axis are {list(METHOD_IDENTITY_FIELDS)}; "
            f"{sorted(EXECUTION_ONLY_FIELDS)} are excluded from the seal and cannot be "
            "compared. A misspelled axis declares nothing and would be refused one line "
            "later for the wrong reason."
        )
    repeated = sorted({field for field in axis if list(axis).count(field) > 1})
    if repeated:
        raise CrossedMethodAxes(f"the declared axis lists {repeated} more than once")

    ids = (method_identity(a, name=names[0]), method_identity(b, name=names[1]))
    differ = [field for field in METHOD_IDENTITY_FIELDS if ids[0][field] != ids[1][field]]
    off_axis = [field for field in differ if field not in axis]
    if off_axis:
        _refuse_undeclared(off_axis, sorted(axis), ids, names)
    unmoved = [field for field in sorted(axis) if field not in differ]
    if unmoved:
        _refuse_unmoved(unmoved, ids, names)
    return tuple(sorted(axis))
