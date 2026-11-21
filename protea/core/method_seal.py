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
#:
#: ``method_id`` is the method row that produced the set (ADR-D60, decision 3).
#: Its id is derived from the method's whole spec (family, code, instance cut,
#: recipe and pins), so two sets under different ids ran different methods even
#: when every other column agrees, and a set produced by a method without an
#: embedding configuration differs from a configuration's set in both fields.
#: Two NULLs agree and correctly: neither set was produced by a method row,
#: which is every set written before the method tables existed.
_IDENTITY_COLUMNS: tuple[str, ...] = (
    "annotation_set_id",
    "distance_threshold",
    "embedding_config_id",
    "limit_per_entry",
    "method_id",
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
    # Whether and how the method honoured the bank's denials when it predicted
    # (protea.core.denials): it changes which candidates exist.
    "denials",
    "donor_policy",
    # The window a native method read its inputs from (run_own_annotations,
    # run_count_map): its targets, its cut's state and its frame's rules, the
    # NOT codes among them, which no other field names whole.
    "evaluation_set_id",
    "exclude_self_neighbour",
    "expand_votes_to_ancestors",
    "faiss",
    "features",
    # The InterPro BP graft on top of the reranker's scores: it rescores BP
    # candidates and adds InterPro's own, so it changes the stored set.
    "interpro_bp_graft",
    "metric",
    # The proteins the set was asked about, when they were handed to the run as a
    # list rather than as a query set: their count and digest
    # (protea.core.query_population). Two arms of one window ask about its targets.
    "query_population",
    "rerankers",
    # How a scored set's candidates became scores: the arithmetic, the depth
    # cut and the code that applied them (score_prediction_set).
    "scoring",
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
    "description": (
        "a sentence the writer derives from the other fields (ADR-D58). It names "
        "nothing they do not, and its wording can change between revisions while "
        "the predictions stay the same"
    ),
    "job_id": (
        "which job row created the set: provenance and not method, for the reason "
        "meta's job_id is. A set re-run under a new job is the same method"
    ),
    "producer_git_sha": (
        "the code that wrote the row, kept for the sets gate. The revision that "
        "identifies the method is meta's code_revision, which run_receipt writes and "
        "_IDENTITY_META compares; the column is not compared, so the sets written "
        "before it existed compare as they did"
    ),
    "summary": (
        "counts of the set's own rows (rows, proteins, totals per aspect, most "
        "frequent terms), kept so its pages stop counting them on every visit. It "
        "follows from the predictions and names nothing about how they were made"
    ),
}

#: ``meta`` keys that are NOT method identity, each with why.
_EXECUTION_META: Mapping[str, str] = {
    "scored_from": (
        "which candidate set a scored set was scored from. The id of a row, "
        "like id: a method re-run and scored again has another, and the method "
        "it ran is the candidate set's record, copied into the scored set's"
    ),
    "scoring_config_id": (
        "which ScoringConfig row the arithmetic was read from. Two rows holding "
        "the same arithmetic score alike, and the arithmetic itself is the "
        "scoring key, which is identity"
    ),
    "denied_removed": (
        "how many candidates the bank's denials removed. A consequence of the "
        "data the method met, not a choice: two runs of the same method over "
        "different queries remove different numbers. The choice is the "
        "denials key, which is identity"
    ),
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
    # The keys a native method's set carries (protea.core.method_write). Its
    # identity is method_id, the method row with its recipe and pins, which is
    # an identity column; these are labels of it or counts of what it met.
    "algorithm": (
        "'native_method' on every set a native method writes. It follows from "
        "method_id being set, and method_id is identity"
    ),
    "method": (
        "the method's short name, read off the row method_id names. It cannot "
        "move unless method_id does"
    ),
    "targets_with_a_term": (
        "how many of the targets got a row, counted off the rows written. A "
        "consequence of the data, like denied_removed"
    ),
    "targets_without_a_sequence": (
        "how many targets had no sequence to read. Which ones follows from the "
        "population and the cut, both identity"
    ),
    "aspects": (
        "what a count map's fit found per aspect: its sizes and the strength the "
        "bits chose. Outcomes of the method over its inputs, which are identity "
        "(method_id with its pins, the cut, the population)"
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
#: This is the presence-first rule ``_frame_gate`` applies to its markers.
#:
#: ``query_set_id`` JOINED THEM 2026-10-28, and it is the same argument about
#: the population rather than the code. Measured over the eleven stored
#: prediction sets: nine declare a query set and in every one of the nine the
#: declared entry count equals the distinct accessions actually predicted
#: (5.810, 40.531 and 63.734). The two that declare nothing are the ingested
#: arms, and they hold **40.531 and 20.854** proteins. So two absences here are
#: demonstrably two populations, and a gate that let them agree would read a
#: difference in who was asked as a difference in method -- which is what the
#: comment on ``query_set_id`` in :data:`_IDENTITY_COLUMNS` already says the
#: field is for.
#:
#: Not waivable by declaring the axis, like the revisions: an axis names a
#: difference between two known values and there is no value here to name.
#: ``ingest_method_predictions`` has accepted ``query_set_id`` all along, so the
#: remedy for a refused ingested arm is to declare the population it predicted.
MUST_BE_RECORDED: tuple[str, ...] = (
    "code_revision",
    "dependency_revisions",
    "query_set_id",
)

#: What each presence-first field's absence means, said in the refusal.
#:
#: One sentence per field rather than one for all three: the old message
#: explained every absence as "no evidence about which code produced it", which
#: was true of the two revisions and would be simply wrong about the population.
_ABSENCE_MEANS: Mapping[str, str] = {
    "code_revision": "there is no evidence about which code produced it",
    "dependency_revisions": "there is no evidence about which libraries produced it",
    "query_set_id": (
        "there is no evidence about which proteins it was asked to predict: neither a "
        "query set nor the digest of the list it was handed (query_population)"
    ),
}

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

    unrecorded = [field for field in MUST_BE_RECORDED if not _recorded(row, meta, field)]
    if unrecorded:
        because = "; ".join(_ABSENCE_MEANS[field] for field in unrecorded)
        raise CrossedMethodAxes(
            f"{name} does not record {unrecorded}, so {because}. Two absences are not a "
            "value two sets share; they are the absence of a field that separates a stored "
            "object from the same stored object under different semantics. This refusal is "
            "not waivable by declaring an axis, which names a DIFFERENCE and not an absence."
        )

    identity = {field: _normalise(row.get(field)) for field in _IDENTITY_COLUMNS}
    identity.update({field: _normalise(meta.get(field)) for field in _IDENTITY_META})
    return identity


def _recorded(row: Mapping[str, Any], meta: Mapping[str, Any], field: str) -> Any:
    """A presence-first field's value, read from wherever that field lives.

    The two revisions are ``meta`` keys and ``query_set_id`` is a column, so the
    check this replaces -- ``meta.get(field)`` for every field -- would have
    called every prediction set in the deployment unrecorded and refused every
    comparison, including the nine that do declare their population.

    Asks the two identity lists which half a field belongs to rather than
    keeping a third list beside them, because a third list is one that can
    disagree with both and :func:`_assert_the_lists_partition` would not see it.
    """
    if field == "query_set_id":
        # The population is declared either way: by a query set, or by the digest
        # of the list the run was handed (protea.core.query_population).
        return row.get(field) or meta.get("query_population")
    return row.get(field) if field in _IDENTITY_COLUMNS else meta.get(field)


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
