# What a curated NOT was telling us, and what the run did with it

A NOT annotation is a curator saying, with evidence, that a protein does not do
something. It is not a gap in the record. It is a statement, and it is usually
written because somebody had reason to believe the opposite, which in this
field almost always means a homologue.

That makes it the one class of record in the corpus aimed squarely at the
error a homology-transfer method makes. On the 2026-08-30 campaign the method
walked into it half the time.

## The counts

Prediction set `9995651a` at depth 30, against its own bank `cbb35a32`, the
corpus as of t0. Nothing here reads the evaluation truth: t0 carries 5,603 NOT
annotations and t1 carries 5,397, and the check uses only the first.

Of those 5,603, **956 are about a protein in the query set**.

| | NOT annotations | predicted anyway | rate |
|---|---|---|---|
| molecular function | 291 | 167 | 57.4% |
| cellular component | 213 | 145 | 68.1% |
| biological process | 452 | 166 | 36.7% |
| **all** | **956** | **478** | **50.0%** |

Half. And the two aspects where a homology method is most confident are the
two where it is most often wrong about this: function at 57 per cent, location
at 68 per cent.

## It is not happening at the tail

The 478 violations sit near the top of their own query's list:

| | |
|---|---|
| predicted in first place | 115 |
| predicted inside the top five | 289 |
| median k_position | 4 |
| median donor distance | 0.0129 |

These are not marginal candidates surviving a generous cut. They are the
method's most confident answers.

## And the evidence against them is direct

Evidence codes on the 478:

| code | n | share |
|---|---|---|
| IDA, direct assay | 205 | 42.9% |
| IMP, mutant phenotype | 62 | 13.0% |
| RCA | 57 | 11.9% |
| ISO | 52 | 10.9% |
| ISS | 24 | 5.0% |
| IKR | 23 | 4.8% |
| IBA | 21 | 4.4% |

286 of the 478 carry an experimental code (EXP, IDA, IPI, IMP, IGI, IEP and
their high-throughput equivalents). Somebody did the experiment, it came out
negative, they recorded it, and the run predicted it anyway.

## The case worth remembering

`O94526`, fission yeast PTEN. The bank says:

```
NOT|enables  GO:0004439  phosphatidylinositol-4,5-bisphosphate 5-phosphatase activity  (IDA)
```

The run predicted exactly that term, at **k=1**, from donor `P40559` at
distance 0.0151.

This is the whole problem in one row. PTEN homologues are lipid phosphatases;
that is what the family is known for. A curator assayed this particular
protein, found it does not have the activity, and wrote it down. The
annotation exists *because* the homology is misleading here. The method used
the homology, ignored the annotation, and produced the error the annotation
was written to prevent.

Second case, `Q9Y468` (LMBL1_HUMAN): `NOT|located_in GO:0005730 nucleolus`
(IDA), predicted at k=2 from `Q96JM7` at distance 0.0090.

## A denial is bigger than the term it names

A positive annotation propagates up: a protein with a child term has the
parent. A denial is the contrapositive and propagates **down**: a protein that
does not have the parent cannot have any descendant of it.

Propagating the 943 denials that have a term in the run's ontology, under
`is_a` and `part_of` only:

| | |
|---|---|
| direct denials | 943 |
| denied (protein, term) pairs after descent | **39,105** |
| expansion | 41.5x |
| of those, predicted by the run | **1,190** |

So the visible violation, 478, is under half of the real one.

## What was done

`_denials.py` (PR 912) removes candidates the bank denies, in all three
writers. Three design points are worth restating because they are about a
defect class this project keeps hitting rather than about NOT annotations:

**The policy reads its own inputs off the `prediction_set` row.** The depth cut
spent an entire campaign silently unapplied because it was a field that two
construction sites both had to remember to fill, and neither did. A check that
can be forgotten into being off will eventually be off.

**All three writers apply it.** The campaign runs through the reranker, which
never builds the cached base frame. A filter that some writers apply and
others do not is that same defect wearing a different name.

**The ontology is the prediction set's own, not the evaluation snapshot's.**
The two disagree by 29 pairs. Reading the later one would be reading DAG
structure from after the cutoff.

## What this does not settle

1,190 pairs out of 2,441,584 will not move a headline metric, and the fix was
not made for the metric. The measurement's value is diagnostic: it shows the
method is confidently wrong exactly where the corpus took the trouble to say
so, and it shows it at k=1, which no distance threshold or depth cut reaches.

Three things stay open.

**The 5,603 NOT annotations are a labelled negative set and nothing uses them
as one.** Filtering removes a wrong answer after the fact. A method that
learned from them would not propose it.

**Nothing tests whether the query has an *implicit* denial**, meaning a term
that is inconsistent with what the query is already known to be rather than
one a curator explicitly ruled out. That is a much larger set and needs the
ontology's own structure to define.

**The rate differs threefold across aspects and nobody has asked why.** If C
is at 68 per cent because location transfers badly between homologues, that is
a statement about the method that goes well beyond these 956 rows.
