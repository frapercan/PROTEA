# Method card: protea-sparse-knn

LAFA submission. Nearest-neighbour GO transfer in the space of a learned
sparse encoder.

This is a verification surface, so it carries the concrete identifiers.
The narrative account of the method lives elsewhere and names things by
what they are.

## What the method does

1. Embed the query with Ankh-base, mean-pooled over the last layer,
   L2-normalised, truncated at 2048 residues.
2. Project the 768-dimensional embedding through a learned linear map to
   a 2048-dimensional dictionary and keep the 128 largest components.
3. Retrieve the 30 nearest reference proteins by cosine in that sparse
   code space, exact brute force on the processor.
4. Transfer the neighbours' GO terms. A term scores as
   `agreement ** 0.33 * proximity ** 0.67`, where agreement is the
   similarity-weighted fraction of neighbours carrying it and proximity is
   the similarity of the closest one that does. Both are in `[0, 1]`, so
   the product is too, with no rescaling.
5. Propagate to ancestors under the true path rule, taking the maximum
   over descendants.

One retrieval pass produces all three aspects. There is no per-aspect
model and no reranker in this image.

## The encoder

A single `nn.Linear(768, 2048)` followed by a top-128 selection. It is
trained so the cosine between two proteins' codes matches the
information-content-weighted Lin semantic similarity of their propagated
GO closures, with hard negatives mined from embedding-near neighbours.
The trained object is 6.3 MB and ships in the bundle.

Held out on 4,000 proteins absent from the training pool, over 120,000
pairs:

| representation | Spearman against Lin similarity |
| --- | --- |
| raw Ankh-base embedding | +0.168 |
| encoder, dense | +0.530 |
| encoder, top-128 (what ships) | +0.467 |

## Training and data cutoff

Everything below is fitted once and frozen. Nothing is refitted at
inference.

| item | value |
| --- | --- |
| annotation release | GOA 227, published 2025-09-04 |
| ontology | `releases/2025-07-22` |
| bank rows | 575,503 canonical proteins, over 487,237 distinct sequences |
| donor annotations | GOA 227, 5,880,402 over 557,071 proteins |
| encoder training pool | 60,000 proteins, strided over the annotated set |
| seed | 42 |
| published bundle | `XaxiPiruli/protea-sparse-knn` on the HuggingFace Hub |

The ontology snapshot is the last one at or before the annotation
release, and the training driver refuses to run if that ordering is
violated, because a later ontology would put terms that did not exist at
the cutoff on the label side of the objective.

The earlier release, GOA 226 of 2025-05-03, is reserved as the window for
choosing hyperparameters and is not used to fit the delivered artefact.

## Bind mounts

| path | mode | contents |
| --- | --- | --- |
| `/bundle` | read-only | `encoder.pt`, `codes_idx.npy`, `codes_val.npy`, `accessions.npy`, `donors.tsv.gz`, `BANK.json`, `EMBEDDING_RECIPE.json` |
| `/input/queries.fasta` | read-only | query FASTA |
| `/input/go-basic.obo` | read-only | the ontology at the cutoff |
| `/output` | writable | `predictions.tsv` |
| `/hf-cache` | read-only | backbone weights |

Missing or unusable mounts exit with distinct codes: 64 bundle, 65
queries, 66 output directory absent, 67 ontology, 68 output not writable
by the container's uid, 69 output not a bind mount so the predictions
would be discarded, 70 no backbone cache.

## Output

One TSV, `EntryID<TAB>GO_ID<TAB>score`, no header, scores in `[0, 1]` at
three decimals. Capped at 1500 terms per query for biological process and
500 for the other two. The cap is per aspect so a large biological process
closure cannot crowd out the molecular function calls, and larger for BP
because its closures are far deeper: under a flat 500 the terms a protein
already carried at the cutoff were lost to the cap rather than to a missing
vote, and the loss fell on biological process while cellular component lost
none.

The counts that used to stand here have been struck. They were internally
impossible, claiming a subset larger than the set it came from, and the run
that produced them is not reproducible from what ships. A number a reader
cannot check is worth less than the mechanism, which is checkable: compare
the per-aspect closure depths.

## Resources

The bank is held as a CSR matrix, about 295 MB for 575,503 rows at 128
non-zeros each. Queries are scored in blocks of 256, so the dense
similarity buffer stays near 590 MB whatever the query count. Peak
resident memory is therefore roughly 1.5 GB plus the backbone.

Measured end to end on one RTX 3060, 20 queries of 150 to 400 residues:
11 seconds including loading the bank, the donors, the ontology and the
backbone. Retrieval itself was under a second.

The backbone runs in bfloat16 on CUDA and float32 on the processor, which
is the backend's own rule: FP16 LayerNorm collapses to NaN on this
family. Retrieval never moves to the GPU.

## Caveats

- No runtime downloads beyond the backbone, which can be pre-seeded into
  the cache mount.
- Deterministic: retrieval is exact, not an approximate index.
- Sequences are truncated at 2048 residues. On the 7,401 targets of one
  release window that touched 216 proteins, 2.9 percent, and the longest
  lost 77 percent of itself. Chunking is supported by the recipe but
  disabled, because the bank was built without it.
- Any target can be scored. Coverage does not depend on the query having
  been seen at training time.
- The queries are embedded with `protea-backends`, the same code that
  produced the bank. This is deliberate: Ankh tokenises a list of single
  characters after mapping `[UZOB]` to `X`, and a lookalike encoder that
  tokenises the raw string would place queries in a different geometry
  and degrade silently.
- The bank is keyed by protein, not by sequence. Accessions sharing a
  sequence are separate rows carrying their own annotations, so they are
  distinct donors but also exact ties in retrieval: a query matching a
  sequence held by five accessions fills five neighbour slots with the
  same vector.

## Parity check

Pass `--self_check` and the container reports it for you:

```
self-check: 20 queries are in the bank; 20 are their own nearest
neighbour; 0 were not retrieved at all; lowest self-cosine 0.9845
self-check: passed. Query and bank are in the same geometry.
```

A protein already in the bank must come back as its own nearest
neighbour. If it does not, the queries were embedded by a recipe that
differs from the one the bank was built with, every retrieval is against
the wrong geometry, and the output is still a well formed file of
plausible and worthless scores. Nothing else in the run would say so,
which is why this exists.

**The criterion is rank, not a cosine threshold.** An earlier version of
this document asked for cosine 0.99 or above, and that bar is wrong. The
bank ships float16 and was built with the backbone in bfloat16 on a
graphics card, while a processor run embeds in float32, so the exact
self-cosine moves with the runtime path. Measured on the twenty reference
proteins with the network disabled and no graphics card, every one of them
ranks first and the lowest self-cosine is 0.9845, which the old bar would
have failed. A threshold that rejects a correctly configured run is worse
than no check, because it sends a working setup back to debug itself.

Accessions sharing a sequence share a code exactly, so a query is often
preceded by its own twins at an identical cosine. Those are ties and
count as rank one.

## The scoring weight

A term's score mixes two quantities that disagree about what evidence is:
how close the nearest donor carrying it is, and how many donors agree. They
are combined geometrically, `agreement ** (1-w) * proximity ** w`, with
`w = 0.67`.

That number is not fitted here. It is PROTEA's own `composite` scoring
configuration, which weights embedding similarity 0.4 against neighbour vote
fraction 0.2, renormalised over the two signals this container has; the other
0.4 there belongs to alignment identity and taxonomic proximity, which this
image does not compute.

Swept over one release window it held up. The optimum landed at 0.70 in four
of the nine cells, more than any other value, and pure agreement won none.
The published 0.67 is kept rather than the swept 0.70, because a constant
confirmed from elsewhere is worth more than one fitted on the window it is
then reported against.

Both ends remain reachable: `--score vote` is pure agreement, `--score maxsim`
pure proximity, `--blend` moves the dial.
