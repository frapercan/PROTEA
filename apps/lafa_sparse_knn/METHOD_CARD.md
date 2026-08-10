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
4. Transfer the neighbours' GO terms. A term scores as the
   similarity-weighted fraction of neighbours carrying it, so it lands in
   `[0, 1]` by construction.
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
| bank rows | 575,503 canonical proteins, over 528,294 distinct sequences |
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
| `/hf-cache` | read-write | backbone weights |

Missing mounts exit with distinct codes: 64 bundle, 65 queries, 66
output, 67 ontology.

## Output

One TSV, `EntryID<TAB>GO_ID<TAB>score`, no header, scores in `[0, 1]` at
three decimals. Capped at 500 terms per query and aspect. The cap is per
aspect rather than global so a large biological process closure cannot
crowd out the molecular function calls.

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
- Sequences are truncated at 2048 residues, which touches about twenty
  proteins in a Swiss-Prot-sized set.
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

Embedding a protein that is already in the bank must return that protein
first at cosine 0.99 or above. On 20 held proteins, 19 returned
themselves and the twentieth returned an accession sharing its exact
sequence, so parity is 20 of 20. A consumer seeing lower has drifted from
`EMBEDDING_RECIPE.json` and should not trust the output. This is the
cheapest way to catch the failure that would otherwise be silent.
