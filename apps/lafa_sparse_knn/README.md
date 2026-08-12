# protea-sparse-knn

LAFA submission container. Nearest-neighbour GO transfer in the space of
a learned sparse encoder.

See `METHOD_CARD.md` for what was fitted, on which release, and with
which ontology. This file covers building and running it.

## What it is

One backbone, one retrieval pass, all three aspects. The query is
embedded with Ankh-base, projected through a learned linear map to a
2048-dimensional dictionary, sparsified to its 128 largest components,
and matched against a frozen bank of 575,503 reference proteins by exact
cosine. A term is scored by how close the nearest donor carrying it is and
by how many donors agree, weighted 2 to 1 in favour of proximity, then
propagated to ancestors.

There is no reranker and no per-aspect model in this image. It is the
retrieval core on its own.

## Building

The image layers on `protea-method-runtime` and pins `protea-backends`
by resolved commit, which is a required build argument. A branch pin
would make that branch the delivery channel and let the image drift from
the bank without anything failing.

```bash
docker build \
    -f apps/lafa_sparse_knn/Dockerfile \
    --build-arg PROTEA_BACKENDS_REF=<resolved commit sha> \
    -t protea-sparse-knn:latest .
```

## Running

```bash
docker run --rm \
    -v /path/to/bundle:/bundle:ro \
    -v $PWD/queries.fasta:/input/queries.fasta:ro \
    -v $PWD/go-basic.obo:/input/go-basic.obo:ro \
    -v $HOME/.cache/huggingface:/hf-cache \
    -v $PWD/out:/output \
    protea-sparse-knn:latest
```

Arguments are forwarded to the driver, so `--k`, `--max_bpo`, `--max_mfo`,
`--max_cco`, `--score`, `--blend`, `--batch_size` and `--device` can be
swept without rebuilding. There is no `--max_per_aspect`: the cap is three
separate flags because biological process needs a larger allowance than the
other two. Run the image with `--help` for the full list rather than
trusting this one.

## The bundle

| file | what it is |
| --- | --- |
| `encoder.pt` | the trained sparse encoder, 6.3 MB |
| `codes_idx.npy` | dictionary positions of the kept components, `uint16` |
| `codes_val.npy` | their values, `float16` |
| `accessions.npy` | row order, mapping a code back to a protein |
| `donors.tsv.gz` | accession and GO id, the annotations transferred |
| `BANK.json` | what the bank was built from |
| `EMBEDDING_RECIPE.json` | the backbone recipe the bank was built with |

`EMBEDDING_RECIPE.json` is load-bearing rather than documentation. The
container reads the layer, pooling, normalisation and truncation off it
and hands them to the backend, so the queries are embedded by the same
recipe as the references. A bundle whose recipe disagrees with its bank
would place the two sides in different geometries.

## Why the backend is a dependency

Ankh is a T5 with a character vocabulary. The producing backend
tokenises a list of single characters with `is_split_into_words=True`
after mapping `[UZOB]` to `X`, and runs in bfloat16 on CUDA because FP16
LayerNorm collapses to NaN on this family. Re-implementing that inside
the container would work until one of those details drifted, and the
failure would be a quiet loss of accuracy rather than an error. The
producer travels with the bank.

## Tests

`tests/test_lafa_sparse_knn_container.py` covers the ontology walk, the
vote, the propagation and the output contract, with no torch, no
backbone and no bank. Run it with the repository's pytest.
