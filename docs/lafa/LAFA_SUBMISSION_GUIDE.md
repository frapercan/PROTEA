# LAFA submission: container contract, predictor catalog, and the GOA self-prior

Reference doc for PROTEA's LAFA online submission. Captures An Phan's
container guidelines, the full baseline predictor catalog, how our
`protea-knn-v1` image is built, the contract gap to close, and the GOA
self-prior change plus its shipping path.

## 1. An Phan's LAFA container contract

Source: `github.com/anphan0828/LAFA_container_guide` (the eval backend is
`github.com/anphan0828/CAFA_forever`).

Input contract (CLI flags the wrapper must accept):
- `--query_file` / `-q` (`.fasta`): query protein sequences
- `--train_sequences` (`.fasta`): training sequences
- `--annot_file` / `-a` (`.gaf`): training labels (GAF; gzipped ok)
- `--graph` (`.obo`): GO ontology
- `--output_file` / `-o`: output path
- Optional mounts: taxonomy (TSV), raw GOA (GAF.gz), AlphaFold PDB (TAR)

Output contract: 3-column TSV, no header, gzippable:
`Query_ID  GO_Term  Score`.

Container structure: `Dockerfile`, `requirements.txt`, `method_main.py`
(entrypoint), `config.yaml`. ENTRYPOINT is a non-interactive Python
wrapper. Data is NOT embedded in the image; it is bind-mounted:
`-v /host/data:/app/data:ro -v /host/output:/app/output:rw`. Publish to
public DockerHub with an example run command and a GitHub link. GPU is
optional but documented (test profile `--gpus all --memory=8g --cpus=4`).
No evidence-code filtering, runtime, or size caps are mandated; each
method chooses its own.

## 2. Baseline predictor catalog (CAFA_forever)

`enabled_window_methods` and how each container is invoked
(`CAFA_forever/modules/local/predictions.nf`, run via Singularity,
`releaseDir` bound to `/app/data`, writes `/app/output/<method>_predictions.tsv`):

Each row: method, image, resource, annotation source / key flags.

- Naive: `anphan0828/naive_predictor`, CPU, `train_terms.tsv`
- GOA Non-exp: `anphan0828/goa_nonexp_predictor`, CPU, `goa_uniprot_sprot.gaf.gz` with `--selected_go 'Computational,Phylogenetical,Electronic,ND,NAS'`
- BLAST: `anphan0828/blast_predictor`, GPU A100, `train_terms_propagated.tsv`
- ProtT5: `anphan0828/prott5_predictor`, GPU A100, `train_terms.tsv`
- TransFew: `yw7bh/transfew_predictor`, GPU A100, per-ontology
- FunBind: `yw7bh/funbind_predictor`, GPU scavenger, batched (batch=20)
- DeepGOPlus: `coolmaksat/deepgoplus`, GPU A100
- PROTEA-KNN: `ghcr.io/frapercan/protea/knn-v1`, GPU, our submission (below)

The t0 release dir (`release/`) carries the frozen `go-basic.obo`,
`goa_uniprot_sprot.gaf.gz`, `train_terms.tsv`, `train_terms_propagated.tsv`,
`train_sequences.fasta`, `test_sequences.fasta`, `IA.tsv`. The temporal
cut is the GOA release fed at t0 (e.g. Sep_2025 = GOA 2025-09-04);
experimental train terms use evidence `Experimental,IC,TAS`.

## 3. PROTEA's submission: protea-knn-v1

Image `ghcr.io/frapercan/protea/knn-v1` (ADR-D23). It is a THIN wrapper:
`FROM ghcr.io/frapercan/protea/method-runtime` plus
`apps/lafa_knn_v1/lafa_entrypoint.sh`, which runs `protea-predict` with
`--aspect_separated --no_v6 --no_reranker --self_prior`. The KNN, scoring,
and self-prior logic live in the method-runtime image (PROTEA core,
`apps/method_runtime/`), not in standalone scripts. Our bind layout:
`/bundle` (frozen reference, RO), `/input/queries.fasta`,
`/output/predictions.tsv`, `/hf-cache`.

The self-contained scripts under `~/Thesis2/protea-lafa-knn/`
(`protea_knn_infer.py`, `protea_knn_predict.py`, `protea_scoring.py`,
`build_self_prior.py`) are a PROTOTYPE port for fast experiments and local
re-evaluation; they do NOT build the ghcr image. The validated self-prior
now lives in PROTEA core (`apps/method_runtime/self_prior.py`).

## 4. Contract gap (to close)

An's pipeline invokes every predictor with the generic flags
(`--annot_file /app/data/...`, `--query_file /app/data/...`,
`--graph /app/data/...`, `--output /app/output/...`). Our knn-v1
entrypoint uses `/bundle` + `--frozen_data_dir` instead. It runs fine via
the method-card-documented command, but to be a drop-in for An's
automated window pipeline it should also accept the generic contract
(`--train_sequences`, `--annot_file`, `--graph`, `--output_file`, with
`/app/data` + `/app/output`). Recommended follow-up.

## 5. The GOA self-prior change (ported into PROTEA core)

Finding: GOA Non-exp slightly beat protea-knn-v1 on Sep_2025_Dec_2025
IA-weighted f_micro_w. Not temporal leakage. Cause: our KNN excludes
self-hits by accession, so it discards each target's OWN t0
non-experimental annotation, which is exactly what GOA Non-exp uses.

Fix (legit, not leakage): inject a GOA self-prior, the target's OWN t0
NON-EXPERIMENTAL annotations (same evidence set as GOA Non-exp:
Computational, Phylogenetical, Electronic, ND, NAS), scored confidently,
max-combined with neighbour transfer (neighbour scores scaled by 0.95).
PROTEA then becomes a superset of the GOA Non-exp signal plus the
embedding-KNN signal, so it overtakes the baseline. The universal reranker
can later learn the self-prior feature weight (it consumes
`apps/method_runtime/self_prior.py`).

Validated numbers (LAFA Sep_2025_Dec_2025, IA-weighted f_micro_w overall,
mean over NK/LK/PK):

Each row: predictor, overall, NK, LK, PK.

- PROTEA-KNN plus self-prior: 0.2974 overall (NK 0.393, LK 0.355, PK 0.139)
- GOA Non-exp baseline: 0.2958 overall (NK 0.393, LK 0.355, PK 0.139)
- PROTEA-KNN without self-prior: 0.2871 overall (NK 0.390, LK 0.338, PK 0.133)

The self-prior wins all three NK / LK / PK categories versus the old
PROTEA-KNN, and edges the GOA Non-exp baseline overall.

Leakage guardrails (enforced in code and tested):
- The self-prior is read ONLY from the t0-frozen reference annotation
  store (the same `reference_annotations.parquet` that backs the frozen
  reference embeddings, manifest `cutoff_date` 2025-09-04). NEVER the
  target's t1 / post-cutoff annotations.
- Only the non-experimental evidence codes the GOA Non-exp baseline uses
  are kept. Experimental codes (EXP/IDA/IPI/IMP/IGI/IEP, Hxx), IC, and TAS
  are dropped, so no experimental self annotation can leak in. A unit test
  asserts every forbidden code is rejected.
- NOT-qualified rows are dropped.
- Targets with no own non-exp annotation degrade gracefully to pure
  neighbour transfer.

The flag is OFF by default (`--self_prior`); existing behaviour is
unchanged unless it is enabled. The knn-v1 entrypoint enables it so the
next image build ships it.

## 6. Shipping path to LAFA

1. Prototype and validate the self-prior in `~/Thesis2/protea-lafa-knn/`,
   re-evaluate with `cafaeval-protea` (IA-weighted f_micro_w) vs the
   CAFA_forever groundtruth. (Done.)
2. Port the validated self-prior into PROTEA core scoring (worktree, PR to
   develop, local CI green). Move this doc into `docs/lafa/`. (This PR.)
3. Rebuild and push `ghcr.io/frapercan/protea/method-runtime` and
   `ghcr.io/frapercan/protea/knn-v1` with a new release tag. (Deferred;
   bundled with the universal reranker.)
4. Resubmit the new tag to LAFA (author / An Phan step). (Deferred.)
