"""Payload for the auto dump operation.

Originally lived in ``protea/core/training_dump_helpers.py``. The model
shape is unchanged; only the file location moved (T2B.6).
"""

from __future__ import annotations

from typing import Annotated

from pydantic import Field, field_validator

from protea.core.contracts.operation import ProteaPayload

PositiveInt = Annotated[int, Field(gt=0)]


# ProteaPayload is a pydantic BaseModel, not a dataclass;
# mypy's dataclass-frozen-from-non-frozen check is a false positive.
class TrainRerankerAutoPayload(ProteaPayload, frozen=True):  # type: ignore[misc]
    """Payload for the dump_helper operation.

    Generates consecutive temporal pairs from ``train_versions``, runs KNN
    once per pair, then trains 3 per-category LightGBM models (NK, LK, PK)
    and evaluates each on the held-out test split.
    """

    name: str
    embedding_config_id: str
    ontology_snapshot_id: str

    # GOA source_version numbers for training pairs (e.g. [160,165,...,220])
    train_versions: list[int]
    # GOA source_version numbers for test evaluation (e.g. [225] or [225,229])
    test_versions: list[int]

    # Annotation source in annotation_set (default "goa")
    annotation_source: str = "goa"

    # KNN parameters. Default to FAISS IVFFlat: numpy brute-force on 500k+
    # refs materialises a full (n_queries x n_refs) distance matrix that
    # peaks at ~10 GB per aspect. IVFFlat keeps peak memory ~2.5 GB and
    # is 5-10x faster.
    limit_per_entry: PositiveInt = 5
    distance_threshold: float | None = None
    search_backend: str = "faiss"
    metric: str = "cosine"
    faiss_index_type: str = "IVFFlat"
    faiss_nlist: int = 256
    faiss_nprobe: int = 32

    # LightGBM parameters
    num_boost_round: int = 1000
    early_stopping_rounds: int = 50
    val_fraction: float = 0.2
    neg_pos_ratio: float | None = None

    # Reranker objective: "binary" (default, classic logloss+AUC) or
    # "lambdarank" (listwise ranking loss with groups keyed by query
    # protein). LambdaRank optimizes ranking order directly and tends to
    # improve Fmax on retrieval-style tasks.
    reranker_objective: str = "binary"

    # Feature computation
    compute_alignments: bool = False
    compute_taxonomy: bool = False

    # lafa-integrate INT-6: train/serve feature parity. When set, the export
    # computes the SAME real self_prior / association / classifier feature
    # values the predict path serves (the producers in
    # ``predict_go_terms._post_knn_pipeline`` and ``classifier_producer``),
    # instead of the zero-fill defaults. Default False so existing exports are
    # bit-identical. Leakage-clean: every value reads only the pre-cutoff t0
    # annotation set (``version_to_set[v_old]`` / ``test_old_set_id``), the same
    # source the KNN reference pool uses, never a post-cutoff set.
    compute_self_prior: bool = False
    compute_association: bool = False
    compute_classifier: bool = False
    # ProtST text-to-GO transfer (protst_text lever). When set, the export stamps
    # the precomputed cosine-kNN ProtST vote onto each candidate via
    # ``predict_go_terms._protst_text.apply_protst_text`` (the SAME producer the
    # predict path uses behind ``compute_protst``), so the training and eval
    # pools carry matched values. Default False: exports stay bit-identical and
    # the family ships NaN (declared absent, ADR-D45).
    compute_protst: bool = False

    # Parity-producer batching: the self_prior / association / classifier
    # producers run ONCE per chunk of this many query proteins rather than once
    # per protein (the historical hotspot: a per-protein SQL + GPU
    # forward-pass storm). Value-preserving (they key strictly by
    # ``(protein_accession, go_term_id)`` and never mix proteins). 512 bounds
    # the records held in RAM; set 0 for per-split (one call over all queries).
    parity_chunk_size: int = 512

    # IA weighting: path to IA TSV file (go_id\tia_value, no header).
    # When set, sample_weight = IA(go_term) during training so the model
    # focuses on informative (rare, specific) GO terms aligned with
    # CAFA evaluation which uses IA weighting.
    ia_file: str | None = None

    # Ancestor expansion: when True, synthesize candidate records for every
    # ancestor of each leaf GO term voted by a neighbor (True Path Rule at
    # vote time). Weight of the inherited vote = IA(ancestor)/IA(leaf) when
    # an IA table is available; 1.0 otherwise. Expands the candidate set
    # and helps the reranker learn on abstract terms that never get direct
    # KNN votes but do appear in ground truth.
    expand_votes_to_ancestors: bool = False

    # Sequence-embedding PCA: when True, fit PCA(16) once on the reference
    # embedding pool, project each query, and emit 16 extra features
    # (emb_pca_query_0..15) per candidate row. Gives LightGBM a location
    # signal in PLM space beyond the scalar query<->ref distance.
    use_embedding_pca: bool = False

    # Training scope:
    #   "per_category" (default): 3 models, one per NK/LK/PK (all aspects).
    #   "per_cell":               up to 9 models ({cat}-{aspect}) plus the
    #                              3 per-category fallbacks. Any cell whose
    #                              positive count falls below
    #                              ``per_cell_min_positives`` is skipped and
    #                              the per-category model is used instead
    #                              (CCO-LK protection).
    training_scope: str = "per_category"
    per_cell_min_positives: PositiveInt = 200

    # Dataset dump: when ``dump_to`` is set, consolidate the generated
    # per-split / test parquet shards into ``{dump_to}/train.parquet`` +
    # ``{dump_to}/eval.parquet`` + ``manifest.json``. When ``dump_only`` is
    # True, skip the LightGBM training stage and return after the dump.
    # Used by the protea-reranker-lab repo to iterate on frozen datasets.
    dump_to: str | None = None
    dump_only: bool = False

    # NFR-ARCH "export decouple". When True, the STABLE per-candidate feature
    # table (everything except the classifier columns) is content-addressed and
    # cached in the artifact store, so a later export with the same stable
    # config but a different classifier reuses the cached table and only
    # recomputes the classifier column(s). Default False so the heavy pipeline
    # and the golden parquet are byte-identical when the flag is off. The cache
    # is keyed by ``_stable_feature_cache.compute_stable_cache_key`` (classifier
    # config is deliberately excluded from the key).
    stable_feature_cache: bool = False
    # Phase-2 SKETCH: a list of classifier-variant specs (each a label + a seed
    # dir / seed paths / model path). When given, the export emits one
    # ``classifier_score__<label>`` / ``classifier_present__<label>`` column
    # family per variant in a single pass over the (cached) stable table, so
    # the lab trains a booster per variant from the same parquet. ``None`` keeps
    # the canonical single ``classifier_score`` / ``classifier_present``
    # columns. Wired in phase 2; carried here so the payload contract is stable.
    classifier_variants: list[dict[str, str]] | None = None

    @field_validator("embedding_config_id", "ontology_snapshot_id", "name", mode="before")
    @classmethod
    def must_be_non_empty(cls, v: str) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ValueError("must be a non-empty string")
        return v.strip()

    @field_validator("train_versions", mode="before")
    @classmethod
    def at_least_two_train(cls, v: list[int]) -> list[int]:
        if len(v) < 2:
            raise ValueError("train_versions must have at least 2 entries to form pairs")
        return sorted(v)

    @field_validator("test_versions", mode="before")
    @classmethod
    def at_least_one_test(cls, v: list[int]) -> list[int]:
        if not v:
            raise ValueError("test_versions must have at least 1 entry")
        return sorted(v)

    @field_validator("training_scope", mode="before")
    @classmethod
    def scope_is_valid(cls, v: str) -> str:
        allowed = {"per_category", "per_cell"}
        if v not in allowed:
            raise ValueError(f"training_scope must be one of {sorted(allowed)}, got {v!r}")
        return v
