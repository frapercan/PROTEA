"""protea-v18 LAFA submission container.

F-LAFA v2 submission #3 per ADR-D23: the full PROTEA pipeline. ProtT5
PLM, aspect-separated KNN GO propagation, v6 feature enrichment (NW/SW
alignments, taxonomy voters, Anc2Vec semantic coherence, embedding-PCA
projection), per-aspect LightGBM reranker. Lineage features
(``lineage_*`` per ALL_FEATURES) are consumed when the per-aspect
booster has been trained against them; the LightGBM native
missing-value branch handles them when the bundle does not ship a
``go_parents`` map (no in-container lineage compute is required to
keep the bundle layout LAFA-blind-eval friendly).

The container layers a thin LAFA-compatible entrypoint on top of
``protea-method-runtime`` (ADR-D15) so the heavy layers (torch +
transformers + protea-method + ProtT5 encoder) ship only once across
the three F-LAFA v2 submissions (``protea-knn-v1`` baseline,
``protea-knn-8plm`` ensemble, ``protea-v18`` full pipeline).

The container expects two bind mounts following the LAFA container
guide (``anphan0828/LAFA_container_guide``):

* ``/input/queries.fasta`` (host: FASTA file mounted in)
* ``/output/predictions.tsv`` (host: writable directory mounted out)

The frozen reference bundle is also bind-mounted at ``/bundle`` and
the HuggingFace cache at ``/hf-cache`` (so the ProtT5 weights stay
out of the image). The v18 bundle adds ``pca_state.npz``,
``anc2vec.npz``, and ``reranker/{F,P,C}.txt`` on top of the v1 base
layout; see the README for details.
"""
