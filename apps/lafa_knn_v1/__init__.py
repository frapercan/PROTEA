"""protea-knn-v1 LAFA submission container.

F-LAFA v2 submission #1 per ADR-D23: ProtT5 PLM, KNN GO propagation,
no learned reranker. Layers a thin LAFA-compatible entrypoint on top
of ``protea-method-runtime`` (ADR-D15) so the heavy layers (torch +
transformers + protea-method) ship only once across the three F-LAFA
v2 submissions (``protea-knn-v1``, ``protea-knn-8plm``, ``protea-v18``).

The container expects two bind mounts following the LAFA container
guide (``anphan0828/LAFA_container_guide``):

* ``/input/queries.fasta`` (host: FASTA file mounted in)
* ``/output/predictions.tsv`` (host: writable directory mounted out)

The frozen reference bundle is also bind-mounted at ``/bundle`` and
the HuggingFace cache at ``/hf-cache`` (so the ProtT5 weights stay
out of the image).
"""
