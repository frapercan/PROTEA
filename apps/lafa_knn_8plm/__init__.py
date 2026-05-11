"""protea-knn-8plm: F-LAFA v2 submission #2 (ADR-D23).

Ensemble of eight protein language models. Each PLM produces a
mean-pooled embedding, an independent per-aspect KNN search emits
``(query, go_term, score)`` candidates, and the ensemble driver
combines the per-PLM candidate sets by score-level mean aggregation.
"""
