"""The search space of an embedding configuration at a cut.

:mod:`~protea.core.embedding_space.statistics` measures the space without labels
(two streaming passes over the bank's shards) and writes the artifact that
:mod:`~protea.core.embedding_space.transforms` reads to centre, standardise,
whiten or remove the dominant directions of a bank's vectors.
"""
