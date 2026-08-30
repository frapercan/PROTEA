"""What a term looks like before the graph says anything about it.

Node features for the graph encoder, built from the ontology's own text. Every
one of GO's 40,214 non-obsolete terms carries a name and a definition, median
19 words, and both are part of the ontology rather than of any corpus, so they
carry no date problem.

A WARNING THAT BELONGS HERE. GO names are compositional and its definitions are
genus-differentia, so the hierarchy is partly written into the text. Measured on
snapshot 36038118: the parent's name appears verbatim inside the child's name on
23.4 per cent of is_a/part_of edges and inside the child's definition on a
further 4.3, so a quarter of the graph is recoverable by string matching alone.
Any claim that text predicts structure has to be reported separately on the
4,644 edges (6.7 per cent) whose names share no content token, or it is a
measurement of str.find.

The definition earns its place on exactly those: for 50 per cent of them the
child's definition recovers a token of the parent's name that the child's name
does not have.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
from sklearn.decomposition import TruncatedSVD
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import normalize


@dataclass(frozen=True)
class TextFeatureConfig:
    """Word n-grams for phrasing, character n-grams for morphology.

    Both, because they fail in opposite places. A word tokeniser shreds
    ``phosphatidylinositol-4,5-bisphosphate`` into nothing reusable, and a
    character tokeniser cannot tell ``regulation of X`` from ``X``.
    """

    dim: int = 256
    word_ngrams: tuple[int, int] = (1, 2)
    char_ngrams: tuple[int, int] = (3, 5)
    min_df: int = 2
    max_features: int = 200_000
    seed: int = 0


def text_features(
    names: list[str], definitions: list[str], config: TextFeatureConfig
) -> np.ndarray:
    """One dense row per term, from ``name`` and ``definition`` together.

    The name is repeated so it is not drowned by a definition several times its
    length. Deterministic: TF-IDF and a seeded truncated SVD, with no download
    and no pretrained weights, so the features are reproducible from the
    ontology alone.
    """
    docs = [f"{n} . {n} . {d}" for n, d in zip(names, definitions, strict=True)]
    blocks = []
    for analyzer, ngrams in (("word", config.word_ngrams), ("char_wb", config.char_ngrams)):
        tf = TfidfVectorizer(
            analyzer=analyzer,
            ngram_range=ngrams,
            min_df=config.min_df,
            max_features=config.max_features,
            sublinear_tf=True,
        )
        blocks.append(tf.fit_transform(docs))
    svd = TruncatedSVD(n_components=config.dim, random_state=config.seed)
    from scipy.sparse import hstack

    return normalize(svd.fit_transform(hstack(blocks).tocsr())).astype(np.float32)


def aspect_features(aspects: list[str]) -> np.ndarray:
    """Which of the three sub-ontologies the term belongs to.

    Admitted as an input where depth and ancestor counts are not: those are
    derived from the very edges the encoder is asked to predict, and a term
    being new means it does not have them yet. The aspect comes with the term.
    """
    order = ["P", "F", "C"]
    out = np.zeros((len(aspects), len(order)), dtype=np.float32)
    for i, a in enumerate(aspects):
        if a in order:
            out[i, order.index(a)] = 1.0
    return out
