"""The lexicon of the explainable-units family, decoded in the platform (ADR-D63).

``model`` reads a checkpoint into the tables a decoder needs, ``candidates`` picks
each protein's themes, the ``kernels_*`` modules hold the exact Triton passes
(they need a card and import Triton), and ``decode`` gives the cut and the theme
mass of a batch of sequences.
"""
