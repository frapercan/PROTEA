"""The count map of the explainable-units family (ADR-D65).

A unit of the lexicon (a theme, or a segment family) is read into GO by counting
the donors of a cut that carry it: ``p(u, t) = (K_u(t) + m * pi(t)) / (N_u + m)``.
``fit`` finds m on the donors alone; ``reading`` scores the targets of a window
with the witness of every row. Ported from the laboratory's ``mapa_cuentas.py``
(cards 1 and 2 of 2026-11-13).
"""
