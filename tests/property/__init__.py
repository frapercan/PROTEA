"""Property-based tests (F6.2).

Hypothesis strategies exercise contract payloads, the scoring engine
and the parquet round-trip path with thousands of randomized inputs
to surface boundary conditions that example-based tests miss.

The CI profile (registered in ``tests/conftest.py``) pins the seed
via ``derandomize=True`` so every run picks the same examples; a
regression here is reproducible from the recorded blob.
"""
