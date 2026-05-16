ADR-D34: Selective rerank resurrection - recompute, not archaeology
===================================================================

:Status: Proposed
:Date: 2026-05-16

Context
-------

The reranker lab maintains validation bands for bench-v1-K5 across v220,
v226, and v230 lineage. The LAFA submission uses v226-v230 band. The
historical "selective rerank at K=10" champion record (avg cafaeval
0.4562) exists only as a memory-only entry. This record predates the
range distinction and was not generated with explicit eval_set_name
tracking.

Lab memory shows the legacy record as leakage-contaminated and of
unknown range provenance. The `SUMMARY_v23-v26.md` lab summary does
not contain 0.4562 for bench-v1-K5-v226-lineage or any other current
validation band. The record is therefore not reproducible or
comparable to current champion runs.

Decision
--------

1. When historical records conflict with, or cannot be reproduced on,
   current validation data, recompute on the current bench rather than
   reverse-engineer the old configuration.

2. Specifically for the selective-rerank-at-K=10 cell: re-train on
   bench-v1-K5-v226-lineage (and optionally v230) using the current
   selective rerank policy, explicit eval_set_name tracking, and
   F-EXP-RESET run.json layout.

3. The recomputed Fmax becomes the live champion record, with no claim
   on comparability to the legacy 0.4562.

4. The legacy memory-only record (key: `project_v18_selective_rerank`)
   is retained as historical context and noted as superseded. It does
   not appear in active champions.md.

5. FARM-EXP.10 slice scope changes from "reconstruct axis tuple from
   RerankerModel table" to "re-train with current policy on current
   bench".

Consequences
------------

**Positive**

- Eliminates the need to reverse-engineer unknown historical configs.
- Produces a valid, reproducible champion record with full range
  traceability (eval_set_name pinned).
- Establishes a scalable pattern for future legacy-record conflicts:
  recompute, not archaeology.

**Negative**

- The legacy 0.4562 record is explicitly marked as not comparable to
  current champions. Any narrative claiming continuity with the old
  cell is incorrect.
- Requires regeneration of the cell, not mere documentation of existing
  artefact.

**Neutral**

- Memory record `project_v18_selective_rerank` documents the historical
  value and its supersession; future maintainers can cross-reference if
  needed.

References
----------

- Memory entry: `project_v18_selective_rerank` (legacy champion, marked superseded)
- Memory entry: `feedback_no_archaeology_recompute` (policy decision)
- Memory entry: `reference_lab_validation_ranges` (v220/v226/v230 distinction)
- FARM-EXP.10 slice definition
- Lab summary: `SUMMARY_v23-v26.md` (current bench results)
