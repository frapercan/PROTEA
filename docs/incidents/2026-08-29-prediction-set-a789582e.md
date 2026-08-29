# Prediction set a789582e, frozen 2026-08-29

`a789582e-fa6d-4d91-9fd3-0a6c4b649427`, 2,801,404 rows, 0 evaluation results.

## Why this set is kept

It is the only artifact in the store that measures the defect PR #890 fixed.
Its reference pool was gated by evidence while its donations were not, and that
state cannot be requested from any configuration that exists today: a run of the
current code with the same payload produces a different set. Deleting it would
remove the measurement along with the mistake.

It is not "the old baseline". It is a third regime, and its numbers must never
be reused as one. The only defensible use of them is an explicitly labelled
before and after of the defect itself, never a shared axis and never a figure
the new numbers beat.

## What it holds

Counted directly against the store on 2026-08-29, after the campaign had been
stopped and before any new retrieval.

**Out-of-policy donations: 1,523,939 rows of 2,801,404 (54.4%).** The set
declares an evidence-code policy of thirteen experimental codes. These rows
donated under codes outside it:

| code | rows    | code | rows   |
|------|---------|------|--------|
| IEA  | 787,716 | ND   | 16,722 |
| IBA  | 297,778 | RCA  | 4,617  |
| ISO  | 215,571 | ISA  | 2,186  |
| ISS  | 151,426 | ISM  | 1,713  |
| NAS  | 46,142  | IGC  | 68     |

**Self-donation: 133,866 rows over 6,012 proteins**, where the donor accession
equals the query accession, under `exclude_self_neighbour: true`. The flag was
recorded and did not govern.

**No donor ledger: 193,303 rows over 1,024 proteins**, exactly one batch of
1,024 queries, with `donor_count` and `sequence_rank` NULL on every row. The
`job_event` stream stamps that batch's host as `desktop-gpu`. The compute node
ran a revision that predated the change adding those columns, and the run
reported success. See PR #893 for the guard.

**Declared ontology snapshot is not the one its terms belong to.** The set
records `ontology_snapshot_id a24e7d91`. Zero of its rows carry a term
belonging to that snapshot; every term resolves to `36038118`. A consumer
trusting that field gets nothing.

## Why the four numbers cannot be compared with the sealed results

The 71 sealed evaluation results come from `d5b634b2` (63, permissive, k=10)
and `8a75f84e` (8, evidence codes under pool-admission semantics, k=10). Four
axes move between them and any corrected run: depth 10 to 30, the policy's
value on 63 of 71, the policy's meaning on all 71, and self-exclusion. Only the
third moves without changing any recorded field, which is why the code revision
is now one of the fields an arm is named by.

The magnitude figure of 20.70 to 12.13 that circulated is an internal before
and after of this set. Against `d5b634b2`, which produced 63 of the 71, the
same comparison is NK 14.46 to 12.53 and PK 9.59 to 10.78, where the sign
reverses. Do not reuse that trio.

## Reproducing the census

```sql
-- out of policy
WITH s AS (SELECT id, meta FROM prediction_set WHERE id = 'a789582e-...'),
     pol AS (SELECT array(SELECT jsonb_array_elements_text(
         meta -> 'donor_policy' -> 'evidence_codes')) c FROM s)
SELECT g.evidence_code, count(*) FROM go_prediction g, s, pol
WHERE g.prediction_set_id = s.id AND NOT (g.evidence_code = ANY(pol.c))
GROUP BY g.evidence_code ORDER BY 2 DESC;

-- self donation
SELECT count(*), count(DISTINCT protein_accession) FROM go_prediction
WHERE prediction_set_id = 'a789582e-...'
  AND ref_protein_accession = protein_accession;

-- rows written without a donor ledger
SELECT count(*), count(DISTINCT protein_accession) FROM go_prediction
WHERE prediction_set_id = 'a789582e-...' AND donor_count IS NULL;
```
