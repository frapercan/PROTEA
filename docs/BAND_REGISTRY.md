# Per-band canonical (ontology snapshot, IA) registry

This note records which `(OntologySnapshot, IA)` pair is authoritative per
evaluation band, why those pairs are pinned rather than free-floated, and how
a train/eval snapshot mismatch inflates a fake PROTEA-vs-LAFA gap. It is the
prose half of `protea/core/band_registry.py` and complements
`docs/IA_PROVENANCE_v227.md` (the IA-source decision) and
`docs/EVAL_LAFA_PARITY.md` (the cafaeval-invocation parity).

## What a band binds

A *band* is a GOA evaluation window. Each band binds two derived artifacts:

1. An `OntologySnapshot` (identified by its `obo_version`). The snapshot
   governs True-Path propagation, the term universe / terms-of-interest, and
   orphan handling. Every cell in the band scores against the same snapshot.
2. An Information Accretion (IA) artifact (identified by a stable file token).
   The IA is computed from that same snapshot plus the band's t0 corpus and
   weights every GO term by its information content.

The registry maps `band -> (canonical obo_versions, canonical IA tokens)`.
These are DERIVED, pinned values, not free payload inputs: a cell never picks
its own snapshot or IA. Adding a new band (v228, CAFA7, ...) is a single new
`Band` row in `protea.core.band_registry.BANDS`.

## Authoritative pairs

| Band | Canonical snapshot (`obo_version`) | Canonical IA token | Source |
| - | - | - | - |
| v226 | `releases/2024-01-17` | `IA_cafa6.tsv` | historical benchmark cut; snapshot `35c3ad67` `ia_url` |
| v227 | `releases/2025-08-01`, `releases/2025-09-01` | `IA.tsv`, `IA-swissprot-exp-v227.txt` | deployed LAFA window (Sep_2025 t0) |

For v227 the authoritative IA is `lafa_t0_Sep_2025/IA.tsv`, the exact table
the deployed LAFA endpoint scored against (the comparability reference per
`docs/IA_PROVENANCE_v227.md`). The lab recompute
`IA-swissprot-exp-v227.txt` (Pearson r=0.98) is accepted as a faithful
reconstruction for the same band. The generic `IA_cafa6.tsv` is a DIFFERENT
corpus and is the v226 IA, so it is rejected for v227.

`obo_versions` is a closed *set* per band (not a single value) so an interim
ontology refresh inside one GOA window does not force a new band. No
`obo_version` and no IA token may be shared by two bands; that keeps the
reverse lookup deterministic and is enforced by the CI guard
(`scripts/check_band_registry.py`).

## Why a snapshot/IA mismatch inflates a fake gap

If a cell declared for one band is scored with the snapshot or IA of another
band, the comparison is no longer measuring prediction quality, it is
measuring artifact drift:

- A cross-band **snapshot** changes the propagation closure, the term
  universe, and the orphan set, so the same prediction is scored against a
  different ground-truth shape. The delta looks like a model regression but is
  a snapshot swap.
- A cross-band **IA** reweights `f_micro_w` against a foreign corpus. The
  v226 `IA_cafa6.tsv` and the v227 `IA.tsv` disagree by up to 14.6 on shared
  terms (`docs/IA_PROVENANCE_v227.md`); scoring a v227 prediction with the
  v226 IA shifts the headline metric without any change in the prediction.

Either substitution opens a phantom PROTEA-vs-LAFA gap: PROTEA reports a
number that LAFA's scorer could never reproduce, because LAFA pins the band's
own snapshot and IA. The fix is structural: bind both artifacts to the band
and reject any mix.

## The guard (runtime + CI)

`protea.core.band_registry.assert_band_consistency(band, obo_version, ia_ref)`
rejects a cell when its pivot snapshot `obo_version` or its resolved IA come
from a band other than the declared one, and forbids the uniform IC=1 fallback
for any band-declared cell. It extends the #599 IA resolver
(`_resolve_ia_file`): #599 binds the IA, this binds the snapshot
(propagation / term universe / orphans) as well.

- **Runtime.** `run_cafa_evaluation` accepts an optional `band` payload field.
  When set, `_enforce_band` resolves the IA reference (payload `ia_file`
  first, else snapshot `ia_url`) and calls the guard right after IA
  resolution, before cafaeval forks. A consistent banded run emits
  `run_cafa_evaluation.band_verified`; a cross-band run raises
  `BandMismatchError` and the job fails. Leaving `band` unset preserves the
  legacy ad-hoc (unbanded) behavior.
- **CI.** `scripts/check_band_registry.py` (wired into `.github/workflows/lint.yml`)
  fails the build if the registry is malformed: a shared `obo_version` or IA
  token between bands, a band that pins neither, or a canonical binding that
  does not round-trip through its own reverse lookup and guard.

## Dispatching a banded evaluation

In the `run_cafa_evaluation` payload, declare the band and pass the canonical
IA for it:

```json
{
  "evaluation_set_id": "<eval set>",
  "prediction_set_id": "<pred set>",
  "band": "v227",
  "ia_file": "/path/to/lafa_t0_Sep_2025/IA.tsv"
}
```

The run resolves the pivot snapshot from the EvaluationSet, verifies its
`obo_version` is canonical for v227, verifies `IA.tsv` is the v227 IA, emits
`run_cafa_evaluation.band_verified`, and only then scores. A dataset name such
as `bench-v1-K5-v227-lineage-prostt5` is also accepted as the `band` value:
the registry extracts the `vNNN` token.
