# 2026-05-26 GPU resume of EXP.13 blocked (cu128 wheel missing)

Slice: F-GPU-RESUME-EXP13 (phase F4, executor loop, P0).
Related memory: `exp13-paused-gpu-pivot-2026-05-26`,
`farm-exp-13-dispatched-2026-05-25`, `multi-plm-v226-sweep-plan`.

## Status

The slice is **BLOCKED** on three independent preconditions, none of
which the executor turn can satisfy on its own. Per the slice
acceptance criteria worst-case clause ("cu128 wheel missing"), this
document captures the exact next-step procedure so the human can
unblock with a single command sequence.

## Preconditions checked

* `protea-method` PR `#31` (`feat/torch-gpu-knn`) is OPEN, not merged
  (title: `feat(retrieval): torch GPU KNN (chunked cdist+topk, CPU
  fallback)`).
* PROTEA PR `#539` is OPEN, not merged (title: `docs(retrieval):
  document torch KNN env vars in knn_search shim`).
* F-OPS-JOBS.1 (dedup + lease + SIGTERM handler) is pending; declared
  as a dep in this slice's frontmatter.
* `torch.cuda.is_available()` in the dev venv is False. Installed
  build is `torch 2.11.0+cpu` from the `pytorch-cpu` source pin.
* Host GPU and driver are OK: `RTX 3060 12 GB`, driver `570.211.01`,
  `CUDA 12.8` reported by `nvidia-smi`.
* Host nvcc is `12.0`, acceptable; runtime libs ship with the torch
  wheel.

The pin in `pyproject.toml` line 53 is explicit:

```
torch = {source = "pytorch-cpu"}
torchvision = {source = "pytorch-cpu"}
```

That source resolves `torch==2.11.0+cpu`, by design (see
`pyproject.toml` lines 45 to 51: keeps the slim CI runner and Docker
image off the ~6 GB NVIDIA / triton stack). GPU hosts override after
`poetry install` via `scripts/install_gpu_torch.sh`.

## Exact unblock procedure

Run these on the host that owns the dev stack (currently the
developer workstation; not in any worktree, not in
`~/Thesis2/repositories/PROTEA/` when an executor agent is active in
there).

### Step 1. Wait for PRs to merge

Use `gh pr view 31 -R frapercan/protea-method --json state,mergedAt`
and `gh pr view 539 -R frapercan/PROTEA --json state,mergedAt`. Both
must report `"state": "MERGED"` before continuing. PR `#31` carries
the actual GPU KNN backend; PR `#539` documents the env vars
(`PROTEA_KNN_BACKEND`, `PROTEA_KNN_DEVICE`, batch sizing) consumed by
the shim.

Also confirm F-OPS-JOBS.1 has shipped via the plan progress script
(`bash agent-farm/scripts/plan-progress.sh` with the F-OPS phase
flag). The dedup, lease and SIGTERM work is what stops the just
resumed jobs from being killed again by the next deploy or by the
SIGKILL path noted in `exp13-paused-gpu-pivot-2026-05-26` (worker
training ignored both SIGTERM and SIGINT and had to be SIGKILL'd).

### Step 2. Bump the `protea-method` pin to the merge commit

After PR `#31` lands, `protea-method`'s `main` branch already moves;
the PROTEA pyproject pin is a `branch = "main"` git ref so `poetry
lock` pulls the new tip with no edit required. The exact invocation
is `poetry lock` followed by `git add poetry.lock` and `git commit -m
"chore: lock protea-method tip for GPU KNN backend"` from
`~/Thesis2/repositories/PROTEA`.

If the pin is later moved to a tagged release (preferred for
reproducibility), edit line 73 of `pyproject.toml` to
`protea-method = { git = "https://github.com/frapercan/protea-method.git",
tag = "v0.X.Y" }`, then run `poetry lock` and commit both files.

### Step 3. Install the CUDA 12.8 torch wheel

The repo ships a wrapper that picks the variant via env var. PyTorch
publishes cu128 wheels on its index since torch 2.6. From
`~/Thesis2/repositories/PROTEA`, run
`CUDA_VARIANT=cu128 bash scripts/install_gpu_torch.sh`.

Self-check at the end of the script must print
`torch 2.X.Y+cu128 cuda_available=True`.

If the wrapper picks up the worktree venv instead of the dev venv,
target it explicitly by setting `VENV_PATH` to the output of
`poetry env info --path` before invoking the install script with the
same `CUDA_VARIANT=cu128` env var.

If the cu128 wheel for the resolved torch version is not yet on the
index (rare), fall back to `cu126` or `cu124`; the 570.x driver is
forward compatible with all three. Do not pass `--no-deps`: torch
needs `nvidia-cudnn-cu12` and friends to import (see comment block at
the top of `scripts/install_gpu_torch.sh` documenting the 2026-05-12
job `384fa8de` regression).

### Step 4. Parity smoke (smallest cell first)

The 9 paused cells are listed in
`exp13-paused-gpu-pivot-2026-05-26`. Smallest is
`bench-v1-K3-v226-lineage-ankh_base`, current QUEUED job
`2fac4975-...`. Re-publish it through the API
(`POST /v1/jobs/:id/resume` once F-OPS-JOBS.2 ships, or via the
dispatch endpoint that `export_research_dataset` uses today). Watch
its row in the `jobs` table for `status`, `started_at`,
`finished_at`, `error_code`.

When it reaches SUCCEEDED, compare its produced `Fmax` against the
matching K=3 ankh_base SUCCEEDED cell from the 2026-05-25 batch. The
slice spec accepts a delta below `0.005` absolute Fmax. Fetch both
via `GET /v1/datasets/by-name/bench-v1-K3-v226-lineage-ankh_base` and
the prior batch's dataset row.

### Step 5. Fan-out the remaining 8

If parity passes, re-publish the remaining 8 paused job IDs (see the
table in `exp13-paused-gpu-pivot-2026-05-26`). Use
`dispatch_with_lock` (memory `FARM-FEAT.13`) to avoid duplicate
enqueues. Mark the CANCELLED rows for `e0fbdf5f-...` (K=5
esmc_600m) and `8813dba3-...` (K=3 esmc_600m) with
`payload.retry_of = <new_job_id>` for audit.

### Step 6. Report

When all 9 reach SUCCEEDED:

1. Update `farm-exp-13-dispatched-2026-05-25` memory with the
   completed run IDs and per-cell Fmax.
2. Flip F-DATA-PACK backfill of the remaining 13 dataset cards
   (memory `data-pack-loop-done-2026-05-25` records the gate).
3. Mark F-GPU-RESUME-EXP13 done in
   `agent-farm/plans/executor/PLAN.md`.

## Why this slice cannot self-unblock

* Installing cu128 wheels (`--force-reinstall`) touches the dev
  workspace venv, which executor agents are forbidden from mutating
  per the root `CLAUDE.md` hard constraint ("NEVER touch
  `~/Thesis2/repositories/PROTEA/`"). The worktree's own venv is
  cheap to rebuild, but the dev stack worker that actually consumes
  `protea.training` queue messages runs out of the dev venv.
* The protea-method PR is owned by upstream review on a different
  repo; an executor on PROTEA cannot merge it.
* F-OPS-JOBS.1 is a separate slice that must ship under its own PR.

So the executor logs this incident, files the heartbeat, and exits
without modifying queue state or installing wheels. The user
("mañana actualizo si hace falta") is the human in the loop.
