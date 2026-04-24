#!/usr/bin/env python3
"""Poll for succeeded predict_go_terms coords on the 220→230 benchmark and fire
one run_cafa_evaluation per (prediction_set, scoring_config) that is not yet
covered. Exits when 24 prediction_sets × 7 scoring_configs = 168 (pred, cfg)
pairs are covered.

Correct dedupe: compares the SET of covered (pred_id, cfg_id) pairs, not a
word-count — the previous launcher exited prematurely because it summed
evaluation_result rows (multiple per job) against a fixed integer.
"""
from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

API = "http://localhost:8000"
EVAL_SET = "7e481ffe-819e-42ee-b4e5-1606a24fb6a0"
QUERY_SET = "00aa9e73-4901-4770-972f-13e6fa36c41f"
POLL_SECONDS = 180

LOG = Path(__file__).parent.parent / "logs" / "cross-scoring-launcher.log"
LOG.parent.mkdir(exist_ok=True)


def log(msg: str) -> None:
    ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
    line = f"{ts} {msg}"
    with LOG.open("a") as f:
        f.write(line + "\n")
    print(line, flush=True)


def get_json(path: str) -> object:
    with urllib.request.urlopen(f"{API}{path}") as r:
        return json.load(r)


def fetch_scoring_configs() -> dict[str, str]:
    """Return {name: id} for every scoring_config currently in the DB."""
    data = get_json("/scoring/configs")
    return {c["name"]: c["id"] for c in data}


def fetch_benchmark_prediction_sets() -> dict[str, str]:
    """Return {prediction_set_id: description} for every pred_set whose predict
    coord is SUCCEEDED and whose query_set matches the benchmark."""
    pred_sets: dict[str, str] = {}
    jobs = get_json("/jobs?operation=predict_go_terms&status=succeeded&limit=200")
    for j in jobs:
        events = get_json(f"/jobs/{j['id']}/events")
        for e in events:
            if e.get("event") == "job.dispatched":
                result = (e.get("fields") or {}).get("result") or {}
                psid = result.get("prediction_set_id")
                if psid:
                    # filter to the 220→230 benchmark via the query_set in the payload
                    payload = get_json(f"/jobs/{j['id']}").get("payload") or {}
                    if payload.get("query_set_id") == QUERY_SET:
                        pred_sets[psid] = j.get("operation_summary", "")
                break
    return pred_sets


def fetch_covered_pairs() -> set[tuple[str, str]]:
    """Return the set of (prediction_set_id, scoring_config_id) that already
    have at least one evaluation_result persisted for the benchmark eval_set."""
    results = get_json(f"/annotations/evaluation-sets/{EVAL_SET}/results")
    return {(r["prediction_set_id"], r["scoring_config_id"]) for r in results}


def fetch_in_flight_pairs() -> set[tuple[str, str]]:
    """Return (prediction_set_id, scoring_config_id) pairs that already have a
    cafa-evaluation job in QUEUED or RUNNING state for this eval_set — so the
    launcher does not re-dispatch the same pair while the first one is still
    waiting in the queue."""
    in_flight: set[tuple[str, str]] = set()
    for status in ("queued", "running"):
        jobs = get_json(
            f"/jobs?operation=run_cafa_evaluation&status={status}&limit=500"
        )
        for j in jobs:
            # The jobs listing doesn't include the payload; fetch per-job detail.
            d = get_json(f"/jobs/{j['id']}")
            payload = d.get("payload") or {}
            if payload.get("evaluation_set_id") != EVAL_SET:
                continue
            psid = payload.get("prediction_set_id")
            cfg = payload.get("scoring_config_id")
            if psid and cfg:
                in_flight.add((psid, cfg))
    return in_flight


def fire_eval(pred_id: str, cfg_id: str) -> str | None:
    body = json.dumps({"prediction_set_id": pred_id, "scoring_config_id": cfg_id}).encode()
    req = urllib.request.Request(
        f"{API}/annotations/evaluation-sets/{EVAL_SET}/run",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req) as r:
            d = json.load(r)
        return d.get("id") or d.get("job_id")
    except urllib.error.HTTPError as e:
        log(f"  fire failed pred={pred_id[:8]} cfg={cfg_id[:8]} HTTP {e.code} {e.read()[:200]!r}")
        return None


def main() -> None:
    configs = fetch_scoring_configs()
    expected_cfg = 7
    if len(configs) < expected_cfg:
        log(f"WARN: only {len(configs)} scoring configs found — expected {expected_cfg}")
    log(f"launcher started · configs={sorted(configs.keys())} · poll={POLL_SECONDS}s")

    target_pairs_total = 0  # grows as more pred_sets complete

    while True:
        preds = fetch_benchmark_prediction_sets()
        covered = fetch_covered_pairs()
        in_flight = fetch_in_flight_pairs()
        blocked = covered | in_flight

        want: list[tuple[str, str, str]] = []
        for psid in preds:
            for name, cid in configs.items():
                if (psid, cid) not in blocked:
                    want.append((psid, cid, name))

        fired_pass = 0
        for psid, cid, name in want:
            jid = fire_eval(psid, cid)
            if jid:
                log(f"  fired pred={psid[:8]} cfg={name:<25} → job={jid[:8]}")
                blocked.add((psid, cid))  # local book-keeping within this pass
                fired_pass += 1
            time.sleep(0.3)

        target_pairs_total = len(preds) * len(configs)
        log(
            f"pass done · preds_ready={len(preds)} · configs={len(configs)} · "
            f"covered={len(covered)}/{target_pairs_total} · "
            f"in_flight={len(in_flight)} · fired_this_pass={fired_pass}"
        )

        if (
            len(preds) == 24
            and len(covered) >= target_pairs_total
            and fired_pass == 0
            and not in_flight
        ):
            log(f"all {target_pairs_total} (pred, config) pairs covered — exiting")
            return

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
